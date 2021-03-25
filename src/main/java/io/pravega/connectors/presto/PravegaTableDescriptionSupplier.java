/*
 * Copyright (c) Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.connectors.presto;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import io.pravega.connectors.presto.schemamangement.CompositeSchemaRegistry;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.pravega.connectors.presto.util.PravegaNameUtils.multiSourceStream;
import static java.util.Objects.requireNonNull;

// pravega scope is a namespace for streams.  stream is unique within scope.
// presto schema is like a database, with collection of tables.
// we will map scope->schema and stream->table
// scope will be our database and streams will be tables in the database
//
// a stream schema could be "local". local definitions take precedence.
// .json file in known directory with naming format <schema>.<table>.json
// there could be any number of local schemas.
//
// additionally a local stream schema could be a composite table.  this is called 'multi-source'.
// stream name will be a regex.  it can match 1 or more source streams.  when this single table is
// queried we will consider events from all source streams
public class PravegaTableDescriptionSupplier
{
    private static final Logger log = Logger.get(PravegaTableDescriptionSupplier.class);

    private final CompositeSchemaRegistry schemaRegistry;

    private Cache<String, Object> schemaCache;

    private Cache<PravegaTableName, Optional<PravegaStreamDescription>> tableCache;

    private JsonCodec<PravegaStreamDescription> streamDescriptionCodec;

    @Inject
    PravegaTableDescriptionSupplier(PravegaConnectorConfig pravegaConnectorConfig,
                                    JsonCodec<PravegaStreamDescription> streamDescriptionCodec)
    {
        requireNonNull(pravegaConnectorConfig, "pravegaConfig is null");
        this.streamDescriptionCodec = streamDescriptionCodec;

        // there will be many successive calls to listSchemas + listTables in short time period
        // do not reach out to pravega each time as it is unlikely things would have changed
        // enhancement issue - can we determine if there are changes/removals and selectively update?
        // https://github.com/StreamingDataPlatform/pravega-sql/issues/101
        this.schemaCache = CacheBuilder.newBuilder()
                .expireAfterWrite(pravegaConnectorConfig.getTableCacheExpireSecs(), TimeUnit.SECONDS)
                .build();

        this.tableCache = CacheBuilder.newBuilder()
                .expireAfterWrite(pravegaConnectorConfig.getTableCacheExpireSecs(), TimeUnit.SECONDS)
                .build();

        this.schemaRegistry = new CompositeSchemaRegistry(pravegaConnectorConfig, streamDescriptionCodec);
    }

    @VisibleForTesting
    public PravegaTableDescriptionSupplier(Cache<String, Object> schemaCache,
                                           Cache<PravegaTableName, Optional<PravegaStreamDescription>> tableCache)
    {
        this.schemaCache = schemaCache;
        this.tableCache = tableCache;
        this.schemaRegistry = null;
    }

    public List<String> listSchemas()
    {
        // if any expired, retrieve list again from pravega
        // they are inserted to cache at same time so will all be same state
        final List<String> schemas = schemaCache.asMap().keySet().stream().collect(Collectors.toList());
        if (schemas.isEmpty()) {
            schemaRegistry.listSchemas().forEach(schema -> {
                schemas.add(schema);
                schemaCache.put(schema, new Object());
            });
        }
        else {
            log.debug("serving listSchemas() from cache");
        }
        return schemas;
    }

    public List<PravegaTableName> listTables(Optional<String> schema)
    {
        List<String> schemas = schema.map(Collections::singletonList).orElseGet(this::listSchemas);

        List<PravegaTableName> tableList = new ArrayList<>();

        for (String s : schemas) {
            List<PravegaTableName> tableListForSchema =
                    tableCache.asMap().keySet().stream()
                            .filter(streamDesc -> streamDesc.getSchemaTableName().getSchemaName().startsWith(s))
                            .collect(Collectors.toList());

            if (tableListForSchema.isEmpty()) {

                List<Pattern> compositeStreams = new ArrayList<>();

                schemaRegistry.listTables(s).forEach(table -> {

                    boolean hidden =
                            compositeStreams.stream().anyMatch(p -> p.matcher(table.getTableName()).matches());

                    PravegaTableName pravegaTableName = new PravegaTableName(s, table.getTableName(), hidden);

                    // don't clobber existing entry
                    if (tableCache.getIfPresent(pravegaTableName) == null ||
                            !tableCache.getIfPresent(pravegaTableName).isPresent()) {
                        tableCache.put(pravegaTableName, Optional.empty());
                    }

                    if (multiSourceStream(table)) {
                        // if component streams specified look for exact match when hiding
                        if (table.getObjectArgs().isPresent()) {
                            table.getObjectArgs().get().forEach(arg -> {
                                compositeStreams.add(Pattern.compile("^" + arg + "$"));
                            });
                        }
                        else {
                            // regex, fuzzy match
                            compositeStreams.add(Pattern.compile(table.getObjectName()));
                        }
                    }
                });
            }
            else {
                log.debug("serving listTables(%s) from cache", s);
            }

            tableList.addAll(tableCache.asMap().keySet().stream()
                    .filter(pravegaStreamDescription ->
                            pravegaStreamDescription.getSchemaTableName().getSchemaName().startsWith(s))
                    .collect(Collectors.toList()));
        }
        return tableList;
    }

    public PravegaStreamDescription getTable(SchemaTableName schemaTableName)
    {
        PravegaTableName pravegaTableName = new PravegaTableName(schemaTableName);
        Optional<PravegaStreamDescription> cachedTable = tableCache.getIfPresent(pravegaTableName);
        if (cachedTable != null && cachedTable.isPresent()) {
            log.debug("serving getTable(%s) from cache", schemaTableName);
            return cachedTable.get();
        }

        PravegaStreamDescription table = schemaRegistry.getTable(schemaTableName);

        if (multiSourceStream(table)) {
            // if component streams not already specified, look them up from pravega based on regex
            List<String> objectArgs = table.getObjectArgs().isPresent()
                    ? table.getObjectArgs().get()
                    : multiSourceStreamComponents(schemaTableName, table.getObjectName());
            if (objectArgs.isEmpty()) {
                throw new IllegalArgumentException("could not get component streams for " + schemaTableName);
            }

            List<PravegaStreamFieldGroup> fieldGroups = table.getEvent().orElse(new ArrayList<>(1));
            if (fieldGroups.isEmpty()) {
                fieldGroups = schemaRegistry.getSchema(new SchemaTableName(schemaTableName.getSchemaName(), objectArgs.get(0)));
            }

            table = new PravegaStreamDescription(table, fieldGroups, objectArgs);
        }
        else if (!fieldsDefined(table)) {
            table = new PravegaStreamDescription(table, schemaRegistry.getSchema(schemaTableName));
        }

        tableCache.put(pravegaTableName, Optional.of(table));
        return table;
    }

    private List<String> multiSourceStreamComponents(SchemaTableName schemaTableName, String sourcePattern)
    {
        Pattern pattern = Pattern.compile(sourcePattern);

        return listTables(Optional.of(schemaTableName.getSchemaName())).stream()
                .map(PravegaTableName::getSchemaTableName)
                .map(SchemaTableName::getTableName)
                .filter(tableName -> pattern.matcher(tableName).matches())
                .collect(Collectors.toList());
    }

    private static boolean fieldsDefined(PravegaStreamDescription table)
    {
        if (!table.getEvent().isPresent() ||
                table.getEvent().get().isEmpty()) {
            return false;
        }

        for (PravegaStreamFieldGroup fieldGroup : table.getEvent().get()) {
            if (fieldGroup.getFields() == null) {
                return false;
            }
        }

        return true;
    }
}
