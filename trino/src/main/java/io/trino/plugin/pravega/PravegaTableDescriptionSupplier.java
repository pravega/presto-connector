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

package io.trino.plugin.pravega;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.Stream;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.trino.spi.connector.SchemaTableName;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.trino.plugin.pravega.ProtobufCommon.encodeSchema;
import static io.trino.plugin.pravega.util.PravegaNameUtils.groupId;
import static io.trino.plugin.pravega.util.PravegaNameUtils.kvFieldMapping;
import static io.trino.plugin.pravega.util.PravegaNameUtils.kvTable;
import static io.trino.plugin.pravega.util.PravegaNameUtils.multiSourceStream;
import static io.trino.plugin.pravega.util.PravegaNameUtils.temp_streamNameToTableName;
import static io.trino.plugin.pravega.util.PravegaNameUtils.temp_tableNameToStreamName;
import static io.trino.plugin.pravega.util.PravegaSchemaUtils.readSchema;
import static io.trino.plugin.pravega.util.PravegaStreamDescUtils.mapFieldsFromSchema;
import static java.nio.file.Files.readAllBytes;
import static java.util.Arrays.asList;
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

    private final PravegaConnectorConfig pravegaConnectorConfig;

    private Cache<String, Object> schemaCache;

    private Cache<PravegaTableName, Optional<PravegaStreamDescription>> tableCache;

    private JsonCodec<PravegaStreamDescription> streamDescriptionCodec;

    // "inline" means that event was written using schema registry wrapped client and schema encoding id
    // is within the raw event data in pravega
    @Inject
    PravegaTableDescriptionSupplier(PravegaConnectorConfig pravegaConnectorConfig,
            JsonCodec<PravegaStreamDescription> streamDescriptionCodec)
    {
        requireNonNull(pravegaConnectorConfig, "pravegaConfig is null");
        this.pravegaConnectorConfig = pravegaConnectorConfig;
        this.streamDescriptionCodec = streamDescriptionCodec;

        // there will be many successive calls to listSchemas + listTables in short time period
        // do not reach out to pravega each time as it is unlikely things would have changed
        // enhancement issue - can we determine if there are changes/removals and selectively update?
        // https://github.com/pravega/presto-connector/issues/30
        this.schemaCache = CacheBuilder.newBuilder()
                .expireAfterWrite(pravegaConnectorConfig.getTableCacheExpireSecs(), TimeUnit.SECONDS)
                .build();

        this.tableCache = CacheBuilder.newBuilder()
                .expireAfterWrite(pravegaConnectorConfig.getTableCacheExpireSecs(), TimeUnit.SECONDS)
                .build();
    }

    @VisibleForTesting
    public PravegaTableDescriptionSupplier(PravegaConnectorConfig pravegaConnectorConfig,
            Cache<String, Object> schemaCache,
            Cache<PravegaTableName, Optional<PravegaStreamDescription>> tableCache)
    {
        this.pravegaConnectorConfig = pravegaConnectorConfig;
        this.schemaCache = schemaCache;
        this.tableCache = tableCache;
    }

    public List<String> listSchemas()
    {
        // if any expired, retrieve list again from pravega
        // they are inserted to cache at same time so will all be same state
        final List<String> schemas = schemaCache.asMap().keySet().stream().collect(Collectors.toList());
        if (schemas.isEmpty()) {
            listLocalSchemas().forEach(schema -> schemaCache.put(schema, new Object()));

            try (StreamManager streamManager =
                    StreamManager.create(ClientConfig.builder().controllerURI(
                            pravegaConnectorConfig.getControllerURI()).build())) {
                Streams.stream(streamManager.listScopes()).filter(s -> !internalObject(s)).forEach(schema -> {
                    schemas.add(schema);
                    schemaCache.put(schema, new Object());
                });
            }
        }
        else {
            log.info("serving listSchemas() from cache");
        }
        return schemas;
    }

    public List<PravegaTableName> listTables(Optional<String> schema)
    {
        List<String> schemas = schema.isPresent() ? Collections.singletonList(schema.get()) : listSchemas();

        StreamManager streamManager = null;

        try {
            List<PravegaTableName> tableList = new ArrayList<>();

            for (String s : schemas) {
                List<PravegaTableName> tableListForSchema =
                        tableCache.asMap().keySet().stream()
                                .filter(streamDesc -> streamDesc.getSchemaTableName().getSchemaName().startsWith(s))
                                .collect(Collectors.toList());

                // not all tables inserted to cache at same time
                if (tableListForSchema.isEmpty()) {
                    if (streamManager == null) {
                        streamManager = StreamManager.create(
                                ClientConfig.builder().controllerURI(pravegaConnectorConfig.getControllerURI()).build());
                    }

                    List<Pattern> compositeStreams = new ArrayList<>();

                    // local takes precedence.  list before pravega.  ifAbsent used later to not clobber.
                    listLocalTables(s).forEach(table -> {
                        PravegaTableName pravegaTableName = new PravegaTableName(s, table);

                        // don't clobber existing entry
                        if (tableCache.getIfPresent(pravegaTableName) == null ||
                                !tableCache.getIfPresent(pravegaTableName).isPresent()) {
                            tableCache.put(pravegaTableName, Optional.empty());
                        }

                        // load .json def to get stream name in order to determine type
                        PravegaStreamDescription localTable = getLocalTable(pravegaTableName.getSchemaTableName());
                        if (multiSourceStream(localTable)) {
                            compositeStreams.add(Pattern.compile(localTable.getObjectName()));
                        }
                    });

                    // (underlying streams used by kv table are seen as internal and thus are skipped)
                    Streams.stream(streamManager.listStreams(s))
                            .filter(stream -> !internalStream(stream))
                            .forEach(stream -> {
                                boolean hidden =
                                        compositeStreams.stream().anyMatch(p -> p.matcher(stream.getStreamName()).matches());
                                // ifAbsent - don't clobber table description if we have it
                                PravegaTableName tableName = new PravegaTableName(s, temp_streamNameToTableName(stream.getStreamName()), hidden);
                                if (tableCache.getIfPresent(tableName) == null ||
                                        !tableCache.getIfPresent(tableName).isPresent()) {
                                    tableCache.put(tableName, Optional.empty());
                                }
                            });
                }
                else {
                    log.info("serving listTables(%s) from cache", s);
                }

                tableList.addAll(tableCache.asMap().keySet().stream()
                        .filter(pravegaStreamDescription ->
                                pravegaStreamDescription.getSchemaTableName().getSchemaName().startsWith(s))
                        .collect(Collectors.toList()));
            }
            return tableList;
        }
        finally {
            if (streamManager != null) {
                streamManager.close();
            }
        }
    }

    public PravegaStreamDescription getTable(SchemaTableName schemaTableName)
    {
        PravegaTableName pravegaTableName = new PravegaTableName(schemaTableName);
        Optional<PravegaStreamDescription> cachedTable = tableCache.getIfPresent(pravegaTableName);
        if (cachedTable != null && cachedTable.isPresent()) {
            log.info("serving getTable(%s) from cache", schemaTableName);
            return cachedTable.get();
        }

        PravegaStreamDescription table = getLocalTable(schemaTableName);
        if (table != null) {
            log.info("found local schema for '%s'", schemaTableName);

            // kv this is list of key family (defined in local schema file)
            // for multi source stream this is list of composite streams (empty here, to be filled in later)
            Optional<List<String>> objectArgs = table.getObjectArgs();

            // field definitions can come from 1 of 4 places
            // (1) defined in local .json schema ("event/fields")
            // (2) uri in "dataSchema" field
            // (3) lookup from a source stream (if multi source stream)
            // (4) lookup directly in schema registry (if kv table)

            Optional<List<PravegaStreamFieldGroup>> fieldGroups = Optional.empty();

            if (fieldsDefined(table)) {
                // case (1) - no-op
                log.info("fields defined in schema file %s", schemaTableName);
                fieldGroups = Optional.of(new LinkedList<>(table.getEvent().get()));
            }
            else if (table.getEvent().isPresent() &&
                    table.getEvent().get().get(0).getDataSchema().isPresent()) {
                fieldGroups = Optional.of(new LinkedList<>());

                // case (2) uri containing schema
                List<PravegaStreamFieldGroup> finalFieldGroups = fieldGroups.get();
                for (int i = 0; i < table.getEvent().get().size(); i++) {
                    PravegaStreamFieldGroup event = table.getEvent().get().get(i);
                    String colPrefix = event.getMapping().orElse(
                            table.getEvent().get().size() > 1 ? kvFieldMapping(i) : "");
                    Optional<String> dataSchema = Optional.of(readSchema(event.getDataSchema().get()));
                    PravegaStreamFieldGroup fieldGroup =
                            new PravegaStreamFieldGroup(event.getDataFormat(),
                                    Optional.empty(),
                                    dataSchema,
                                    Optional.of(
                                            mapFieldsFromSchema(colPrefix, event.getDataFormat(), dataSchema.get())));
                    finalFieldGroups.add(fieldGroup);
                }
            }
            else if (kvTable(table)) {
                fieldGroups = fieldGroupsFromSchemaRegistry(schemaTableName);
            }

            if (multiSourceStream(table)) {
                // stream name will be some regex.
                // find all the possible source streams.
                Pattern pattern = Pattern.compile(table.getObjectName());

                List<PravegaTableName> sourceTableNames =
                        listTables(Optional.of(schemaTableName.getSchemaName())).stream()
                                .filter(t -> pattern.matcher(t.getSchemaTableName().getTableName()).matches())
                                .collect(Collectors.toList());

                objectArgs = Optional.of(sourceTableNames.stream()
                        .map(PravegaTableName::getSchemaTableName)
                        .map(SchemaTableName::getTableName)
                        .collect(Collectors.toList()));

                if (!fieldGroups.isPresent()) {
                    // case (3) schema not already defined, look one up
                    // lookup actual schema from any of them - implies all sources are the same
                    PravegaStreamDescription sourceTable = sourceTableNames.isEmpty()
                            ? null
                            : getTable(sourceTableNames.get(0).getSchemaTableName());
                    if (sourceTable == null) {
                        throw new IllegalArgumentException("no stream found for multi source");
                    }
                    fieldGroups = Optional.of(new LinkedList<>());
                    fieldGroups.get().add(new PravegaStreamFieldGroup(
                            sourceTable.getEvent().get().get(0).getDataFormat(),
                            Optional.empty(),
                            sourceTable.getEvent().get().get(0).getDataSchema(),
                            Optional.of(sourceTable.getEvent().get().get(0).getFields())));
                }
            }

            fieldGroups.orElseThrow(() ->
                    new IllegalArgumentException("unable to determine schema for " + schemaTableName));

            // our final table definition.  use schema that we looked up, and set all source stream names here
            table = new PravegaStreamDescription(schemaTableName.getTableName(),
                    Optional.of(schemaTableName.getSchemaName()),
                    table.getObjectName(),
                    Optional.of(table.getObjectType()),
                    objectArgs,
                    fieldGroups);

            tableCache.put(pravegaTableName, Optional.of(table));
            return table;
        }

        Optional<List<PravegaStreamFieldGroup>> fieldGroups = fieldGroupsFromSchemaRegistry(schemaTableName);

        table = new PravegaStreamDescription(
                schemaTableName.getTableName(),
                Optional.of(schemaTableName.getSchemaName()),
                temp_tableNameToStreamName(schemaTableName.getTableName()),
                Optional.of(ObjectType.STREAM),
                Optional.empty() /* args */,
                fieldGroups);
        tableCache.put(pravegaTableName, Optional.of(table));
        return table;
    }

    /**
     * construct PravegaStreamFieldGroup by looking up schema in schema registry
     *
     * @param schemaTableName
     * @return
     */
    private Optional<List<PravegaStreamFieldGroup>> fieldGroupsFromSchemaRegistry(final SchemaTableName schemaTableName)
    {
        log.info("look up description of '%s' from pravega", schemaTableName);
        String groupName = groupId(schemaTableName.getSchemaName(), temp_tableNameToStreamName(schemaTableName.getTableName()));

        SchemaRegistryClientConfig registryConfig =
                SchemaRegistryClientConfig.builder()
                        .schemaRegistryUri(pravegaConnectorConfig.getSchemaRegistryURI()).build();
        SchemaRegistryClient registryClient = SchemaRegistryClientFactory.withDefaultNamespace(registryConfig);

        List<PravegaStreamFieldGroup> fieldGroups = new ArrayList<>(2);

        GroupProperties properties =
                registryClient.getGroupProperties(groupName);

        List<SchemaWithVersion> schemas = registryClient.getSchemas(groupName);
        if (schemas.size() == 0 || schemas.size() > 2) {
            throw new IllegalStateException(schemaTableName + " has " + schemas.size() + " registered schemas.  expecting either 1 or 2");
        }

        // kv table will have > 1 schema.  key+value likely different types
        boolean kv = schemas.size() > 1;

        for (int i = 0; i < schemas.size(); i++) {
            // colPrefix used for display so can differentiate between fields from key or value
            String colPrefix = kv ? kvFieldMapping(i) : "";

            SerializationFormat format = schemas.get(i).getSchemaInfo().getSerializationFormat();
            fieldGroups.add(new PravegaStreamFieldGroup(
                    normalizeDataFormat(format),
                    Optional.of(colPrefix),
                    dataSchema(format, schemas.get(i)),
                    Optional.of(mapFieldsFromSchema(colPrefix, format, schemas.get(i)))));
        }

        return Optional.of(fieldGroups);
    }

    private static boolean fieldsDefined(PravegaStreamDescription table)
    {
        // event is optional, fields within event is also optional
        // for kv table - 0 or 2 schemas.  so fine to just check for 1.
        return table.getEvent().isPresent() && (table.getEvent().get().get(0).getFields() != null);
    }

    private List<String> listLocalSchemas()
    {
        return localSchemaStream()
                .map(file -> file.getName().split("\\.")[0])
                .collect(Collectors.toList());
    }

    // scope.stream -> schema.table
    private List<String> listLocalTables(String schema)
    {
        return localSchemaStream()
                .filter(file -> file.getName().endsWith(".json"))
                .filter(file -> file.getName().startsWith(schema))
                .filter(file -> file.getName().split("\\.").length == 3)
                .map(file -> file.getName().split("\\.")[1])
                .collect(Collectors.toList());
    }

    private PravegaStreamDescription getLocalTable(SchemaTableName schemaTableName)
    {
        try {
            File file = new File(pravegaConnectorConfig.getTableDescriptionDir(),
                    String.format("%s.%s.json", schemaTableName.getSchemaName(), schemaTableName.getTableName()));
            if (!file.exists()) {
                return null;
            }
            return streamDescriptionCodec.fromJson(readAllBytes(file.toPath()));
        }
        catch (IOException e) {
            log.error("%s", e);
            throw new UncheckedIOException(e);
        }
        catch (RuntimeException e) {
            log.error("%s", e);
            throw e;
        }
    }

    private java.util.stream.Stream<File> localSchemaStream()
    {
        return listFiles(pravegaConnectorConfig.getTableDescriptionDir()).stream()
                .filter(file -> file.isFile() && file.getName().endsWith(".json"));
    }

    private static List<File> listFiles(File dir)
    {
        if ((dir != null) && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                log.debug("Considering files: %s", asList(files));
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    private static String normalizeDataFormat(SerializationFormat format)
    {
        // (CSV is custom)
        return format == SerializationFormat.Custom
                ? format.getFullTypeName().toLowerCase(Locale.ENGLISH)
                : format.name().toLowerCase(Locale.ENGLISH);
    }

    private static Optional<String> dataSchema(SerializationFormat format, SchemaWithVersion schemaWithVersion)
    {
        // it is intentional that nothing is returned for Custom
        // pass schema to row decoders.  refer to PravegaRecordSetProvider
        switch (format) {
            case Protobuf:
                return Optional.of(encodeSchema(schemaWithVersion));
            case Avro:
                return Optional.of(new String(schemaWithVersion.getSchemaInfo().getSchemaData().array(), StandardCharsets.UTF_8));
            default:
                return Optional.empty();
        }
    }

    private static boolean internalStream(Stream stream)
    {
        return internalObject(stream.getStreamName());
    }

    private static boolean internalObject(String object)
    {
        return object.startsWith("_") /* pravega internal */ ||
                object.endsWith("-SC") /* application internal - stream cuts */;
    }
}
