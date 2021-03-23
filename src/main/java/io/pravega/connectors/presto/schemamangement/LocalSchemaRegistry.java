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
package io.pravega.connectors.presto.schemamangement;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import io.pravega.connectors.presto.ObjectType;
import io.pravega.connectors.presto.PravegaStreamDescription;
import io.pravega.connectors.presto.PravegaStreamFieldGroup;
import io.pravega.connectors.presto.PravegaTableHandle;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static io.pravega.connectors.presto.util.PravegaNameUtils.kvFieldMapping;
import static io.pravega.connectors.presto.util.PravegaStreamDescUtils.resolveAllSchemas;
import static java.nio.file.Files.readAllBytes;

public class LocalSchemaRegistry
        implements SchemaSupplier, SchemaRegistry
{
    private final File localTableDir;

    private final JsonCodec<PravegaStreamDescription> streamDescriptionCodec;

    public LocalSchemaRegistry(File localTableDir, JsonCodec<PravegaStreamDescription> streamDescriptionCodec)
    {
        this.localTableDir = localTableDir;
        this.streamDescriptionCodec = streamDescriptionCodec;
    }

    @Override
    public List<String> listSchemas()
    {
        return localSchemaStream()
                .map(file -> file.getName().split("\\.")[0])
                .collect(Collectors.toList());
    }

    @Override
    public List<PravegaTableHandle> listTables(String schema)
    {
        final List<PravegaTableHandle> tables = new ArrayList<>();

        localSchemaStream()
                .filter(file -> file.getName().startsWith(schema))
                .filter(file -> file.getName().split("\\.").length == 3)
                .map(file -> file.getName().split("\\.")[1])
                .map(file -> getLocalTable(new SchemaTableName(schema, file)))
                .forEach(table -> {
                    tables.add(new PravegaTableHandle(table.getSchemaName().get(),
                            table.getTableName(),
                            table.getObjectName(),
                            table.getObjectType(),
                            table.getObjectArgs()));
                });

        return tables;
    }

    @Override
    public List<PravegaStreamFieldGroup> getSchema(SchemaTableName schemaTableName) {
        PravegaStreamDescription streamDescription = getLocalTable(schemaTableName);
        return streamDescription == null ? null : streamDescription.getEvent().orElse(null);
    }

    @Override
    public PravegaStreamDescription getTable(SchemaTableName schemaTableName)
    {
        // reads table definition from local file
        // if table definition has pointers to schema, read it and populate fields
        // (for e.g. local schema file or url to schema)
        PravegaStreamDescription table = getLocalTable(schemaTableName);

        // either not found or no fields, nothing to do.  will be resolved later
        if (table == null || !table.getEvent().isPresent()) {
            return table;
        }

        // fields already defined
        if (table.getEvent().get().stream().noneMatch(
                schema -> schema.getDataSchema().isPresent())) {
            return table;
        }

        // at least 1 schema for a fieldGroup must be resolved.  read schema from local file or url
        List<PravegaStreamFieldGroup> finalSchemas =
                resolveAllSchemas(table.getEvent().get(), (i) -> columnPrefix(table, i));

        return new PravegaStreamDescription(table, finalSchemas);
    }

    static String columnPrefix(PravegaStreamDescription table, int schemaIndex) {
        // if kv table, returns something like key/{fieldName}, value/{fieldName}
        return table.getObjectType() == ObjectType.KV_TABLE ? kvFieldMapping(schemaIndex) : "";
    }

    private java.util.stream.Stream<File> localSchemaStream()
    {
        return listFiles(localTableDir).stream()
                .filter(file -> file.isFile() && file.getName().endsWith(".json"));
    }

    private static List<File> listFiles(File dir)
    {
        if ((dir != null) && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    private PravegaStreamDescription getLocalTable(SchemaTableName schemaTableName)
    {
        try {
            File file = new File(localTableDir, String.format("%s.%s.json",
                    schemaTableName.getSchemaName(), schemaTableName.getTableName()));
            return !file.exists() ?
                    null
                    : streamDescriptionCodec.fromJson(readAllBytes(file.toPath()));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
