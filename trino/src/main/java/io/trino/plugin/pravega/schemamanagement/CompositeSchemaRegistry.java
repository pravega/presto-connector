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
package io.trino.plugin.pravega.schemamanagement;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.json.JsonCodec;
import io.trino.plugin.pravega.PravegaConnectorConfig;
import io.trino.plugin.pravega.PravegaStreamDescription;
import io.trino.plugin.pravega.PravegaStreamFieldGroup;
import io.trino.plugin.pravega.PravegaTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.ArrayList;
import java.util.List;

public class CompositeSchemaRegistry
        implements SchemaSupplier, SchemaRegistry {
    private final List<SchemaSupplier> schemaSuppliers;

    private final List<SchemaRegistry> schemaRegistries;

    public CompositeSchemaRegistry(PravegaConnectorConfig config, JsonCodec<PravegaStreamDescription> streamDescriptionCodec) {
        schemaSuppliers = new ArrayList<>();
        schemaRegistries = new ArrayList<>();

        // local will override, always add first
        if (config.getTableDescriptionDir() != null &&
                config.getTableDescriptionDir().exists() &&
                config.getTableDescriptionDir().isDirectory()) {
            LocalSchemaRegistry schemaRegistry =
                    new LocalSchemaRegistry(config.getTableDescriptionDir(), streamDescriptionCodec);
            schemaSuppliers.add(schemaRegistry);
            schemaRegistries.add(schemaRegistry);
        }

        if (config.getSchemaRegistryURI() != null) {
            PravegaSchemaRegistry schemaRegistry =
                    new PravegaSchemaRegistry(config.getControllerURI(), config.getSchemaRegistryURI());
            schemaSuppliers.add(schemaRegistry);
            schemaRegistries.add(schemaRegistry);
        }
    }

    @VisibleForTesting
    public CompositeSchemaRegistry(List<SchemaSupplier> schemaSuppliers, List<SchemaRegistry> schemaRegistries)
    {
        this.schemaSuppliers = schemaSuppliers;
        this.schemaRegistries = schemaRegistries;
    }

    @Override
    public List<String> listSchemas()
    {
        final List<String> schemas = new ArrayList<>();
        schemaSuppliers.forEach(p -> schemas.addAll(p.listSchemas()));
        return schemas;
    }

    @Override
    public List<PravegaTableHandle> listTables(String schema)
    {
        final List<PravegaTableHandle> tables = new ArrayList<>();
        schemaSuppliers.forEach(p -> tables.addAll(p.listTables(schema)));
        return tables;
    }

    @Override
    public List<PravegaStreamFieldGroup> getSchema(SchemaTableName schemaTableName) {
        for (SchemaRegistry schemaRegistry : schemaRegistries) {
            List<PravegaStreamFieldGroup> schema = schemaRegistry.getSchema(schemaTableName);
            if (schema != null) {
                return schema;
            }
        }
        return null;
    }

    @Override
    public PravegaStreamDescription getTable(SchemaTableName schemaTableName)
    {
        for (SchemaRegistry schemaRegistry : schemaRegistries) {
            PravegaStreamDescription streamDescription = schemaRegistry.getTable(schemaTableName);
            if (streamDescription != null) {
                return streamDescription;
            }
        }
        return null;
    }
}
