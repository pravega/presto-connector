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
package io.pravega.connectors.presto.util;

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;
import io.pravega.client.admin.StreamManager;
import io.pravega.connectors.presto.schemamanagement.CompositeSchemaRegistry;
import io.pravega.connectors.presto.schemamanagement.LocalSchemaRegistry;
import io.pravega.connectors.presto.schemamanagement.PravegaSchemaRegistry;
import io.pravega.connectors.presto.schemamanagement.SchemaRegistry;
import io.pravega.connectors.presto.schemamanagement.SchemaSupplier;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;

import java.util.ArrayList;
import java.util.List;

import static io.pravega.connectors.presto.util.PravegaTestUtils.avroSchema;

/**
 * build CompositeSchemaRegistry for use in unit tests
 */
public class SchemaRegistryUtil
{
    private final StreamManager streamManager;
    private final SchemaRegistryClient schemaRegistryClient;
    private final PravegaSchemaRegistry pravegaSchemaRegistry;

    private final List<LocalSchemaRegistry> localSchemaRegistryList;

    public SchemaRegistryUtil()
    {
        this.streamManager = new MockStreamManager();
        this.schemaRegistryClient = new MockSchemaRegistryClient();

        this.pravegaSchemaRegistry = new PravegaSchemaRegistry(schemaRegistryClient, streamManager);

        this.localSchemaRegistryList = new ArrayList<>();
    }

    public CompositeSchemaRegistry getSchemaRegistry()
    {
        List<SchemaSupplier> schemaSuppliers = new ArrayList<>();
        List<SchemaRegistry> schemaRegistries = new ArrayList<>();

        localSchemaRegistryList.forEach(lsr -> {
            schemaSuppliers.add(lsr);
            schemaRegistries.add(lsr);
        });

        schemaSuppliers.add(pravegaSchemaRegistry);
        schemaRegistries.add(pravegaSchemaRegistry);

        return new CompositeSchemaRegistry(schemaSuppliers, schemaRegistries);
    }

    public void addLocalSchema(String dir)
    {
        localSchemaRegistryList.add(PravegaTestUtils.localSchemaRegistry(dir));
    }

    public boolean addSchema(String schema)
    {
        return streamManager.createScope(schema);
    }

    public boolean addTable(SchemaTableName schemaTableName)
    {
        return streamManager.createStream(schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                null);
    }

    public boolean addTable(String schema, String stream)
    {
        return addTable(new SchemaTableName(schema, stream));
    }

    public boolean addTable(SchemaTableName schemaTableName, String schema)
    {
        if (!addTable(schemaTableName)) {
            return false;
        }
        addAvroSchema(schemaTableName, schema);
        return true;
    }

    public void addAvroSchema(SchemaTableName schemaTableName, String schema)
    {
        schemaRegistryClient.addGroup(groupId(schemaTableName), groupProperties(false));
        schemaRegistryClient.addSchema(groupId(schemaTableName), AvroSchema.of(avroSchema(schema)).getSchemaInfo());
    }

    private static GroupProperties groupProperties(boolean inline)
    {
        return new GroupProperties(
                SerializationFormat.Avro,
                Compatibility.allowAny(),
                false,
                ImmutableMap.<String, String>builder().put(inline ? "inline" : "", "").build());
    }

    private static String groupId(SchemaTableName schemaTableName)
    {
        return PravegaNameUtils.groupId(schemaTableName.getSchemaName(), schemaTableName.getTableName());
    }
}
