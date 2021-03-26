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
package io.pravega.connectors.presto.schemamanagement;

import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;
import io.pravega.client.admin.StreamManager;
import io.pravega.connectors.presto.PravegaStreamDescription;
import io.pravega.connectors.presto.PravegaStreamFieldDescription;
import io.pravega.connectors.presto.PravegaStreamFieldGroup;
import io.pravega.connectors.presto.PravegaTableHandle;
import io.pravega.connectors.presto.util.MockSchemaRegistryClient;
import io.pravega.connectors.presto.util.MockStreamManager;
import io.pravega.connectors.presto.util.PravegaNameUtils;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import org.testng.annotations.Test;

import java.util.List;

import static io.pravega.connectors.presto.util.PravegaTestUtils.avroSchema;
import static io.pravega.connectors.presto.util.TestSchemas.EMPLOYEE_AVSC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test
public class PravegaSchemaRegistryTest
{
    @Test
    public void testListSchemas()
    {
        StreamManager streamManager = new MockStreamManager();

        streamManager.createScope("schema1");
        streamManager.createScope("schema2");

        PravegaSchemaRegistry schemaRegistry = new PravegaSchemaRegistry(new MockSchemaRegistryClient(), streamManager);

        List<String> schemas = schemaRegistry.listSchemas();
        assertEquals(schemas.size(), 2);
        assertEquals("schema1", schemas.get(0));
        assertEquals("schema2", schemas.get(1));
    }

    @Test
    public void testListTables()
    {
        StreamManager streamManager = new MockStreamManager();

        streamManager.createStream("schema1", "stream1", null);
        streamManager.createStream("schema2", "stream2", null);
        // stream starting with '_' is internal/hidden
        streamManager.createStream("schema2", "_markStream2", null);

        PravegaSchemaRegistry schemaRegistry = new PravegaSchemaRegistry(new MockSchemaRegistryClient(), streamManager);
        List<PravegaTableHandle> tables = schemaRegistry.listTables("schema2");
        assertEquals(tables.size(), 1);
        assertEquals("stream2", tables.get(0).getObjectName());
    }

    @Test
    public void testGetTable()
    {
        SchemaTableName schemaTableName = new SchemaTableName("hr", "employee");

        SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
        schemaRegistryClient.addGroup(groupId(schemaTableName), groupProperties(false));
        schemaRegistryClient.addSchema(groupId(schemaTableName), AvroSchema.of(avroSchema(EMPLOYEE_AVSC)).getSchemaInfo());


        PravegaSchemaRegistry schemaRegistry = new PravegaSchemaRegistry(schemaRegistryClient, new MockStreamManager());

        PravegaStreamDescription table = schemaRegistry.getTable(schemaTableName);

        assertNotNull(table);
        assertTrue(table.getEvent().isPresent());
        assertEquals(table.getEvent().get().size(), 1);

        PravegaStreamFieldGroup fieldGroup = table.getEvent().get().get(0);
        assertEquals(fieldGroup.getFields().size(), 2);

        PravegaStreamFieldDescription field = fieldGroup.getFields().get(0);
        assertEquals(field.getName(), "first");
        assertTrue(field.getType() instanceof VarcharType);

        field = fieldGroup.getFields().get(1);
        assertEquals(field.getName(), "last");
        assertTrue(field.getType() instanceof VarcharType);
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
