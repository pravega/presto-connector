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
import io.pravega.connectors.presto.PravegaStreamDescription;
import io.pravega.connectors.presto.PravegaStreamFieldDescription;
import io.pravega.connectors.presto.PravegaStreamFieldGroup;
import io.pravega.connectors.presto.PravegaTableHandle;
import io.pravega.connectors.presto.util.SchemaRegistryUtil;
import org.testng.annotations.Test;

import java.util.List;

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
        SchemaRegistryUtil schemaRegistryUtil = new SchemaRegistryUtil();

        schemaRegistryUtil.addSchema("schema1");
        schemaRegistryUtil.addSchema("schema2");

        List<String> schemas = schemaRegistryUtil.getSchemaRegistry().listSchemas();
        assertEquals(schemas.size(), 2);
        assertEquals("schema1", schemas.get(0));
        assertEquals("schema2", schemas.get(1));
    }

    @Test
    public void testListTables()
    {
        SchemaRegistryUtil schemaRegistryUtil = new SchemaRegistryUtil();

        schemaRegistryUtil.addTable("schema1", "stream1");
        schemaRegistryUtil.addTable("schema2", "stream2");
        // stream starting with '_' is internal/hidden
        schemaRegistryUtil.addTable("schema2", "_markStream2");

        List<PravegaTableHandle> tables = schemaRegistryUtil.getSchemaRegistry().listTables("schema2");
        assertEquals(tables.size(), 1);
        assertEquals("stream2", tables.get(0).getObjectName());
    }

    @Test
    public void testGetTable()
    {
        SchemaRegistryUtil schemaRegistryUtil = new SchemaRegistryUtil();

        SchemaTableName schemaTableName = new SchemaTableName("hr", "employee");
        schemaRegistryUtil.addAvroSchema(schemaTableName, EMPLOYEE_AVSC);

        SchemaRegistry schemaRegistry = schemaRegistryUtil.getSchemaRegistry();

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
}
