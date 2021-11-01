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

import io.trino.plugin.pravega.PravegaStreamDescription;
import io.trino.plugin.pravega.PravegaStreamFieldDescription;
import io.trino.plugin.pravega.PravegaStreamFieldGroup;
import io.trino.plugin.pravega.PravegaTableHandle;
import io.trino.plugin.pravega.util.PravegaTestUtils;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.*;

@Test
public class TestLocalSchemaRegistry
{
    // uses resources/tpch for table description dir

    @Test
    public void testListSchemas()
    {
        LocalSchemaRegistry schemaRegistry = PravegaTestUtils.localSchemaRegistry("tpch");

        List<String> schemas = schemaRegistry.listSchemas();
        assertEquals(schemas.size(), 1);
        assertEquals(schemas.get(0), "tpch");
    }

    @Test
    public void testListTables()
    {
        LocalSchemaRegistry schemaRegistry = PravegaTestUtils.localSchemaRegistry("tpch");

        List<PravegaTableHandle> tables = schemaRegistry.listTables("tpch");
        assertEquals(tables.size(), 8);

        PravegaTableHandle customerTableHandle =
                tables.stream().filter(h -> h.getTableName().equals("customer")).findFirst().get();
        assertEquals(customerTableHandle.getSchemaName(), "tpch");
        assertEquals(customerTableHandle.getTableName(), "customer");
        assertEquals(customerTableHandle.getObjectName(), "customer");
    }

    @Test
    public void testGetSchema()
    {
        LocalSchemaRegistry schemaRegistry = PravegaTestUtils.localSchemaRegistry("tpch");

        List<PravegaStreamFieldGroup> schema =
                schemaRegistry.getSchema(new SchemaTableName("tpch", "customer"));

        assertNotNull(schema);
        assertEquals(1, schema.size());

        validateCustomerSchema(schema.get(0));
    }

    @Test
    public void testGetTable()
    {
        LocalSchemaRegistry schemaRegistry = PravegaTestUtils.localSchemaRegistry("tpch");

        PravegaStreamDescription table =
                schemaRegistry.getTable(new SchemaTableName("tpch", "customer"));
        assertNotNull(table);

        assertTrue(table.getEvent().isPresent());
        assertEquals(1, table.getEvent().get().size());

        validateCustomerSchema(table.getEvent().get().get(0));
    }

    @Test
    public void testTableDoesNotExist()
    {
        LocalSchemaRegistry schemaRegistry = PravegaTestUtils.localSchemaRegistry("tpch");
        assertNull(schemaRegistry.getTable(new SchemaTableName("tpch", "abcxyz123")));
    }

    private void validateCustomerSchema(PravegaStreamFieldGroup fieldGroup)
    {
        // spot check a fiew fields

        assertEquals(fieldGroup.getDataFormat(), "json");
        assertEquals(fieldGroup.getFields().size(), 8);

        PravegaStreamFieldDescription field = fieldGroup.getFields().get(0);
        assertEquals(field.getName(), "custkey");
        assertEquals(field.getMapping(), "custkey");
        assertTrue(field.getType() instanceof BigintType);

        field = fieldGroup.getFields().get(6);
        assertEquals(field.getName(), "mktsegment");
        assertEquals(field.getMapping(), "mktsegment");
        assertTrue(field.getType() instanceof VarcharType);
    }
}
