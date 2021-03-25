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

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.SchemaTableName;
import io.pravega.connectors.presto.PravegaStreamDescription;
import io.pravega.connectors.presto.PravegaStreamFieldDescription;
import io.pravega.connectors.presto.PravegaStreamFieldGroup;
import io.pravega.connectors.presto.PravegaTableHandle;
import io.pravega.connectors.presto.util.PravegaTestUtils;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test
public class LocalSchemaRegistryTest
{
    @Test
    public void testListSchemas()
    {
        LocalSchemaRegistry schemaRegistry = PravegaTestUtils.localSchemaRegistry();

        List<String> schemas = schemaRegistry.listSchemas();

        assertEquals(schemas.size(), 2);

        schemas = schemas.stream().sorted().collect(Collectors.toList());
        assertEquals(schemas.get(0), "kv");
        assertEquals(schemas.get(1), "tpch");
    }

    @Test
    public void testListTables()
    {
        LocalSchemaRegistry schemaRegistry = PravegaTestUtils.localSchemaRegistry();

        List<PravegaTableHandle> tables = schemaRegistry.listTables("tpch");
        assertEquals(tables.size(), 8);

        PravegaTableHandle customerTableHandle =
                tables.stream().filter(h -> h.getTableName().equals("customer")).findFirst().get();
        assertEquals(customerTableHandle.getSchemaName(), "tpch");
        assertEquals(customerTableHandle.getTableName(), "customer");
    }

    @Test
    public void testGetSchema()
    {
        LocalSchemaRegistry schemaRegistry = PravegaTestUtils.localSchemaRegistry();

        List<PravegaStreamFieldGroup> schema =
                schemaRegistry.getSchema(new SchemaTableName("tpch", "customer"));

        assertNotNull(schema);
        assertEquals(1, schema.size());

        validateCustomerSchema(schema.get(0));
    }

    @Test
    public void testGetTable()
    {
        LocalSchemaRegistry schemaRegistry = PravegaTestUtils.localSchemaRegistry();

        PravegaStreamDescription table =
                schemaRegistry.getTable(new SchemaTableName("tpch", "customer"));
        assertNotNull(table);

        assertTrue(table.getEvent().isPresent());
        assertEquals(1, table.getEvent().get().size());

        validateCustomerSchema(table.getEvent().get().get(0));
    }

    private void validateCustomerSchema(PravegaStreamFieldGroup fieldGroup)
    {
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
