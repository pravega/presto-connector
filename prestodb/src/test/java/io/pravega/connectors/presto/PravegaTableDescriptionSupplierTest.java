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

import com.facebook.presto.spi.SchemaTableName;
import io.pravega.connectors.presto.util.SchemaRegistryUtil;
import org.testng.annotations.Test;

import java.util.List;

import static io.pravega.connectors.presto.util.TestSchemas.EMPLOYEE_AVSC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test
public class PravegaTableDescriptionSupplierTest
{
    private static final String SCHEMA = "ut";

    @Test
    public void testTableDoesNotExist()
    {
        PravegaTableDescriptionSupplier tableSupplier =
                new PravegaTableDescriptionSupplier(new SchemaRegistryUtil().getSchemaRegistry());
        assertNull(tableSupplier.getTable(new SchemaTableName(SCHEMA, "stream1")));
    }

    @Test
    public void testMultiSourceStreamRegex()
    {
        SchemaRegistryUtil schemaRegistryUtil = new SchemaRegistryUtil();
        schemaRegistryUtil.addLocalSchema(SCHEMA);

        PravegaTableDescriptionSupplier tableSupplier =
                new PravegaTableDescriptionSupplier(schemaRegistryUtil.getSchemaRegistry());


        schemaRegistryUtil.addSchema(SCHEMA);
        schemaRegistryUtil.addTable(new SchemaTableName(SCHEMA, "stream1"), EMPLOYEE_AVSC);
        schemaRegistryUtil.addTable(new SchemaTableName(SCHEMA, "stream2"), EMPLOYEE_AVSC);
        schemaRegistryUtil.addTable(new SchemaTableName(SCHEMA, "stream3"), EMPLOYEE_AVSC);

        PravegaStreamDescription table =
                tableSupplier.getTable(new SchemaTableName(SCHEMA, "multiregex"));

        assertNotNull(table);
        assertTrue(table.getObjectArgs().isPresent());

        List<String> components = table.getObjectArgs().get();
        assertEquals(components.size(), 3);
        assertEquals(components.stream().sorted().toArray(), new String[]{"stream1", "stream2", "stream3"});
    }

    @Test
    public void testMultiSourceStreamExplicit()
    {
        // same setup as regex.  but multi source def. only has 2 component streams.
        SchemaRegistryUtil schemaRegistryUtil = new SchemaRegistryUtil();
        schemaRegistryUtil.addLocalSchema(SCHEMA);

        PravegaTableDescriptionSupplier tableSupplier =
                new PravegaTableDescriptionSupplier(schemaRegistryUtil.getSchemaRegistry());

        schemaRegistryUtil.addSchema(SCHEMA);
        schemaRegistryUtil.addTable(new SchemaTableName(SCHEMA, "stream1"), EMPLOYEE_AVSC);
        schemaRegistryUtil.addTable(new SchemaTableName(SCHEMA, "stream2"), EMPLOYEE_AVSC);
        schemaRegistryUtil.addTable(new SchemaTableName(SCHEMA, "stream3"), EMPLOYEE_AVSC);

        PravegaStreamDescription table =
                tableSupplier.getTable(new SchemaTableName(SCHEMA, "multiexplicit"));

        assertNotNull(table);
        assertTrue(table.getObjectArgs().isPresent());

        List<String> components = table.getObjectArgs().get();
        assertEquals(components.size(), 2);
        assertEquals(components.stream().sorted().toArray(), new String[]{"stream1", "stream3"});
    }
}
