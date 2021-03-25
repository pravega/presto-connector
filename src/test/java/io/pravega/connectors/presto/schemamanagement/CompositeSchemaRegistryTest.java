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

import com.facebook.presto.spi.SchemaTableName;
import io.pravega.connectors.presto.ObjectType;
import io.pravega.connectors.presto.PravegaStreamDescription;
import io.pravega.connectors.presto.PravegaStreamFieldGroup;
import io.pravega.connectors.presto.PravegaTableHandle;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Test
public class CompositeSchemaRegistryTest
{
    @Test
    public void testOne()
    {
        List<SchemaSupplier> schemaSuppliers = new ArrayList<>();
        List<SchemaRegistry> schemaRegistries = new ArrayList<>();

        schemaSuppliers.add(new SchemaSupplier() {
            @Override
            public List<String> listSchemas() {
                return Collections.singletonList("schema1");
            }

            @Override
            public List<PravegaTableHandle> listTables(String schema) {
                return Collections.singletonList(new PravegaTableHandle("schema1", "table1", "table1", ObjectType.STREAM, Optional.empty()));
            }
        });

        schemaRegistries.add(new SchemaRegistry() {
            @Override
            public PravegaStreamDescription getTable(SchemaTableName schemaTableName) {
                return null;
            }

            @Override
            public List<PravegaStreamFieldGroup> getSchema(SchemaTableName schemaTableName) {
                return null;
            }
        });
    }
}
