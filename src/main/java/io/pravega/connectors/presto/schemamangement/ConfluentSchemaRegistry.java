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

import com.facebook.presto.spi.SchemaTableName;
import io.pravega.connectors.presto.PravegaStreamDescription;
import io.pravega.connectors.presto.PravegaStreamFieldGroup;

import java.net.URI;
import java.util.List;

public class ConfluentSchemaRegistry
        implements SchemaRegistry
{
    //private final SchemaRegistryClient schemaRegistryClient;

    public ConfluentSchemaRegistry(URI registryURI)
    {
        //this.schemaRegistryClient = new CachedSchemaRegistryClient(registryURI.toASCIIString(), Integer.MAX_VALUE);
    }

    @Override
    public List<PravegaStreamFieldGroup> getSchema(SchemaTableName schemaTableName)
    {
        /*
        try {
            SchemaMetadata metadata = schemaRegistryClient.getLatestSchemaMetadata(format(schemaTableName));

            List<PravegaStreamFieldDescription> fields =
                    mapFieldsFromSchema("", AVRO, metadata.getSchema());

            return Collections.singletonList(
                    new PravegaStreamFieldGroup(AVRO,
                            Optional.empty(),
                            Optional.of(metadata.getSchema()),
                            Optional.of(fields)));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
         */
        return null;
    }

    @Override
    public PravegaStreamDescription getTable(SchemaTableName schemaTableName)
    {
        return null;
    }

    static String format(SchemaTableName schemaTableName)
    {
        return String.format("%s-%s", schemaTableName.getSchemaName(), schemaTableName.getTableName());
    }
}