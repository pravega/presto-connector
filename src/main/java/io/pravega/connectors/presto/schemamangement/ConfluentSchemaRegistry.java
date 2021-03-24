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
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.pravega.connectors.presto.ObjectType;
import io.pravega.connectors.presto.PravegaStreamDescription;
import io.pravega.connectors.presto.PravegaStreamFieldDescription;
import io.pravega.connectors.presto.PravegaStreamFieldGroup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static io.pravega.connectors.presto.util.PravegaNameUtils.temp_tableNameToStreamName;
import static io.pravega.connectors.presto.util.PravegaSchemaUtils.AVRO;
import static io.pravega.connectors.presto.util.PravegaStreamDescUtils.mapFieldsFromSchema;

public class ConfluentSchemaRegistry
        implements SchemaRegistry
{
    private final SchemaRegistryClient schemaRegistryClient;

    public ConfluentSchemaRegistry(URI registryURI)
    {
        this.schemaRegistryClient = new CachedSchemaRegistryClient(registryURI.toASCIIString(), Integer.MAX_VALUE);
    }

    @Override
    public List<PravegaStreamFieldGroup> getSchema(SchemaTableName schemaTableName)
    {
        try {
            SchemaMetadata metadata = schemaRegistryClient.getLatestSchemaMetadata(format(schemaTableName));
            if (!metadata.getSchemaType().equalsIgnoreCase(AVRO)) {
                throw new UnsupportedOperationException("schema type '" + metadata.getSchemaType() + "' is not supported");
            }

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
        catch (RestClientException e) {
            if (e.getStatus() == 404) {
                return null;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public PravegaStreamDescription getTable(SchemaTableName schemaTableName)
    {
        List<PravegaStreamFieldGroup> schema = getSchema(schemaTableName);
        if (schema == null) {
            return null;
        }

        return new PravegaStreamDescription(
                schemaTableName.getTableName(),
                Optional.of(schemaTableName.getSchemaName()),
                temp_tableNameToStreamName(schemaTableName.getTableName()),
                Optional.of(ObjectType.STREAM),
                Optional.empty() /* args */,
                Optional.of(schema));
    }

    static String format(SchemaTableName schemaTableName)
    {
        return String.format("%s-%s", schemaTableName.getSchemaName(), schemaTableName.getTableName());
    }
}