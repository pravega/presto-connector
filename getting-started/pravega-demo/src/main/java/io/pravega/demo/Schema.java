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
package io.pravega.demo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.client.exceptions.RegistryExceptions;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import org.apache.avro.util.ByteBufferInputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;

import static io.pravega.demo.DemoUtils.groupId;

public class Schema {
    public static void showSchema(PrintWriter log, SchemaRegistryClientConfig config, String scope, String stream) throws IOException  {
        SchemaRegistryClient client =
                SchemaRegistryClientFactory.withDefaultNamespace(config);

        String groupId = groupId(scope, stream);
        GroupProperties groupProperties;
        try {
            groupProperties = client.getGroupProperties(groupId);
        }
        catch (RegistryExceptions.ResourceNotFoundException e) {
            log.println("group (" + groupId + ") does not exist");
            return;
        }

        log.println(groupId + " schema; format: " + groupProperties.getSerializationFormat());

        for (SchemaWithVersion schema : client.getSchemas(groupId)) {
            switch (schema.getSchemaInfo().getSerializationFormat()) {
                case Avro:
                    org.apache.avro.Schema avroSchema =
                            new org.apache.avro.Schema.Parser().parse(
                                    new ByteArrayInputStream(schema.getSchemaInfo().getSchemaData().array()));
                    log.println(avroSchema.toString(true));
                    break;

                case Json:
                    ObjectMapper objectMapper = new ObjectMapper();
                    JsonNode node = objectMapper.readTree(
                            new ByteBufferInputStream(Collections.singletonList(schema.getSchemaInfo().getSchemaData())));
                    log.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(node));
                    break;

                default:
                    log.println("unhandled format");
                    break;
            }
        }

        log.flush();
    }
}
