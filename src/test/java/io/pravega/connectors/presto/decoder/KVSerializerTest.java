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
package io.pravega.connectors.presto.decoder;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.client.stream.Serializer;
import io.pravega.connectors.presto.integration.EmbeddedSchemaRegistry;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.json.schemas.JSONSchema;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.testng.Assert.*;

@Test
public class KVSerializerTest {
    private final EmbeddedSchemaRegistry schemaRegistry;

    public KVSerializerTest() {
        this.schemaRegistry = new EmbeddedSchemaRegistry();
        this.schemaRegistry.start();
    }

    private static class Employee {
        public int id;
        public String first;
        public String last;

        @JsonCreator
        public Employee(@JsonProperty("id") int id,
                        @JsonProperty("first") String first,
                        @JsonProperty("last") String last) {
            this.id = id;
            this.first = first;
            this.last = last;
        }

        @JsonProperty
        public int getId() {
            return id;
        }

        @JsonProperty
        public String getFirst() {
            return first;
        }

        @JsonProperty
        public String getLast() {
            return last;
        }
    }

    @Test
    public void testJson() throws IOException
    {
        Employee expected = new Employee(1, "John", "Smith");
        ByteBuffer serialized = ByteBuffer.wrap(new ObjectMapper().writeValueAsBytes(expected));

        JsonSerializer jsonSerializer = new JsonSerializer(null /* no need to pass config - won't fall back */);

        JsonNode actual = jsonSerializer.deserialize(serialized);

        assertEquals(actual.get("id").asInt(), expected.id);
        assertEquals(actual.get("first").asText(), expected.first);
        assertEquals(actual.get("last").asText(), expected.last);

        assertFalse(jsonSerializer.schemaRegistryDeserializer());
    }

    @Test
    public void testJsonInline() {
        SerializerConfig serializerConfig = jsonGroup("inline");

        Serializer<Employee> serializer =
                SerializerFactory.jsonSerializer(serializerConfig, JSONSchema.of(Employee.class));

        Employee expected = new Employee(1, "John", "Smith");
        ByteBuffer serialized = serializer.serialize(expected);

        JsonSerializer jsonSerializer = new JsonSerializer(serializerConfig);

        JsonNode actual = jsonSerializer.deserialize(serialized);

        assertEquals(actual.get("id").asInt(), expected.id);
        assertEquals(actual.get("first").asText(), expected.first);
        assertEquals(actual.get("last").asText(), expected.last);

        assertTrue(jsonSerializer.schemaRegistryDeserializer());
    }

    private SerializerConfig jsonGroup(String stream) {
        String groupId = "json." + stream;
        addGroup(groupId, SerializationFormat.Json);
        return serializerConfig(groupId);
    }

    private void addGroup(String groupId, SerializationFormat format)
    {
        schemaRegistry.client().addGroup(groupId,
                new GroupProperties(
                        format,
                        Compatibility.allowAny(),
                        true));
    }

    private SerializerConfig serializerConfig(String group) {
        return SerializerConfig.builder()
                .groupId(group).registryConfig(SchemaRegistryClientConfig.builder()
                        .schemaRegistryUri(schemaRegistry.getURI())
                        .build())
                .registerSchema(true)
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        schemaRegistry.close();
    }
}
