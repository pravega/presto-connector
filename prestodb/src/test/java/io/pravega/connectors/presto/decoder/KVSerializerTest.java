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
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.pravega.avro.Sensor;
import io.pravega.client.stream.Serializer;
import io.pravega.connectors.presto.integration.EmbeddedSchemaRegistry;
import io.pravega.connectors.presto.util.PravegaSerializationUtils;
import io.pravega.protobuf.ProductOuterClass;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.exceptions.RegistryExceptions;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.json.schemas.JSONSchema;
import io.pravega.schemaregistry.serializer.protobuf.schemas.ProtobufSchema;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static io.pravega.connectors.presto.ProtobufCommon.encodeSchema;
import static org.testng.Assert.*;

/**
 * when an object is written to pravega using serializer from SchemaRegistry, encoding info
 * will be added to the raw event data so the event can be deserialized automatically later
 * this is referred to in the connector code as "inline"
 *
 * when reading data in the connector, we don't know how event was serialized
 * so first try with the actual avro, protobuf, or json schema
 * if that fails fallback to trying with SchemaRegistry SerializerConfig
 */
@Test
public class KVSerializerTest {
    private final EmbeddedSchemaRegistry schemaRegistry;

    private final Random random = new Random();

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
    public void testJson() throws IOException {
        Employee expected = new Employee(1, "John", "Smith");

        ByteBuffer serialized = ByteBuffer.wrap(new ObjectMapper().writeValueAsBytes(expected));

        // use null SerializerConfig as we are not expecting initial deserialize to fail
        JsonSerializer jsonSerializer = new JsonSerializer(null);

        JsonNode actual = jsonSerializer.deserialize(serialized);

        assertEquals(actual.get("id").asInt(), expected.id);
        assertEquals(actual.get("first").asText(), expected.first);
        assertEquals(actual.get("last").asText(), expected.last);

        assertFalse(jsonSerializer.schemaRegistryDeserializer());
    }

    @Test
    public void testJsonInline()
    {
        Employee expected = new Employee(1, "John", "Smith");

        SerializerConfig serializerConfig = jsonGroup("inline");
        Serializer<Employee> schemaRegistrySerializer =
                SerializerFactory.jsonSerializer(serializerConfig, JSONSchema.of(Employee.class));
        // using Serializer provided by SchemaRegistry
        ByteBuffer serializedValue = schemaRegistrySerializer.serialize(expected);

        JsonSerializer jsonSerializer = new JsonSerializer(serializerConfig);

        JsonNode actual = jsonSerializer.deserialize(serializedValue);

        assertEquals(actual.get("id").asInt(), expected.id);
        assertEquals(actual.get("first").asText(), expected.first);
        assertEquals(actual.get("last").asText(), expected.last);

        assertTrue(jsonSerializer.schemaRegistryDeserializer());
    }

    @Test
    public void testAvro()
    {
        Sensor sensor = new Sensor();
        sensor.setSensorId(random.nextInt());
        sensor.setTimestamp(System.currentTimeMillis());
        sensor.setRate(random.nextDouble());

        ByteBuffer serializedValue = PravegaSerializationUtils.serialize((GenericRecord) sensor);

        // use null SerializerConfig as we are expecting parse with given schema to succeed
        AvroSerializer avroSerializer = new AvroSerializer(null, sensor.getSchema().toString());
        GenericRecord actual = avroSerializer.deserialize(serializedValue);

        assertEquals(actual.get("sensorId"), sensor.getSensorId());
        assertEquals(actual.get("timestamp"), sensor.getTimestamp());
        assertEquals(actual.get("rate"), sensor.getRate());

        assertFalse(avroSerializer.schemaRegistryDeserializer());
    }

    @Test
    public void testAvroInline()
    {
        Sensor sensor = new Sensor();
        sensor.setSensorId(random.nextInt());
        sensor.setTimestamp(System.currentTimeMillis());
        sensor.setRate(random.nextDouble());

        SerializerConfig serializerConfig = avroGroup("inline");
        Serializer<Sensor> schemaRegistrySerializer =
                SerializerFactory.avroSerializer(serializerConfig, AvroSchema.of(Sensor.class));
        // using Serializer provided by SchemaRegistry
        ByteBuffer serializedValue = schemaRegistrySerializer.serialize(sensor);

        AvroSerializer avroSerializer = new AvroSerializer(serializerConfig, sensor.getSchema().toString());
        GenericRecord actual = avroSerializer.deserialize(serializedValue);

        assertEquals(actual.get("sensorId"), sensor.getSensorId());
        assertEquals(actual.get("timestamp"), sensor.getTimestamp());
        assertEquals(actual.get("rate"), sensor.getRate());

        assertTrue(avroSerializer.schemaRegistryDeserializer());
    }

    @Test
    public void testProtobufInline()
    {
        ProductOuterClass.Product product = ProductOuterClass.Product.newBuilder()
                .setId(random.nextInt())
                .setName(UUID.randomUUID().toString())
                .build();

        SerializerConfig serializerConfig = protobufGroup("inline");
        Serializer<ProductOuterClass.Product> schemaRegistrySerializer =
                SerializerFactory.protobufSerializer(serializerConfig, ProtobufSchema.of(ProductOuterClass.Product.class));
        // using Serializer provided by SchemaRegistry
        ByteBuffer serializedValue = schemaRegistrySerializer.serialize(product);

        // get file descriptor, which is needed for initial DynamicMessage#parseFrom
        String schema = encodeSchema(schemaRegistry.client().getSchemas("protobuf.inline").get(0));

        ProtobufSerializer protobufSerializer = new ProtobufSerializer(serializerConfig, schema);
        DynamicMessage dynamicMessage = protobufSerializer.deserialize(serializedValue);

        int id = -1;
        String name = null;
        for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : dynamicMessage.getAllFields().entrySet()) {
            if (entry.getKey().getJsonName().equals("id")) {
                id = (int) entry.getValue();
            }
            else if (entry.getKey().getJsonName().equals("name")) {
                name = (String) entry.getValue();
            }
        }
        assertNotEquals(id, -1);
        assertNotNull(name);

        assertEquals(id, product.getId());
        assertEquals(name, product.getName());

        assertTrue(protobufSerializer.schemaRegistryDeserializer());
    }

    private SerializerConfig avroGroup(String stream)
    {
        String groupId = "avro." + stream;
        addGroup(groupId, SerializationFormat.Avro);
        return serializerConfig(groupId);
    }

    private SerializerConfig protobufGroup(String stream)
    {
        String groupId = "protobuf." + stream;
        addGroup(groupId, SerializationFormat.Protobuf);
        return serializerConfig(groupId);
    }

    private SerializerConfig jsonGroup(String stream)
    {
        String groupId = "json." + stream;
        addGroup(groupId, SerializationFormat.Json);
        return serializerConfig(groupId);
    }

    private void addGroup(String groupId, SerializationFormat format)
    {
        try {
            schemaRegistry.client().getGroupProperties(groupId);
        }
        catch (RegistryExceptions.ResourceNotFoundException e) {
            schemaRegistry.client().addGroup(groupId,
                    new GroupProperties(
                            format,
                            Compatibility.allowAny(),
                            true));
        }
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
