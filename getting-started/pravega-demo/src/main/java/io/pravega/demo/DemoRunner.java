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

import com.google.common.collect.ImmutableMap;
import io.pravega.client.stream.Serializer;
import io.pravega.demo.avro.Inventory;
import io.pravega.demo.objects.Transaction;
import io.pravega.demo.objects.Sensor;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.avro.schemas.AvroSchema;
import io.pravega.schemaregistry.serializer.json.schemas.JSONSchema;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static io.pravega.demo.DemoUtils.*;

public class DemoRunner {
    private final static String INVENTORY_STREAM = "inventory";

    private final static String SENSOR_STREAM = "sensor";

    private final static String TRANSACTIONS_STREAM = "transactions";

    private final URI controllerUri;
    private final URI schemaRegistryUri;
    private final String scope;

    private final Random rnd = new Random();

    public DemoRunner(URI controllerUri, URI schemaRegistryUri, String scope) {
        this.controllerUri = controllerUri;
        this.schemaRegistryUri = schemaRegistryUri;
        this.scope = scope;
    }

    public void run() throws IOException {

        PrintWriter log = new PrintWriter(System.out);

        SchemaRegistryClientConfig schemaRegistryConfig =
                SchemaRegistryClientConfig.builder().schemaRegistryUri(schemaRegistryUri).build();

        // nothing works without schema registry, wait for it
        // if SR is up, pravega from our docker-compose would be up
        waitForSchemaRegistry(log, schemaRegistryConfig);

        // case 1
        // we write events using Serializers provided by SchemaRegistry
        // we do not have to maintain the schema ourselves
        // event will be encoded along with some metadata
        new DataIngest<Inventory>(controllerUri).ingest(log,
                scope, INVENTORY_STREAM,
                inventorySchemaRegistrySerializer(schemaRegistryConfig, scope, INVENTORY_STREAM),
                inventoryDataSupplier());

        Schema.showSchema(log, schemaRegistryConfig, scope, INVENTORY_STREAM);


        // case 2
        // we serialize events ourselves
        // schema is persisted in SchemaRegistry explicitly
        new DataIngest<Sensor>(controllerUri).ingest(log,
                scope, SENSOR_STREAM,
                sensorSerializer(schemaRegistryConfig, scope, SENSOR_STREAM),
                sensorDataSupplier());

        Schema.showSchema(log, schemaRegistryConfig, scope, SENSOR_STREAM);


        // case 3
        // we serialize events ourselves
        // no schema is persisted
        new DataIngest<Transaction>(controllerUri).ingest(log,
                scope, TRANSACTIONS_STREAM,
                new JsonSerializer<>(),
                txnDataSupplier());

        // no schema to show
    }

    Serializer<Sensor> sensorSerializer(SchemaRegistryClientConfig schemaRegistryConfig,
                                        String scope, String stream) {
        addSchemaRegistryGroupIfAbsent(schemaRegistryConfig,
                SerializationFormat.Json,
                scope, stream, null);

        SchemaRegistryClient schemaRegistryClient =
                SchemaRegistryClientFactory.withDefaultNamespace(schemaRegistryConfig);
        schemaRegistryClient.addSchema(groupId(scope, stream), JSONSchema.of(Sensor.class).getSchemaInfo());

        return new JsonSerializer<>();
    }

    Serializer<Inventory> inventorySchemaRegistrySerializer(SchemaRegistryClientConfig schemaRegistryConfig,
                                                          String scope, String stream) {
        // https://github.com/pravega/presto-connector/issues/20
        ImmutableMap<String, String> properties =
                ImmutableMap.<String, String>builder().put("inline", "true").build();
        addSchemaRegistryGroupIfAbsent(schemaRegistryConfig,
                SerializationFormat.Avro,
                scope, stream,
                properties);

        SerializerConfig serializerConfig =
                SerializerConfig.builder()
                        .registryConfig(schemaRegistryConfig)
                        .groupId(groupId(scope, stream))
                        .registerSchema(true)
                        .build();

        return SerializerFactory.avroSerializer(serializerConfig, AvroSchema.of(Inventory.class));
    }

    Supplier<Inventory> inventoryDataSupplier() {
        AtomicInteger i = new AtomicInteger();
        final int eventsToWrite = 1000;
        return () -> i.incrementAndGet() <= eventsToWrite
                ? Inventory.newBuilder()
                .setProductId(rnd.nextInt(100))
                .setOriginSrc(rnd.nextInt(10))
                .setQuantity(rnd.nextInt(10000))
                .build()
                : null;
    }

    Supplier<Sensor> sensorDataSupplier() {
        AtomicInteger i = new AtomicInteger();
        final int eventsToWrite = 1000;
        return () -> i.incrementAndGet() <= eventsToWrite
                ? new Sensor("sensor" + rnd.nextInt(10), rnd.nextLong(), System.currentTimeMillis())
                : null;
    }

    Supplier<Transaction> txnDataSupplier() {
        AtomicInteger i = new AtomicInteger();
        final int eventsToWrite = 1000;
        return () -> i.incrementAndGet() <= eventsToWrite
                ? new Transaction(rnd.nextInt() /* txnId */,
                rnd.nextInt(20) /* productId */,
                "location" + RandomStringUtils.randomAlphabetic(1).toUpperCase(),
                System.currentTimeMillis())
                : null;
    }
}
