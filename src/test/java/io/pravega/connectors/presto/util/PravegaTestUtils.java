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
package io.pravega.connectors.presto.util;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.tests.TestingPrestoClient;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import io.pravega.client.admin.StreamManager;
import io.pravega.connectors.presto.PravegaStreamDescription;
import io.pravega.connectors.presto.PravegaStreamFieldDescription;
import io.pravega.connectors.presto.PravegaStreamFieldGroup;
import io.pravega.connectors.presto.integration.PravegaKeyValueLoader;
import io.pravega.connectors.presto.integration.PravegaLoader;
import io.pravega.connectors.presto.schemamanagement.LocalSchemaRegistry;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static io.pravega.connectors.presto.util.PravegaStreamDescUtils.mapFieldsFromSchema;
import static java.lang.String.format;

public final class PravegaTestUtils
{
    private PravegaTestUtils() {}

    public static LocalSchemaRegistry localSchemaRegistry()
    {
        JsonCodec<PravegaStreamDescription> streamDescCodec = new CodecSupplier<>(
                PravegaStreamDescription.class,
                FunctionAndTypeManager.createTestFunctionAndTypeManager()).get();
        return new LocalSchemaRegistry(new File("src/test/resources/etc").getAbsoluteFile(), streamDescCodec);
    }

    public static PravegaStreamDescription getStreamDesc(JsonCodec<PravegaStreamDescription> streamDescriptionCodec, String schema, String table)
    {
        try (InputStream inputStream = PravegaTestUtils.class.getResourceAsStream(String.format("/etc/%s.%s.json", schema, table))) {
            return streamDescriptionCodec.fromJson(ByteStreams.toByteArray(inputStream));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static String readSchema(String schema)
    {
        try (InputStreamReader reader = new InputStreamReader(PravegaTestUtils.class.getResourceAsStream(String.format("/etc/%s", schema)))) {
            return CharStreams.toString(reader);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void loadTpchStream(URI controller, StreamManager streamManager, TestingPrestoClient prestoClient, String schema, String stream, QualifiedObjectName tpchTableName)
    {
        try (PravegaLoader tpchLoader = new PravegaLoader(controller, streamManager, schema, stream, prestoClient.getServer(), prestoClient.getDefaultSession())) {
            tpchLoader.execute(format("SELECT * from %s", tpchTableName));
        }
    }

    public static void loadKeyValueTable(URI controller, StreamManager streamManager, String schema, String table, String keyFamily)
    {
        PravegaStreamDescription tableDesc = getKvStreamDesc(table);

        Schema keySchema = avroSchema(tableDesc, 0);
        GenericRecordBuilder keyBuilder = new GenericRecordBuilder(keySchema);

        Schema valueSchema = avroSchema(tableDesc, 1);
        GenericRecordBuilder valueBuilder = new GenericRecordBuilder(valueSchema);

        try (PravegaKeyValueLoader keyValueLoader =
                     new PravegaKeyValueLoader(controller,
                             streamManager, schema, table,
                             avroSchema(tableDesc, 0),
                             avroSchema(tableDesc, 1))) {
            try (InputStream inputStream = PravegaTestUtils.class.getResourceAsStream(String.format("/etc/%s.records", table));
                    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                // each line in file is a record, key + value
                // '|' character separates key fields from values fields
                // fields separated by ','
                String datum = reader.readLine();
                while (datum != null && !datum.isEmpty()) {
                    String keyString = datum.split("\\|")[0];
                    String valueString = datum.split("\\|")[1];

                    String[] keyFieldValues = keyString.split(",");
                    String[] valueFieldValues = valueString.split(",");

                    for (int i = 0; i < keyFieldValues.length; i++) {
                        setAvroValue(keyBuilder, keySchema.getFields().get(i), keyFieldValues[i]);
                    }

                    for (int i = 0; i < valueFieldValues.length; i++) {
                        setAvroValue(valueBuilder, valueSchema.getFields().get(i), valueFieldValues[i]);
                    }

                    keyValueLoader.put(keyFamily, keyBuilder.build(), valueBuilder.build());

                    keySchema.getFields().forEach(keyBuilder::clear);
                    valueSchema.getFields().forEach(valueBuilder::clear);

                    datum = reader.readLine();
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    static void setAvroValue(GenericRecordBuilder builder, Schema.Field field, String value)
    {
        Object object;
        switch (field.schema().getType()) {
            case INT:
                object = Integer.parseInt(value);
                break;
            case LONG:
                object = Long.parseLong(value);
                break;
            default:
                object = value;
                break;
        }
        builder.set(field, object);
    }

    public static PravegaStreamDescription getKvStreamDesc(String table)
    {
        JsonCodec<PravegaStreamDescription> jsonCodec = JsonCodec.jsonCodec(PravegaStreamDescription.class);

        PravegaStreamDescription streamDescription = getStreamDesc(jsonCodec, "kv", table);
        streamDescription = getStreamDesc(jsonCodec, "tpch", "customer");
        streamDescription.getEvent().orElseThrow(IllegalArgumentException::new);

        PravegaStreamFieldGroup keyEvent = streamDescription.getEvent().get().get(0);
        keyEvent.getDataSchema().orElseThrow(IllegalArgumentException::new);
        String keySchema = PravegaTestUtils.readSchema(keyEvent.getDataSchema().get());

        PravegaStreamFieldGroup valueEvent = streamDescription.getEvent().get().get(1);
        valueEvent.getDataSchema().orElseThrow(IllegalArgumentException::new);
        String valueSchema = PravegaTestUtils.readSchema(valueEvent.getDataSchema().get());

        List<PravegaStreamFieldDescription> keyFields =
                mapFieldsFromSchema("key", keyEvent.getDataFormat(), keySchema);

        List<PravegaStreamFieldDescription> valueFields =
                mapFieldsFromSchema("value", valueEvent.getDataFormat(), valueSchema);

        List<PravegaStreamFieldGroup> newFieldGroups = new ArrayList<>(2);
        newFieldGroups.add(new PravegaStreamFieldGroup(keyEvent, keySchema, keyFields));
        newFieldGroups.add(new PravegaStreamFieldGroup(valueEvent, valueSchema, valueFields));

        return new PravegaStreamDescription(streamDescription, newFieldGroups);
    }

    public static Schema avroSchema(PravegaStreamDescription streamDescription, int event)
    {
        return new Schema.Parser().parse(streamDescription.getEvent().get().get(event).getDataSchema().get());
    }
}
