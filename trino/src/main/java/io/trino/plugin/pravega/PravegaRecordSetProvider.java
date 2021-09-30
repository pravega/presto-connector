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

package io.trino.plugin.pravega;

import io.airlift.log.Logger;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.plugin.pravega.decoder.AvroRowDecoder;
import io.trino.plugin.pravega.decoder.AvroSerializer;
import io.trino.plugin.pravega.decoder.BytesEventDecoder;
import io.trino.plugin.pravega.decoder.CsvRowDecoder;
import io.trino.plugin.pravega.decoder.CsvSerializer;
import io.trino.plugin.pravega.decoder.EventDecoder;
import io.trino.plugin.pravega.decoder.JsonRowDecoderFactory;
import io.trino.plugin.pravega.decoder.JsonSerializer;
import io.trino.plugin.pravega.decoder.KVSerializer;
import io.trino.plugin.pravega.decoder.MultiSourceRowDecoder;
import io.trino.plugin.pravega.decoder.ProtobufRowDecoder;
import io.trino.plugin.pravega.decoder.ProtobufSerializer;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.pravega.PravegaHandleResolver.convertSplit;
import static io.trino.plugin.pravega.util.PravegaSchemaUtils.AVRO;
import static io.trino.plugin.pravega.util.PravegaSchemaUtils.CSV;
import static io.trino.plugin.pravega.util.PravegaSchemaUtils.JSON;
import static io.trino.plugin.pravega.util.PravegaSchemaUtils.PROTOBUF;
import static java.util.Objects.requireNonNull;

/**
 * Factory for Pravega specific {@link RecordSet} instances.
 */
public class PravegaRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private static final Logger log = Logger.get(PravegaRecordSetProvider.class);
    private JsonRowDecoderFactory jsonRowDecoderFactory;
    private final PravegaSegmentManager streamReaderManager;

    @Inject
    public PravegaRecordSetProvider(JsonRowDecoderFactory jsonRowDecoderFactory,
            PravegaSegmentManager streamReaderManager)
    {
        this.jsonRowDecoderFactory = requireNonNull(jsonRowDecoderFactory, "jsonRowDecoderFactory is null");
        this.streamReaderManager = requireNonNull(streamReaderManager, "streamReaderManager is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            List<? extends ColumnHandle> columns)
    {
        final PravegaSplit pravegaSplit = convertSplit(split);

        List<PravegaColumnHandle> pravegaColumns = columns.stream()
                .map(PravegaHandleResolver::convertColumnHandle)
                .collect(toImmutableList());

        SerializerConfig serializerConfig =
                streamReaderManager.serializerConfig(pravegaSplit.getschemaRegistryGroupId());

        List<KVSerializer<?>> serializers = new ArrayList<>(2);
        List<EventDecoder> eventDecoders = new ArrayList<>(2);

        // for stream there is 1 schema
        // for kv table there are 2.  1 for key, 1 for value and (very likely) they are of different types
        for (int i = 0; i < pravegaSplit.getSchema().size(); i++) {
            int finalI = i;
            PravegaObjectSchema schema = pravegaSplit.getSchema().get(i);

            // decoderColumnHandles will contain columns included only in current schema
            Set<DecoderColumnHandle> decoderColumnHandles =
                    pravegaColumns.stream()
                            .filter(col -> !col.isInternal())
                            .filter(col -> !col.isKeyDecoder())
                            .filter(col -> col.getSchemaNum() == finalI)
                            .collect(toImmutableSet());

            // serializer: de/serialize to/from object with given schema
            // (KV table will have 2 serializers.  1 for key, 1 for value)
            KVSerializer<?> serializer = serializer(schema, serializerConfig);
            serializers.add(serializer);

            // EventDecoder
            // accepts an already deserialized object (DynamicMessage, GenericRecord, JsonNode) and decodes it as a row
            // impl. for each of avro, protobuf, json
            //
            // BytesEventDecoder
            // takes raw bytes from a source and deserializes
            //
            // when iterate from KV table it gives us a TableEntry with key+value already deserialize
            // for this we give right to EventDecoder
            //
            // for stream it will come from raw bytes
            // (2 flavors of this, SR serializerConfig or our own decoder w/ provided schema)
            // for this wrap EventDecoder in BytesEventDecoder
            EventDecoder eventDecoder = eventDecoder(schema, decoderColumnHandles);

            if (pravegaSplit.getObjectType() == ObjectType.KV_TABLE) {
                // KV table API will give us back deserialized object, use
                eventDecoders.add(eventDecoder);
            }
            else {
                // stream API gives us bytes back
                eventDecoders.add(new BytesEventDecoder(serializer, eventDecoder));
            }
        }

        pravegaColumns.forEach(s -> log.debug("pravega column: %s", s));

        switch (pravegaSplit.getObjectType()) {
            case STREAM:
                if (eventDecoders.size() != 1) {
                    throw new IllegalStateException("stream should have 1 event decoder (vs " + eventDecoders.size() + ")");
                }

                return new PravegaRecordSet(new PravegaProperties(session),
                        pravegaSplit,
                        streamReaderManager,
                        pravegaColumns,
                        eventDecoders.get(0));

            case KV_TABLE:
                return new PravegaKVRecordSet(new PravegaProperties(session),
                        pravegaSplit,
                        streamReaderManager,
                        pravegaColumns,
                        new MultiSourceRowDecoder(eventDecoders),
                        serializers);
            default:
                throw new IllegalArgumentException("unexpected split type: " + pravegaSplit.toString());
        }
    }

    private KVSerializer<?> serializer(PravegaObjectSchema schema, SerializerConfig serializerConfig)
    {
        switch (schema.getFormat()) {
            case AVRO:
                return new AvroSerializer(serializerConfig, schema.getSchemaLocation().get());

            case PROTOBUF:
                return new ProtobufSerializer(serializerConfig, schema.getSchemaLocation().get());

            case JSON:
                return new JsonSerializer(serializerConfig);

            case CSV:
                return new CsvSerializer();

            default:
                throw new IllegalArgumentException(schema.toString());
        }
    }

    private EventDecoder eventDecoder(PravegaObjectSchema schema, Set<DecoderColumnHandle> decoderColumnHandles)
    {
        switch (schema.getFormat()) {
            case AVRO:
                return new AvroRowDecoder(decoderColumnHandles);

            case PROTOBUF:
                return new ProtobufRowDecoder(decoderColumnHandles);

            case JSON:
                return jsonRowDecoderFactory.create(decoderColumnHandles);

            case CSV: {
                return new CsvRowDecoder();
            }
            default:
                throw new IllegalArgumentException(schema.toString());
        }
    }
}
