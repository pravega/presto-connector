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

package com.facebook.presto.pravega;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.pravega.decoder.AvroRowDecoder;
import com.facebook.presto.pravega.decoder.AvroSerializer;
import com.facebook.presto.pravega.decoder.BytesEventDecoder;
import com.facebook.presto.pravega.decoder.CsvRowDecoder;
import com.facebook.presto.pravega.decoder.CsvSerializer;
import com.facebook.presto.pravega.decoder.EventDecoder;
import com.facebook.presto.pravega.decoder.JsonRowDecoderFactory;
import com.facebook.presto.pravega.decoder.JsonSerializer;
import com.facebook.presto.pravega.decoder.KVSerializer;
import com.facebook.presto.pravega.decoder.MultiSourceRowDecoder;
import com.facebook.presto.pravega.decoder.ProtobufRowDecoder;
import com.facebook.presto.pravega.decoder.ProtobufSerializer;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.pravega.PravegaHandleResolver.convertSplit;
import static com.facebook.presto.pravega.util.PravegaSchemaUtils.AVRO;
import static com.facebook.presto.pravega.util.PravegaSchemaUtils.AVRO_INLINE;
import static com.facebook.presto.pravega.util.PravegaSchemaUtils.CSV;
import static com.facebook.presto.pravega.util.PravegaSchemaUtils.JSON;
import static com.facebook.presto.pravega.util.PravegaSchemaUtils.JSON_INLINE;
import static com.facebook.presto.pravega.util.PravegaSchemaUtils.PROTOBUF;
import static com.facebook.presto.pravega.util.PravegaSchemaUtils.PROTOBUF_INLINE;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
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
                .collect(ImmutableList.toImmutableList());

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
                return new AvroSerializer(schema.getSchemaLocation().get());
            case AVRO_INLINE:
                return new AvroSerializer(serializerConfig);

            case PROTOBUF:
                return new ProtobufSerializer(schema.getSchemaLocation().get());
            case PROTOBUF_INLINE:
                return new ProtobufSerializer(serializerConfig);

            case JSON:
                return new JsonSerializer();
            case JSON_INLINE:
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
            case AVRO_INLINE:
                return new AvroRowDecoder(decoderColumnHandles);

            case PROTOBUF:
            case PROTOBUF_INLINE:
                return new ProtobufRowDecoder(decoderColumnHandles);

            case JSON:
            case JSON_INLINE:
                return jsonRowDecoderFactory.create(decoderColumnHandles);

            case CSV: {
                return new CsvRowDecoder();
            }
            default:
                throw new IllegalArgumentException(schema.toString());
        }
    }
}
