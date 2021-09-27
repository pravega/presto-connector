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

import io.pravega.connectors.presto.util.ByteBufferInputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

import static io.pravega.connectors.presto.ProtobufCommon.decodeSchema;
import static io.pravega.connectors.presto.ProtobufCommon.descriptorFor;

// deserialize using externally provided schema or using SR+SerializerConfig
public class ProtobufSerializer
        extends KVSerializer<DynamicMessage>
{
    private static class DynamicMessageSerializer
            implements Serializer<Object>
    {
        private final Descriptors.Descriptor descriptor;

        DynamicMessageSerializer(Descriptors.Descriptor descriptor)
        {
            this.descriptor = descriptor;
        }

        @Override
        public ByteBuffer serialize(Object value)
        {
            return ByteBuffer.wrap(((DynamicMessage) value).toByteArray());
        }

        @Override
        public DynamicMessage deserialize(ByteBuffer serializedValue)
        {
            try {
                return DynamicMessage.parseFrom(descriptor,
                        new ByteBufferInputStream(serializedValue));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private final Serializer<Object> delegate;

    public ProtobufSerializer(SerializerConfig config)
    {
        this.delegate = SerializerFactory.genericDeserializer(config);
    }

    public ProtobufSerializer(String encodedSchema)
    {
        Pair<String, ByteBuffer> pair = decodeSchema(encodedSchema);
        this.delegate = new DynamicMessageSerializer(descriptorFor(pair.getLeft(), pair.getRight()));
    }

    @Override
    public ByteBuffer serialize(DynamicMessage value)
    {
        return delegate.serialize(value);
    }

    @Override
    public DynamicMessage deserialize(ByteBuffer serializedValue)
    {
        return (DynamicMessage) delegate.deserialize(serializedValue);
    }

    @Override
    public DecodableEvent toEvent(Object obj)
    {
        return new ProtobufEvent(obj);
    }
}
