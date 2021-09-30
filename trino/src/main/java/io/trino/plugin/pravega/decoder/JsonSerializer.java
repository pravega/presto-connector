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

package io.trino.plugin.pravega.decoder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.trino.plugin.pravega.util.ByteBufferInputStream;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

// deserialize using externally provided schema or using SR+SerializerConfig
public class JsonSerializer
        extends KVSerializer<JsonNode>
{
    private static class JsonTreeSerializer
            implements Serializer<Object>
    {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public ByteBuffer serialize(Object value)
        {
            try {
                return ByteBuffer.wrap(objectMapper.writeValueAsBytes(value));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public JsonNode deserialize(ByteBuffer serializedValue)
        {
            try {
                return objectMapper.readTree(new ByteBufferInputStream(serializedValue));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public JsonSerializer(SerializerConfig config)
    {
        super(config, null);
    }

    @Override
    public Serializer<Object> serializerForSchema(String schema /* null for json */)
    {
        return new JsonTreeSerializer();
    }

    @Override
    public ByteBuffer serialize(JsonNode value)
    {
        return delegate.serialize(value);
    }

    @Override
    public JsonNode deserialize(ByteBuffer serializedValue)
    {
        return super.deserialize(serializedValue);
    }

    @Override
    public DecodableEvent toEvent(Object obj)
    {
        return new JsonEvent(obj);
    }
}
