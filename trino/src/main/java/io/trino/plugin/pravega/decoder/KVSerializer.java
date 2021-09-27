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

import io.airlift.log.Logger;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;

import java.nio.ByteBuffer;

// deserialize using externally provided schema or using SR+SerializerConfig
public abstract class KVSerializer<T>
        implements Serializer<T>
{
    private static final Logger log = Logger.get(KVSerializer.class);

    protected Serializer<Object> delegate = null;

    private boolean schemaRegistryDeserializer;

    private final SerializerConfig serializerConfig;

    private final String schema;

    protected KVSerializer(SerializerConfig serializerConfig, String schema) {
        this.serializerConfig = serializerConfig;
        this.schema = schema;
    }

    public boolean schemaRegistryDeserializer()
    {
        return schemaRegistryDeserializer;
    }

    // format of data is unknown, whether schema is encoded inline by pravega schema registry or not
    // try to deserialize without, and if it fails, use serializerConfig
    protected void chooseDeserializer(ByteBuffer serializedValue)
    {
        Serializer<Object> serializer = serializerForSchema(schema);
        serializedValue.mark();
        try {
            if (serializer.deserialize(serializedValue) != null) {
                delegate = serializer;
            }
        }
        catch (RuntimeException e) {
            log.info("could not deserialize, try SR deserializer");
            delegate = SerializerFactory.genericDeserializer(serializerConfig);
            schemaRegistryDeserializer = true;
        }
        finally {
            serializedValue.reset();
        }
    }

    public T deserialize(ByteBuffer serializedValue)
    {
        if (delegate == null) {
            chooseDeserializer(serializedValue);
        }
        return (T) delegate.deserialize(serializedValue);
    }

    public abstract Serializer<Object> serializerForSchema(String schema);

    // create an event that can be passed down to decoders
    public abstract DecodableEvent toEvent(Object obj);
}
