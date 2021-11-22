/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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
 *
 * Note: This class file is from Trinodb
 * (rev 03d8e5abc686c5c4a9f96ca14db07e2aed880174)
 * https://github.com/trinodb/trino/blob/359/plugin/trino-kafka/src/test/java/io/trino/plugin/kafka/util/CodecSupplier.java
 */

package io.trino.plugin.pravega.integration;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.metadata.Metadata;
import io.trino.spi.type.Type;

import java.util.HashSet;
import java.util.function.Supplier;

import static io.trino.sql.analyzer.TypeSignatureTranslator.parseTypeSignature;

public final class CodecSupplier<T>
        implements Supplier<JsonCodec<T>>
{
    private final Metadata metadata;
    private final JsonCodecFactory codecFactory;
    private final Class<T> clazz;

    public CodecSupplier(Class<T> clazz, Metadata metadata)
    {
        this.clazz = clazz;
        this.metadata = metadata;
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer()));
        this.codecFactory = new JsonCodecFactory(objectMapperProvider);
    }

    @Override
    public JsonCodec<T> get()
    {
        return codecFactory.jsonCodec(clazz);
    }

    private class TypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private static final long serialVersionUID = 1L;

        public TypeDeserializer()
        {
            super(Type.class);
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = metadata.getType(parseTypeSignature(value, new HashSet<>()));
            if (type == null) {
                throw new IllegalArgumentException(String.valueOf("Unknown type " + value));
            }
            return type;
        }
    }
}
