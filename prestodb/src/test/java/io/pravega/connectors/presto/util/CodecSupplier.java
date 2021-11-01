/*
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
 * Note: This class file is from PrestoDb
 * (rev a8968160e1840ac67a5f63def27d31c0ef0acde7)
 * https://github.com/prestodb/presto/blob/0.247/presto-kafka/src/test/java/com/facebook/presto/kafka/util/CodecSupplier.java
 */
package io.pravega.connectors.presto.util;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.ObjectMapperProvider;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.common.collect.ImmutableMap;

import java.util.function.Supplier;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;

public final class CodecSupplier<T>
        implements Supplier<JsonCodec<T>>
{
    private final FunctionAndTypeManager typeManager;
    private final JsonCodecFactory codecFactory;
    private final Class<T> clazz;

    public CodecSupplier(Class<T> clazz, FunctionAndTypeManager typeManager)
    {
        this.clazz = clazz;
        this.typeManager = typeManager;
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
            Type type = typeManager.getType(parseTypeSignature(value));
            if (type == null) {
                throw new IllegalArgumentException(String.valueOf("Unknown type " + value));
            }
            return type;
        }
    }
}
