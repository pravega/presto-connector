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

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;

import java.util.Map;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.decoder.DecoderErrorCode.DECODER_CONVERSION_NOT_SUPPORTED;
import static io.trino.plugin.pravega.util.PravegaSchemaUtils.NESTED_RECORD_SEPARATOR;
import static io.trino.spi.type.StandardTypes.VARBINARY;
import static io.trino.spi.type.StandardTypes.VARCHAR;
import static io.trino.spi.type.Varchars.truncateToLength;
import static java.lang.String.format;

public class ProtobufColumnDecoder
{
    private final Type type;
    private final String name;
    private final String[] mapping;

    public ProtobufColumnDecoder(DecoderColumnHandle columnHandle)
    {
        this.type = columnHandle.getType();
        this.name = columnHandle.getName();
        this.mapping = columnHandle.getMapping().split(NESTED_RECORD_SEPARATOR);
    }

    public FieldValueProvider decodeField(DynamicMessage dynamicMessage)
    {
        return decodeField(0, dynamicMessage);
    }

    /*
        protobuf schema:
            Address {
                string street;
                string city;
                string state;
            }

            Person {
                string first;
                string last;
                Address address;
            }

        our list of columns will be:
            first, last, address/street, address/city, address/state

        Address here is nested.  if this column is "address/city":
        this.name = "address/city"
        this.mapping[0] = "address"
        this.mapping[1] = "city"

        starting out with mapping level 0 we look for mapping[0] / "address"
        iterating fields of Person, we will get field name "address" of type MESSAGE
        here we recurse into decodeField bumping the mapping level to look for mapping[1] / "city"
     */

    private FieldValueProvider decodeField(int level, DynamicMessage dynamicMessage)
    {
        for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : dynamicMessage.getAllFields().entrySet()) {
            if (entry.getKey().getJsonName().equals(this.mapping[level])) {
                if (entry.getKey().getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
                    if (level == this.mapping.length - 1) {
                        throw new IllegalArgumentException("unexpected end to mapping " + name);
                    }
                    return decodeField(level + 1,
                            (DynamicMessage) entry.getValue());
                }
                else {
                    return new ProtobufFieldValueProvider(this.type, this.name, entry.getValue());
                }
            }
        }
        // record does not have this field.  will return null.
        return new ProtobufFieldValueProvider(this.type, this.name, null);
    }

    private static class ProtobufFieldValueProvider
            extends FieldValueProvider
    {
        private final Type type;
        private final String name;
        private final Object object;

        ProtobufFieldValueProvider(Type type, String name, Object object)
        {
            this.type = type;
            this.name = name;
            this.object = object;
        }

        @Override
        public boolean getBoolean()
        {
            return (boolean) object;
        }

        @Override
        public long getLong()
        {
            if (object instanceof Integer) {
                return (long) (int) object;
            }
            else {
                return (long) object;
            }
        }

        @Override
        public double getDouble()
        {
            if (object instanceof Float) {
                return (double) (float) object;
            }
            else {
                return (double) object;
            }
        }

        @Override
        public Slice getSlice()
        {
            return getSlice(object, type, name);
        }

        @Override
        public boolean isNull()
        {
            return object == null;
        }

        private static Slice getSlice(Object value, Type type, String columnName)
        {
            switch (type.getTypeSignature().getBase()) {
                case VARCHAR:
                    if (value instanceof String) {
                        return truncateToLength(utf8Slice(value.toString()), type);
                    }
                case VARBINARY:
                    if (value instanceof ByteString) {
                        return Slices.wrappedBuffer(((ByteString) value).asReadOnlyByteBuffer());
                    }
                default:
                    throw new TrinoException(DECODER_CONVERSION_NOT_SUPPORTED,
                            format("cannot decode object of '%s' as '%s' for column '%s'", value.getClass(), type, columnName));
            }
        }
    }
}
