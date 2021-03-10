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

package com.facebook.presto.pravega.decoder;

import com.facebook.presto.pravega.util.ByteBufferInputStream;
import com.google.protobuf.DynamicMessage;
import io.pravega.client.stream.Serializer;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;
import io.pravega.schemaregistry.serializers.SerializerFactory;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

// deserialize using externally provided schema or using SR+SerializerConfig
public class AvroSerializer
        extends KVSerializer<GenericRecord>
{
    private static class GenericRecordSerializer
            implements Serializer<Object>
    {
        private final DatumReader<GenericRecord> datumReader;

        private final Schema schema;

        GenericRecordSerializer(Schema schema)
        {
            this.datumReader = new GenericDatumReader(schema);
            this.schema = schema;
        }

        @Override
        public ByteBuffer serialize(Object value)
        {
            return ByteBuffer.wrap(((DynamicMessage) value).toByteArray());
        }

        @Override
        public GenericRecord deserialize(ByteBuffer serializedValue)
        {
            try (DataFileStream<GenericRecord> dataFileReader =
                    new DataFileStream<>(new ByteBufferInputStream(serializedValue), datumReader)) {
                // TODO: need to figure out how to auto-detect format of avro data
                // for e.g, is schema provided for every row? (this is how the normal presto avro decoder takes it)
                // i would think more typically case would be that schema defined once and thus schema not provided
                // in every rows data
                //
                // for now we will do it the "presto way"
                return dataFileReader.next();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private final Serializer<Object> delegate;

    public AvroSerializer(SerializerConfig config)
    {
        this.delegate = SerializerFactory.genericDeserializer(config);
    }

    public AvroSerializer(String encodedSchema)
    {
        Schema schema = (new Schema.Parser()).parse(encodedSchema);
        this.delegate = new GenericRecordSerializer(schema);
    }

    @Override
    public ByteBuffer serialize(GenericRecord value)
    {
        return delegate.serialize(value);
    }

    @Override
    public GenericRecord deserialize(ByteBuffer serializedValue)
    {
        return (GenericRecord) delegate.deserialize(serializedValue);
    }

    @Override
    public DecodableEvent toEvent(Object obj)
    {
        return new AvroEvent(obj);
    }
}
