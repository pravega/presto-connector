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

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

public class PravegaSerializationUtils
{
    private PravegaSerializationUtils()
    {
    }

    public static byte[] serialize(Serializable s)
    {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(s);
            return baos.toByteArray();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static <T> T deserialize(byte[] bytes, Class<T> clazz)
    {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            ObjectInputStream bis = new ObjectInputStream(bais);
            return (T) bis.readObject();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static ByteBuffer serialize(GenericRecord record)
    {
        try {
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(record.getSchema());
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(writer);

            ByteArrayOutputStream os = new ByteArrayOutputStream();
            dataFileWriter.create(record.getSchema(), os);
            dataFileWriter.append(record);
            dataFileWriter.close();
            return ByteBuffer.wrap(os.toByteArray());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
