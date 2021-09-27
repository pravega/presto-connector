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
package io.trino.plugin.pravega.integration;

import io.pravega.client.ClientConfig;
import io.pravega.client.KeyValueTableFactory;
import io.pravega.client.admin.KeyValueTableManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.Serializer;
import io.pravega.client.tables.KeyValueTable;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.trino.plugin.pravega.util.ByteBufferInputStream;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.ByteBuffer;

public class PravegaKeyValueLoader
        implements AutoCloseable
{
    static class AvroSerializer
            implements Serializer<GenericRecord>
    {
        private final DatumReader<GenericRecord> datumReader;

        public AvroSerializer(Schema schema)
        {
            this.datumReader = new GenericDatumReader(schema);
        }

        @Override
        public ByteBuffer serialize(GenericRecord record)
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

        @Override
        public GenericRecord deserialize(ByteBuffer serializedValue)
        {
            try (DataFileStream<GenericRecord> dataFileReader =
                         new DataFileStream<>(new ByteBufferInputStream(serializedValue), datumReader)) {
                return dataFileReader.next();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private final KeyValueTableManager tableManager;

    private final KeyValueTableFactory tableFactory;

    private final KeyValueTable<GenericRecord, GenericRecord> table;

    public PravegaKeyValueLoader(URI controller,
                                 StreamManager streamManager,
                                 String schema,
                                 String table,
                                 Schema keySchema,
                                 Schema valueSchema)
    {
        if (!streamManager.checkScopeExists(schema)) {
            streamManager.createScope(schema);
        }

        ClientConfig clientConfig = ClientConfig.builder().controllerURI(controller).build();

        this.tableManager = KeyValueTableManager.create(clientConfig);
        this.tableManager.createKeyValueTable(schema, table,
                KeyValueTableConfiguration.builder().partitionCount(1).build());

        this.tableFactory = KeyValueTableFactory.withScope(schema, clientConfig);

        this.table = tableFactory.forKeyValueTable(table,
                new AvroSerializer(keySchema),
                new AvroSerializer(valueSchema),
                KeyValueTableClientConfiguration.builder().build());
    }

    public void put(String keyFamily, GenericRecord key, GenericRecord value)
    {
        table.put(keyFamily, key, value).join();
    }

    @Override
    public void close()
    {
        closeQuietly(table);
        closeQuietly(tableFactory);
        closeQuietly(tableManager);
    }

    private void closeQuietly(AutoCloseable closeable)
    {
        try {
            closeable.close();
        }
        catch (Exception quiet) {
        }
    }
}
