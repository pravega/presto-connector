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
package io.pravega.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.pravega.client.stream.Serializer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

public class JsonSerializer<T> implements Serializer<T> {
    ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public ByteBuffer serialize(T node) {
        try {
            return ByteBuffer.wrap(objectMapper.writeValueAsBytes(node));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public T deserialize(ByteBuffer byteBuffer) {
        throw new UnsupportedOperationException("deserialize not needed here for ingest");
    }
}