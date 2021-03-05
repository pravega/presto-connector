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
 */
package com.facebook.presto.pravega.decoder;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.DynamicMessage;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;

public abstract class DecodableEvent
{
    private DecodableEvent next;

    public void setNext(DecodableEvent next)
    {
        this.next = next;
    }

    public DecodableEvent next()
    {
        return next;
    }

    public DynamicMessage asProtobuf()
    {
        throw new UnsupportedOperationException("not protobuf format");
    }

    public GenericRecord asAvro()
    {
        throw new UnsupportedOperationException("not avro format");
    }

    public JsonNode asJson()
    {
        throw new UnsupportedOperationException("not json format");
    }

    public ByteBuffer asBytes()
    {
        throw new UnsupportedOperationException("not bytes format");
    }

    protected abstract int size();

    public int totalSize()
    {
        return size() + (next == null ? 0 : next.totalSize());
    }
}
