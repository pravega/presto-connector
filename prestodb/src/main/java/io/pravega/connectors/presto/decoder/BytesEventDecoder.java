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

package io.pravega.connectors.presto.decoder;

import io.pravega.connectors.presto.PravegaRecordValue;

// raw byte source.  deserialize and then pass off to decoder
public class BytesEventDecoder
        implements EventDecoder
{
    private final KVSerializer<?> kvSerializer;

    private final EventDecoder delegate;

    public BytesEventDecoder(KVSerializer<?> kvSerializer, EventDecoder delegate)
    {
        this.kvSerializer = kvSerializer;
        this.delegate = delegate;
    }

    @Override
    public boolean decodeEvent(DecodableEvent event, PravegaRecordValue record)
    {
        return delegate.decodeEvent(kvSerializer.toEvent(kvSerializer.deserialize(event.asBytes())), record);
    }
}
