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

package io.trino.plugin.pravega;

import io.airlift.log.Logger;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.trino.plugin.pravega.decoder.BytesEvent;
import io.trino.plugin.pravega.decoder.DecodableEvent;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class EventStreamIterator
        implements Iterator<DecodableEvent>
{
    private static final Logger log = Logger.get(EventStreamIterator.class);

    private final PravegaSegmentManager segmentManager;
    private final ReaderArgs readerArgs;
    private EventStreamReader<ByteBuffer> reader;
    private final long readTimeoutMs;

    private ByteBuffer event;

    public EventStreamIterator(PravegaSegmentManager segmentManager, ReaderArgs readerArgs, PravegaProperties properties)
    {
        this.segmentManager = segmentManager;
        this.readerArgs = readerArgs;
        this.readTimeoutMs = properties.getEventReadTimeoutMs();
    }

    private boolean _next()
    {
        EventRead<ByteBuffer> read;
        do {
            read = reader.readNextEvent(readTimeoutMs);
        }
        while (read.isCheckpoint());
        event = read.getEvent();
        return event != null;
    }

    @Override
    public boolean hasNext()
    {
        return event != null || _next();
    }

    @Override
    public DecodableEvent next()
    {
        BytesEvent bytesEvent = new BytesEvent(event);
        event = null;
        return bytesEvent;
    }
}
