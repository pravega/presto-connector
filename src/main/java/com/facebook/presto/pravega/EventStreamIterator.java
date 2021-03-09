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
 * limitations under the License.Copyright (c) Pravega Authors.
 */

package com.facebook.presto.pravega;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.pravega.decoder.BytesEvent;
import com.facebook.presto.pravega.decoder.DecodableEvent;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.impl.ByteBufferSerializer;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.UUID;

import static com.facebook.presto.pravega.util.PravegaNameUtils.scopedName;

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

    private void init()
    {
        log.info("open iterator for stream " + readerArgs);
        String readerGroupName = readerArgs.getReaderGroup();
        if (readerArgs.getReaderGroup() == null) {
            readerGroupName = "reader-group-" + UUID.randomUUID().toString();
            ReaderGroupConfig config =
                    ReaderGroupConfig.builder()
                            .stream(scopedName(readerArgs.getScope(), readerArgs.getStream()),
                                    readerArgs.getStreamCutRange().getStart(),
                                    readerArgs.getStreamCutRange().getEnd())
                            .build();
            log.info("create reader group " + readerGroupName);
            segmentManager.readerGroupManager(
                    readerArgs.getScope()).createReaderGroup(readerGroupName, config);
        }

        String readerId = UUID.randomUUID().toString();
        log.info("create reader " + readerId);
        reader = segmentManager.getEventStreamClientFactory(readerArgs.getScope())
                .createReader(readerId,
                        readerGroupName,
                        new ByteBufferSerializer(),
                        ReaderConfig.builder().build());
    }

    private boolean _next()
    {
        if (reader == null) {
            init();
        }

        EventRead<ByteBuffer> read;
        do {
            read = reader.readNextEvent(readTimeoutMs);
        } while (read.isCheckpoint());
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
