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
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.stream.impl.ByteBufferSerializer;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class SegmentRangeIterator
        implements Iterator<DecodableEvent>
{
    private static final Logger log = Logger.get(SegmentRangeIterator.class);

    private final PravegaSegmentManager segmentManager;

    private final Iterator<SegmentRange> segmentIterator;

    private SegmentIterator<ByteBuffer> segmentEventIterator;

    private ByteBuffer event;

    private final StreamCutRange streamCutRange;

    private int fullSegments;

    private int emptySegments;

    private int events;

    public SegmentRangeIterator(PravegaSegmentManager segmentManager, ReaderArgs readerArgs)
    {
        this.segmentManager = segmentManager;

        this.streamCutRange = readerArgs.getStreamCutRange();

        log.info("open iterator for " + streamCutRange);
        this.segmentIterator =
                segmentManager.getSegments(readerArgs.getScope(),
                        readerArgs.getStream(),
                        readerArgs.getStreamCutRange().getStart(),
                        readerArgs.getStreamCutRange().getEnd()).getIterator();
    }

    private ByteBuffer _next()
    {
        if (segmentEventIterator != null && segmentEventIterator.hasNext()) {
            events++;
            return segmentEventIterator.next();
        }

        do {
            if (!segmentIterator.hasNext()) {
                log.info("done with " + streamCutRange + "; full: " + fullSegments + ", empty: " + emptySegments + ", events: " + events);
                return null;
            }

            segmentEventIterator = segmentManager.getSegmentIterator(segmentIterator.next(), new ByteBufferSerializer());
            log.info("next segment " + streamCutRange + " has event? " + segmentEventIterator.hasNext());
            if (segmentEventIterator.hasNext()) {
                fullSegments++;
                events++;
                return segmentEventIterator.next();
            }
            emptySegments++;
            // maybe segment was empty, continue
        } while (true);
    }

    @Override
    public boolean hasNext()
    {
        if (event == null) {
            event = _next();
        }
        return event != null;
    }

    @Override
    public DecodableEvent next()
    {
        ByteBuffer toReturn = event;
        event = null;
        return new BytesEvent(toReturn);
    }
}
