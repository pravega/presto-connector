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

package com.facebook.presto.pravega;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.pravega.decoder.BytesEvent;
import com.facebook.presto.pravega.decoder.DecodableEvent;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.stream.impl.ByteBufferSerializer;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class SegmentEventIterator
        implements Iterator<DecodableEvent>
{
    private static final Logger log = Logger.get(SegmentEventIterator.class);

    private final SegmentIterator<ByteBuffer> segmentEventIterator;

    public SegmentEventIterator(PravegaSegmentManager segmentManager, SegmentRange segmentRange)
    {
        log.info("open iterator for " + segmentRange);
        this.segmentEventIterator = segmentManager.getSegmentIterator(segmentRange, new ByteBufferSerializer());
    }

    @Override
    public boolean hasNext()
    {
        return segmentEventIterator.hasNext();
    }

    @Override
    public DecodableEvent next()
    {
        return new BytesEvent(segmentEventIterator.next());
    }
}
