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
package com.facebook.presto.pravega;

import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.ByteBufferSerializer;

import java.nio.ByteBuffer;
import java.util.Iterator;

import static com.facebook.presto.pravega.util.PravegaNameUtils.streamCutName;

public class StreamCutSupplier
        implements AutoCloseable
{
    private PravegaSegmentManager segmentManager;

    private Iterator<SegmentRange> rangeIterator;

    private SegmentIterator<ByteBuffer> segmentIterator;

    private StreamCut previous;

    private boolean empty;

    public StreamCutSupplier(PravegaSegmentManager segmentManager, String scope, String stream)
    {
        if (segmentManager.streamExists(scope, streamCutName(stream))) {
            // for now, read stream cuts from internal stream
            // https://github.com/pravega/pravega-sql/issues/24
            this.segmentManager = segmentManager;

            this.rangeIterator = segmentManager.getSegments(scope, streamCutName(stream), null, null).getIterator();
            // init fist stream cut
            this.previous = nextStreamCut();
        }

        if (this.previous == null) {
            // either stream doesn't exist or no stream cuts logged
            this.empty = true;
        }
    }

    private StreamCut nextStreamCut()
    {
        do {
            if (segmentIterator != null && segmentIterator.hasNext()) {
                return StreamCut.fromBytes(segmentIterator.next());
            }

            if (!rangeIterator.hasNext()) {
                return null;
            }

            segmentIterator = segmentManager.getSegmentIterator(rangeIterator.next(),
                    new ByteBufferSerializer());
        } while (true);
    }

    private StreamCutRange next()
    {
        if (previous == null) {
            return null;
        }

        StreamCut start = previous;
        StreamCut end = nextStreamCut();
        previous = end;

        // looking for explicitly defined start+end stream cuts
        // so we return null when we have no end (vs. start->UNBOUNDED)
        return previous == null ? null : new StreamCutRange(start, end);
    }

    public StreamCutRange get()
    {
        if (empty) {
            StreamCutRange range = StreamCutRange.NULL_PAIR;
            empty = false;
            return range;
        }
        return next();
    }

    @Override
    public void close()
    {
    }
}
