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
import io.pravega.client.BatchClientFactory;
import io.pravega.client.admin.StreamInfo;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.batch.impl.SegmentRangeImpl;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.watermark.WatermarkSerializer;
import io.pravega.shared.NameUtils;
import io.pravega.shared.watermarks.Watermark;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;

public class SegmentRangeTable
{
    private static final Logger log = Logger.get(SegmentRangeTable.class);

    private final StreamManager streamManager;

    private final BatchClientFactory batchClient;

    private final String scope;

    public SegmentRangeTable(final StreamManager streamManager, final BatchClientFactory batchClient, final String scope)
    {
        this.streamManager = streamManager;
        this.batchClient = batchClient;
        this.scope = scope;
    }

    public void load(final String streamName, Queue<SegmentRange> queue, final PravegaProperties properties)
    {
        Stream stream = Stream.of(scope, streamName);
        log.info("load SegmentRanges for " + stream);

        if (!streamManager.checkStreamExists(stream.getScope(), NameUtils.getMarkStreamForStream(stream.getStreamName()))) {
            log.info("watermark stream for " + stream + " does not exist, return unbounded segments");
            loadUnboundedSegmentRanges(stream, queue);
            return; // early exit!!
        }

        Iterator<SegmentRange> iterator =
                batchClient.getSegments(Stream.of(stream.getScope(),
                        NameUtils.getMarkStreamForStream(stream.getStreamName())), null, null).getIterator();

        if (!iterator.hasNext()) {
            log.info("no segments in watermark stream for " + stream + ", return unbounded segments");
            loadUnboundedSegmentRanges(stream, queue);
            return; // early exit!!
        }

        SegmentIterator<Watermark> segmentIterator =
                batchClient.readSegment(iterator.next(), new WatermarkSerializer());

        if (iterator.hasNext()) {
            // does watermark stream have > 1 segment?
            throw new IllegalStateException("> 1 segment not handled");
        }

        if (!segmentIterator.hasNext()) {
            log.info("empty watermark stream for " + stream + ", return unbounded segments");
            loadUnboundedSegmentRanges(stream, queue);
            return; // early exit!!
        }

        // our target split size
        int desiredSegmentRangeSizeBytes = properties.getSegmentRangeSplitSizeBytes();
        // so we don't have to create Segment over+again
        final Map<Long, Segment> lookup = new HashMap<>();
        // 'start' offset in a split/segment
        final Map<Long, StreamCut> baseline = new HashMap<>();

        StreamInfo streamInfo = streamManager.getStreamInfo(stream.getScope(), stream.getStreamName());

        // for each head cut, put to baseline map
        streamInfo.getHeadStreamCut().asImpl().getPositions().forEach((segment, offset) -> {
            lookup.put(segment.getSegmentId(), segment);
            baseline.put(segment.getSegmentId(), new StreamCutImpl(stream, segment, offset));
        });

        // for each watermark
        while (segmentIterator.hasNext()) {
            // for each segment,offset within the watermark
            // compare offset against our baseline for the segment
            // if exceeds split size, that is our SegmentRange. update baseline.
            segmentIterator.next().getStreamCut().forEach((swr, offset) -> {
                Segment segment = lookup.get(swr.getSegmentId());
                if (segment == null) {
                    // scale up
                    // insert first position by looking up starting offset for the segment
                    segment = lookup.computeIfAbsent(swr.getSegmentId(),
                            k -> new Segment(stream.getScope(), stream.getStreamName(), swr.getSegmentId()));
                    baseline.put(segment.getSegmentId(), batchClient.firstStreamCut(segment));
                }
                else if (distance(segment, baseline.get(segment.getSegmentId()), offset) >= desiredSegmentRangeSizeBytes) {
                    // size exceeded, this is a split
                    StreamCut sc = new StreamCutImpl(stream, segment, offset);
                    queue.add(constructRange(segment, baseline.get(segment.getSegmentId()), sc));
                    baseline.put(segment.getSegmentId(), sc);
                }
            });
        }

        // end of watermarks - finish up by creating last splits regardless of size

        // refresh tail stream cuts.  it could be the case that stream was being written to
        // in which case watermarks would have taken us past our previous tail stream cut view taken at start
        streamInfo = streamManager.getStreamInfo(stream.getScope(), stream.getStreamName());
        streamInfo.getTailStreamCut().asImpl().getPositions().forEach((segment, offset) ->
                queue.add(constructRange(segment, baseline.remove(segment.getSegmentId()), new StreamCutImpl(stream, segment, offset))));

        // we removed above while iterating tail stream cuts - so these don't exist in tail
        baseline.forEach((sid, sc) -> {
            // scale down
            queue.add(constructRange(lookup.get(sid), sc, null));
        });
    }

    private SegmentRange constructRange(Segment segment, StreamCut start, StreamCut end)
    {
        if (start == null) {
            start = batchClient.firstStreamCut(segment);
        }

        if (end == null) {
            end = batchClient.lastStreamCut(segment);
        }

        return SegmentRangeImpl.fromStreamCuts(segment, start, end);
    }

    long distance(Segment segment, StreamCut start, long endOffset)
    {
        Long startOffset = start.asImpl().getPositions().getOrDefault(segment, -1L);
        if (startOffset < 0 || endOffset <= startOffset) {
            throw new IllegalStateException(start + " " + endOffset + " " + segment);
        }
        return endOffset - startOffset;
    }

    private void loadUnboundedSegmentRanges(Stream stream, Queue<SegmentRange> queue)
    {
        Iterator<SegmentRange> iterator =
                batchClient.getSegments(Stream.of(stream.getScope(), stream.getStreamName()), null, null).getIterator();
        while (iterator.hasNext()) {
            queue.add(iterator.next());
        }
    }
}