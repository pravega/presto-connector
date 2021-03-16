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

import com.facebook.airlift.concurrent.MoreFutures;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.batch.SegmentRange;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.pravega.util.PravegaSerializationUtils.serialize;

public class PravegaSplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(PravegaSplitSource.class);

    private ListeningScheduledExecutorService tp =
            MoreExecutors.listeningDecorator(new ScheduledThreadPoolExecutor(2));

    private volatile boolean allDone;

    private BlockingQueue<SegmentRange> queue;

    private final String connectorId;

    private final PravegaTableHandle tableHandle;

    private final List<String> streams;

    private final PravegaSegmentManager segmentManager;

    private final PravegaProperties properties;

    private final ListenableFuture<Boolean> loader;

    private int batchCalls;
    private int total;
    private int maxReturned;
    private int minReturned = Integer.MAX_VALUE;

    private class AsyncLoader
            implements Callable<Boolean>
    {
        @Override
        public Boolean call()
        {
            log.info("start asyncLoader, " + streams.size() + " stream(s)");

            try (StreamManager streamManager = segmentManager.getStreamManager()) {
                SegmentRangeTable segmentRangeTable =
                        new SegmentRangeTable(streamManager,
                                segmentManager.batchClientFactory(tableHandle.getSchemaName()),
                                tableHandle.getSchemaName());
                for (String stream : streams) {
                    log.info("start loading ranges for stream " + stream);
                    segmentRangeTable.load(stream, queue, properties);
                }
            }
            log.info("done loading ranges for all streams");
            allDone = true;
            return true;
        }
    }

    public PravegaSplitSource(String connectorId,
                              PravegaTableHandle tableHandle,
                              List<String> streams,
                              PravegaSegmentManager segmentManager,
                              PravegaProperties properties)
    {
        this.connectorId = connectorId;
        this.tableHandle = tableHandle;
        this.streams = streams;
        this.segmentManager = segmentManager;
        this.properties = properties;

        this.queue = new LinkedBlockingQueue<>();
        this.loader = tp.submit(new AsyncLoader());
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        batchCalls++;

        ListenableFuture<ConnectorSplitBatch> future = tp.submit(() -> {
            log.debug("load next " + maxSize + " splits");

            ArrayList<SegmentRange> ranges = new ArrayList<>();
            while (!allDone) {
                SegmentRange first = queue.poll(500, TimeUnit.MILLISECONDS);
                if (first != null) {
                    ranges.add(first);
                    break;
                }
            }
            queue.drainTo(ranges, maxSize - 1);

            log.debug("got " + ranges.size() + " segmentRanges from loader");

            ArrayList<ConnectorSplit> results = new ArrayList<>();
            for (SegmentRange segmentRange : ranges) {
                PravegaSplit split = new PravegaSplit(
                        connectorId,
                        tableHandle.getObjectType(),
                        Collections.singletonList(tableHandle.getSchema().get(0)),
                        ReaderType.SEGMENT_RANGE_PER_SPLIT,
                        serialize(segmentRange),
                        tableHandle.getSchemaRegistryGroupId());
                results.add(split);
            }

            if (ranges.size() > maxReturned) {
                maxReturned = ranges.size();
            }
            else if (ranges.size() < minReturned) {
                minReturned = ranges.size();
            }
            total += ranges.size();
            return new ConnectorSplitBatch(results, isFinished());
        });
        return MoreFutures.toCompletableFuture(future);
    }

    @Override
    public void close()
    {
        log.info(Arrays.toString(streams.toArray()) +
                " [calls: " + batchCalls + " min: " + minReturned + " max: " + maxReturned + " total: " + total + "]");
        loader.cancel(true);
        tp.shutdownNow();
    }

    @Override
    public boolean isFinished()
    {
        return allDone && queue.isEmpty();
    }
}
