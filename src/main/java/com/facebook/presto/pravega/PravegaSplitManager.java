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
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.stream.ReaderGroupConfig;

import javax.inject.Inject;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.facebook.presto.pravega.PravegaErrorCode.PRAVEGA_SPLIT_ERROR;
import static com.facebook.presto.pravega.PravegaHandleResolver.convertLayout;
import static com.facebook.presto.pravega.util.PravegaNameUtils.multiSourceStream;
import static com.facebook.presto.pravega.util.PravegaNameUtils.scopedName;
import static com.facebook.presto.pravega.util.PravegaSerializationUtils.serialize;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Pravega specific implementation of {@link ConnectorSplitManager}.
 */
public class PravegaSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(PravegaSegmentManager.class);
    private final String connectorId;
    private final PravegaConnectorConfig pravegaConnectorConfig;
    private final PravegaSegmentManager streamReaderManager;

    @Inject
    public PravegaSplitManager(
            PravegaConnectorId connectorId,
            PravegaConnectorConfig pravegaConnectorConfig,
            PravegaSegmentManager streamReaderManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.streamReaderManager = requireNonNull(streamReaderManager, "streamReaderManager is null");
        this.pravegaConnectorConfig = requireNonNull(pravegaConnectorConfig, "pravegaConnectorConfig is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        PravegaTableHandle pravegaTableHandle = convertLayout(layout).getTable();
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

        try {
            if (pravegaTableHandle.getObjectType() == ObjectType.KV_TABLE) {
                buildKVSplits(pravegaTableHandle, splits);
            }
            else {
                buildStreamSplits(new PravegaProperties(session), pravegaTableHandle, splits);
            }

            return new FixedSplitSource(splits.build());
        }
        catch (Exception e) { // Catch all exceptions because Pravega library is written in scala and checked exceptions are not declared in method signature.
            if (e instanceof PrestoException) {
                throw e;
            }
            throw new PrestoException(PRAVEGA_SPLIT_ERROR,
                    format("Cannot list splits for table '%s' reading stream '%s'",
                            pravegaTableHandle.getTableName(), pravegaTableHandle.getObjectName()), e);
        }
    }

    private static ReaderType readerType(PravegaProperties properties)
    {
        String type = properties.getReaderType();
        switch (type) {
            case "event":
                return ReaderType.EVENT_STREAM;
            case "grouped_event":
                return ReaderType.SINGLE_GROUP_EVENT_STREAM;
            case "segment_range_per_split":
                return ReaderType.SEGMENT_RANGE_PER_SPLIT;
            default:
                return ReaderType.SEGMENT_RANGE;
        }
    }

    private void buildKVSplits(PravegaTableHandle pravegaTableHandle, ImmutableList.Builder<ConnectorSplit> splits)
    {
        pravegaTableHandle.getOjectArgs().orElseThrow(() ->
                new IllegalArgumentException("no KF defined for " + pravegaTableHandle));

        for (String kf : pravegaTableHandle.getOjectArgs().get()) {
            PravegaSplit split =
                    new PravegaSplit(connectorId,
                            ObjectType.KV_TABLE,
                            pravegaTableHandle.getSchema(),
                            ReaderType.KVT,
                            serialize(new PravegaKVTable(pravegaTableHandle.getSchemaName(), pravegaTableHandle.getObjectName(), kf)),
                            pravegaTableHandle.getSchemaRegistryGroupId());
            splits.add(split);
        }

        log.info("created " + pravegaTableHandle.getOjectArgs().get().size() + " kv splits");
    }

    private void buildStreamSplits(final PravegaProperties properties,
                                   PravegaTableHandle pravegaTableHandle,
                                   ImmutableList.Builder<ConnectorSplit> splits)
    {
        // TODO: Enable begin and end cuts to be configurable: https://github.com/pravega/pravega-sql/issues/24
        List<String> sourceStreams = multiSourceStream(pravegaTableHandle)
                ? pravegaTableHandle.getOjectArgs().orElseThrow(
                        () -> new IllegalArgumentException("no args for multi source table found"))
                : Collections.singletonList(pravegaTableHandle.getObjectName());

        AtomicInteger splitCounter = new AtomicInteger(0);
        ReaderType readerType = readerType(properties);

        sourceStreams.forEach(stream -> {
            StreamCutSupplier streamCutSupplier = new StreamCutSupplier(streamReaderManager, pravegaTableHandle.getSchemaName(), stream);

            Supplier<PravegaSplit> splitSupplier;
            log.info("get split supplier for " + readerType);
            switch (readerType) {
                case EVENT_STREAM:
                case SEGMENT_RANGE:
                    splitSupplier = splitSupplier(readerType, pravegaTableHandle, stream, streamCutSupplier);
                    break;

                case SINGLE_GROUP_EVENT_STREAM:
                    splitSupplier = groupedReaderSplitSupplier(readerType, pravegaTableHandle, stream, streamCutSupplier, properties.getGroupedEventSplits());
                    break;

                case SEGMENT_RANGE_PER_SPLIT:
                    splitSupplier = segmentPerSplitSupplier(readerType, pravegaTableHandle, stream, streamCutSupplier);
                    break;

                default:
                    throw new IllegalArgumentException("" + readerType);
            }

            PravegaSplit split = splitSupplier.get();
            do {
                splits.add(split);
                splitCounter.incrementAndGet();
                split = splitSupplier.get();
            } while (split != null);
        });

        log.info("created " + splitCounter.get() + " stream splits of type " + readerType);
    }

    Supplier<PravegaSplit> splitSupplier(final ReaderType readerType,
                                         final PravegaTableHandle tableHandle,
                                         final String stream,
                                         final StreamCutSupplier streamCutSupplier)
    {
        return () -> {
            StreamCutRange range = streamCutSupplier.get();
            if (range == null) {
                return null;
            }

            log.info(readerType + " split " + range);

            return new PravegaSplit(
                    connectorId,
                    ObjectType.STREAM,
                    Collections.singletonList(tableHandle.getSchema().get(0)),
                    readerType,
                    serialize(new ReaderArgs(tableHandle.getSchemaName(), stream, range, null)),
                    tableHandle.getSchemaRegistryGroupId());
        };
    }

    Supplier<PravegaSplit> groupedReaderSplitSupplier(final ReaderType readerType,
                                                      final PravegaTableHandle tableHandle,
                                                      final String stream,
                                                      final StreamCutSupplier streamCutSupplier,
                                                      final int numSplits)
    {
        StreamCutRange first = streamCutSupplier.get();
        StreamCutRange last = null;
        do {
            StreamCutRange range = streamCutSupplier.get();
            if (range == null) {
                break;
            }
            last = range;
        } while (true);

        if (last == null) {
            throw new IllegalStateException("no end split");
        }
        StreamCutRange range = new StreamCutRange(first.getStart(), last.getEnd());

        log.info(readerType + " split " + range);
        String readerGroup = UUID.randomUUID().toString();

        final ReaderArgs readerArgs =
                new ReaderArgs(tableHandle.getSchemaName(), stream, range, readerGroup);
        ReaderGroupConfig config =
                ReaderGroupConfig.builder()
                        .stream(scopedName(readerArgs.getScope(), readerArgs.getStream()),
                                readerArgs.getStreamCutRange().getStart(),
                                readerArgs.getStreamCutRange().getEnd())
                        .build();

        log.info("create reader group " + readerGroup);
        streamReaderManager.readerGroupManager(
                tableHandle.getSchemaName()).createReaderGroup(readerGroup, config);

        final AtomicInteger splitCounter = new AtomicInteger();

        return () -> {
            if (splitCounter.getAndIncrement() == numSplits) {
                return null;
            }
            return new PravegaSplit(
                    connectorId,
                    ObjectType.STREAM,
                    Collections.singletonList(tableHandle.getSchema().get(0)),
                    readerType,
                    serialize(readerArgs),
                    tableHandle.getSchemaRegistryGroupId());
        };
    }

    Supplier<PravegaSplit> segmentPerSplitSupplier(final ReaderType readerType,
                                                   final PravegaTableHandle tableHandle,
                                                   final String stream,
                                                   final StreamCutSupplier streamCutSupplier)
    {
        final AtomicReference<Iterator<SegmentRange>> iterator = new AtomicReference<>();

        return () -> {
            if (iterator.get() == null || !iterator.get().hasNext()) {
                StreamCutRange range = streamCutSupplier.get();
                if (range == null) {
                    return null;
                }
                log.info(readerType + " split " + range);
                iterator.set(streamReaderManager.getSegments(tableHandle.getSchemaName(), stream, range.getStart(), range.getEnd()).getIterator());
                if (iterator.get() == null || !iterator.get().hasNext()) {
                    log.info("no more splits");
                    return null;
                }
            }

            SegmentRange segmentRange = iterator.get().next();
            log.info(readerType + " split " + segmentRange);

            return new PravegaSplit(
                    connectorId,
                    ObjectType.STREAM,
                    Collections.singletonList(tableHandle.getSchema().get(0)),
                    readerType,
                    serialize(segmentRange),
                    tableHandle.getSchemaRegistryGroupId());
        };
    }
}
