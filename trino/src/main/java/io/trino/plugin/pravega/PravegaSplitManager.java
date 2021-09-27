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

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.pravega.client.batch.SegmentRange;
import io.trino.spi.HostAddress;
import io.trino.spi.Node;
import io.trino.spi.NodeManager;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableLayoutHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedSplitSource;

import javax.inject.Inject;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static io.trino.plugin.pravega.PravegaErrorCode.PRAVEGA_SPLIT_ERROR;
import static io.trino.plugin.pravega.PravegaHandleResolver.convertLayout;
import static io.trino.plugin.pravega.util.PravegaNameUtils.multiSourceStream;
import static io.trino.plugin.pravega.util.PravegaSerializationUtils.serialize;
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
    private final NodeManager nodeManager;

    @Inject
    public PravegaSplitManager(
            PravegaConnectorId connectorId,
            PravegaConnectorConfig pravegaConnectorConfig,
            PravegaSegmentManager streamReaderManager,
            NodeManager nodeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.streamReaderManager = requireNonNull(streamReaderManager, "streamReaderManager is null");
        this.pravegaConnectorConfig = requireNonNull(pravegaConnectorConfig, "pravegaConnectorConfig is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingStrategy splitSchedulingStrategy)
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
            if (e instanceof TrinoException) {
                throw e;
            }
            throw new TrinoException(PRAVEGA_SPLIT_ERROR,
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

    private List<HostAddress> getNodeAddresses()
    {
        ImmutableList.Builder<HostAddress> addresses = ImmutableList.builder();
        for (Node node : nodeManager.getRequiredWorkerNodes()) {
            addresses.add(node.getHostAndPort());
        }
        return addresses.build();
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
                            pravegaTableHandle.getSchemaRegistryGroupId(),
                            getNodeAddresses());
            splits.add(split);
        }

        log.info("created " + pravegaTableHandle.getOjectArgs().get().size() + " kv splits");
    }

    private void buildStreamSplits(final PravegaProperties properties,
            PravegaTableHandle pravegaTableHandle,
            ImmutableList.Builder<ConnectorSplit> splits)
    {
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
            }
            while (split != null);
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
                    tableHandle.getSchemaRegistryGroupId(),
                    getNodeAddresses());
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
                    tableHandle.getSchemaRegistryGroupId(),
                    getNodeAddresses());
        };
    }
}
