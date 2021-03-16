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

import javax.inject.Inject;

import java.util.Collections;
import java.util.List;

import static com.facebook.presto.pravega.PravegaErrorCode.PRAVEGA_SPLIT_ERROR;
import static com.facebook.presto.pravega.PravegaHandleResolver.convertLayout;
import static com.facebook.presto.pravega.util.PravegaNameUtils.multiSourceStream;
import static com.facebook.presto.pravega.util.PravegaSerializationUtils.serialize;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Pravega specific implementation of {@link ConnectorSplitManager}.
 */
public class PravegaSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(PravegaSplitManager.class);
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

        try {
            return pravegaTableHandle.getObjectType() == ObjectType.KV_TABLE
                    ? buildKVSplits(pravegaTableHandle)
                    : buildStreamSplits(new PravegaProperties(session), pravegaTableHandle);
        }
        catch (Exception e) {
            if (e instanceof PrestoException) {
                throw e;
            }
            throw new PrestoException(PRAVEGA_SPLIT_ERROR,
                    format("Cannot list splits for table '%s' reading stream '%s'",
                            pravegaTableHandle.getTableName(), pravegaTableHandle.getObjectName()), e);
        }
    }

    private ConnectorSplitSource buildKVSplits(PravegaTableHandle pravegaTableHandle)
    {
        pravegaTableHandle.getOjectArgs().orElseThrow(() ->
                new IllegalArgumentException("no KF defined for " + pravegaTableHandle));

        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

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

        return new FixedSplitSource(splits.build());
    }

    private ConnectorSplitSource buildStreamSplits(final PravegaProperties properties,
                                                   PravegaTableHandle pravegaTableHandle)
    {
        List<String> sourceStreams = multiSourceStream(pravegaTableHandle)
                ? pravegaTableHandle.getOjectArgs().orElseThrow(() -> new IllegalArgumentException("no args for multi source table found"))
                : Collections.singletonList(pravegaTableHandle.getObjectName());

        return new PravegaSplitSource(connectorId,
                pravegaTableHandle,
                sourceStreams,
                streamReaderManager,
                properties);
    }
}
