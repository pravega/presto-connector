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

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorHandleResolver;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayoutHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Pravega specific {@link ConnectorHandleResolver} implementation.
 */
public class PravegaHandleResolver
        implements ConnectorHandleResolver
{
    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass()
    {
        return PravegaTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass()
    {
        return PravegaColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass()
    {
        return PravegaSplit.class;
    }

    @Override
    public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass()
    {
        return PravegaTableLayoutHandle.class;
    }

    @Override
    public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass()
    {
        return PravegaTransactionHandle.class;
    }

    static PravegaTableHandle convertTableHandle(ConnectorTableHandle tableHandle)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof PravegaTableHandle, "tableHandle is not an instance of PravegaTableHandle");
        return (PravegaTableHandle) tableHandle;
    }

    static PravegaColumnHandle convertColumnHandle(ColumnHandle columnHandle)
    {
        requireNonNull(columnHandle, "columnHandle is null");
        checkArgument(columnHandle instanceof PravegaColumnHandle, "columnHandle is not an instance of PravegaColumnHandle");
        return (PravegaColumnHandle) columnHandle;
    }

    static PravegaSplit convertSplit(ConnectorSplit split)
    {
        requireNonNull(split, "split is null");
        checkArgument(split instanceof PravegaSplit, "split is not an instance of PravegaSplit");
        return (PravegaSplit) split;
    }

    static PravegaTableLayoutHandle convertLayout(ConnectorTableLayoutHandle layout)
    {
        requireNonNull(layout, "layout is null");
        checkArgument(layout instanceof PravegaTableLayoutHandle, "layout is not an instance of PravegaTableLayoutHandle");
        return (PravegaTableLayoutHandle) layout;
    }
}
