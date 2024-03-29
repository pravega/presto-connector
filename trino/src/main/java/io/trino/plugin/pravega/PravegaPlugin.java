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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Presto plugin to use Apache Pravega as a data source.
 */
public class PravegaPlugin
        implements Plugin
{
    private Optional<PravegaTableDescriptionSupplier> tableDescriptionSupplier = Optional.empty();

    @VisibleForTesting
    public synchronized void setTableDescriptionSupplier(PravegaTableDescriptionSupplier tableDescriptionSupplier)
    {
        this.tableDescriptionSupplier = Optional.of(requireNonNull(tableDescriptionSupplier, "tableDescriptionSupplier is null"));
    }

    @Override
    public synchronized Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new PravegaConnectorFactory(tableDescriptionSupplier, getClassLoader()));
    }

    private static ClassLoader getClassLoader()
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = PravegaPlugin.class.getClassLoader();
        }
        return classLoader;
    }
}
