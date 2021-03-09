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

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorMetadata;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorSplitManager;
import com.google.inject.Injector;
import com.google.inject.Scopes;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

/**
 * Creates Pravega Connectors based off connectorId and specific configuration.
 */
public class PravegaConnectorFactory
        implements ConnectorFactory
{
    private final Optional<PravegaTableDescriptionSupplier> tableDescriptionSupplier;
    private final ClassLoader classLoader;

    PravegaConnectorFactory(Optional<PravegaTableDescriptionSupplier> tableDescriptionSupplier, ClassLoader classLoader)
    {
        this.tableDescriptionSupplier = requireNonNull(tableDescriptionSupplier, "tableDescriptionSupplier is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public String getName()
    {
        return "pravega";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new PravegaHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(config, "config is null");

        try {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new PravegaConnectorModule(),
                    binder -> {
                        binder.bind(PravegaConnectorId.class).toInstance(new PravegaConnectorId(catalogName));
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());

                        if (tableDescriptionSupplier.isPresent()) {
                            binder.bind(PravegaTableDescriptionSupplier.class).toInstance(tableDescriptionSupplier.get());
                        }
                        else {
                            binder.bind(PravegaTableDescriptionSupplier.class).in(Scopes.SINGLETON);
                        }
                    });

            Injector injector = app.doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);
            PravegaMetadata pravegaMetadata = injector.getInstance(PravegaMetadata.class);
            PravegaSplitManager pravegaSplitManager = injector.getInstance(PravegaSplitManager.class);
            PravegaRecordSetProvider pravegaRecordSetProvider = injector.getInstance(PravegaRecordSetProvider.class);

            return new PravegaConnector(lifeCycleManager,
                    new ClassLoaderSafeConnectorMetadata(pravegaMetadata, classLoader),
                    new ClassLoaderSafeConnectorSplitManager(pravegaSplitManager, classLoader),
                    new ClassLoaderSafeRecordSetProvider(pravegaRecordSetProvider, classLoader));
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
