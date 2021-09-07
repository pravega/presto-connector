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
package io.trino.plugin.pravega.integration;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.pravega.client.admin.StreamManager;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.pravega.PravegaPlugin;
import io.trino.plugin.pravega.PravegaStreamDescription;
import io.trino.plugin.pravega.PravegaTableDescriptionSupplier;
import io.trino.plugin.pravega.PravegaTableName;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingTrinoClient;
import io.trino.tpch.TpchTable;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.pravega.integration.PravegaTestUtils.getKvStreamDesc;
import static io.trino.plugin.pravega.integration.PravegaTestUtils.getStreamDesc;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class PravegaQueryRunner
{
    private PravegaQueryRunner()
    {
    }

    private static final Logger log = Logger.get("PravegaQueryRunner");
    private static final String PRAVEGA_CATALOG = "pravega";
    private static final String KV_SCHEMA = "kv";
    private static final String KV_KEY_FAMILY = "kf1"; // TODO: could randomly pick 1 from objectArgs in table desc
    private static final String TPCH_SCHEMA = "tpch";

    public static DistributedQueryRunner createQueryRunner(URI controller, Iterable<TpchTable<?>> tpchTables, Iterable<String> keyValueTables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(createSession()).setNodeCount(2).build();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            PravegaTableDescriptionSupplier tableDescriptionSupplier =
                    createSchemas(queryRunner.getCoordinator().getMetadata(), tpchTables, keyValueTables);

            installPlugin(controller, queryRunner, tableDescriptionSupplier);

            TestingTrinoClient prestoClient = queryRunner.getClient();

            log.info("Loading data...");
            long startTime = System.nanoTime();
            try (StreamManager streamManager = StreamManager.create(controller)) {
                log.info("Loading tpch data...");
                for (TpchTable<?> table : tpchTables) {
                    loadTpchStream(controller, streamManager, prestoClient, table);
                }
                log.info("Loading tpch complete");

                log.info("Loading pravega data...");
                for (String table : keyValueTables) {
                    loadPravegaKVTable(controller, streamManager, table);
                }
                log.info("Loading pravega complete");
            }

            log.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static void loadTpchStream(URI controller, StreamManager streamManager, TestingTrinoClient prestoClient, TpchTable<?> table)
    {
        long start = System.nanoTime();
        log.info("Running import for %s", table.getTableName());
        PravegaTestUtils.loadTpchStream(controller, streamManager, prestoClient, TPCH_SCHEMA, table.getTableName(), new QualifiedObjectName("tpch", TINY_SCHEMA_NAME, table.getTableName().toLowerCase(ENGLISH)));
        log.info("Imported %s in %s", 0, table.getTableName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    private static void loadPravegaKVTable(URI controller, StreamManager streamManager, String table)
    {
        long start = System.nanoTime();
        log.info("Running import for %s", table);
        PravegaTestUtils.loadKeyValueTable(controller, streamManager, KV_SCHEMA, table, KV_KEY_FAMILY);
        log.info("Imported %s in %s", 0, table, nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    private static PravegaTableDescriptionSupplier createSchemas(Metadata metadata, Iterable<TpchTable<?>> tables, Iterable<String> keyValueTables)
    {
        JsonCodec<PravegaStreamDescription> streamDescCodec = new CodecSupplier<>(PravegaStreamDescription.class, metadata).get();

        Cache<String, Object> schemaCache = CacheBuilder.newBuilder().build();
        Cache<PravegaTableName, Optional<PravegaStreamDescription>> tableCache = CacheBuilder.newBuilder().build();

        for (TpchTable<?> table : tables) {
            SchemaTableName schemaTableName = new SchemaTableName(TPCH_SCHEMA, table.getTableName());
            PravegaTableName pravegaTableName = new PravegaTableName(schemaTableName);

            schemaCache.put(schemaTableName.getSchemaName(), new Object());
            tableCache.put(pravegaTableName, Optional.of(getStreamDesc(streamDescCodec, "tpch", table.getTableName())));
        }

        for (String table : keyValueTables) {
            SchemaTableName schemaTableName = new SchemaTableName(KV_SCHEMA, table);
            PravegaTableName pravegaTableName = new PravegaTableName(schemaTableName);

            schemaCache.put(schemaTableName.getSchemaName(), new Object());
            tableCache.put(pravegaTableName, Optional.of(getKvStreamDesc(table)));
        }

        // all schemas + tables will be served from these provided caches
        return new PravegaTableDescriptionSupplier(null, schemaCache, tableCache);
    }

    public static Session createSession()
    {
        return testSessionBuilder().setCatalog(PRAVEGA_CATALOG).setSchema(TPCH_SCHEMA).build();
    }

    public static void installPlugin(URI controller, QueryRunner queryRunner, PravegaTableDescriptionSupplier tableDescriptionSupplier)
    {
        PravegaPlugin pravegaPlugin = new PravegaPlugin();
        pravegaPlugin.setTableDescriptionSupplier(tableDescriptionSupplier);
        queryRunner.installPlugin(pravegaPlugin);

        Map<String, String> config = ImmutableMap.of(
                "pravega.controller", controller.toASCIIString(),
                "pravega.schema-registry", "http://localhost:9092");

        queryRunner.createCatalog("pravega", "pravega", config);
    }

    public static void main(String[] args)
            throws Exception
    {
        // you need an already running pravega - this code won't start one
        Logging.initialize();
        DistributedQueryRunner queryRunner = createQueryRunner(URI.create("tcp://127.0.0.1:9090"), TpchTable.getTables(), KeyValueTable.getTables());
        Thread.sleep(10);
        Logger log = Logger.get(PravegaQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
