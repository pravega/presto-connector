/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Note: this class is based on SetupUtils from pravega/flink-connectors
 * https://github.com/pravega/flink-connectors/blob/v0.9.0/src/test/java/io/pravega/connectors/flink/utils/SetupUtils.java
 */
package io.pravega.connectors.presto.integration;

import com.facebook.airlift.log.Logger;
import io.pravega.local.InProcPravegaCluster;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class EmbeddedPravega
        implements Closeable
{
    private static final Logger log = Logger.get(EmbeddedPravega.class);

    private static final String PRAVEGA_USERNAME = "admin";
    private static final String PRAVEGA_PASSWORD = "1111_aaaa";
    private static final String PASSWD_FILE = "passwd";
    private static final String KEY_FILE = "server-key.key";
    private static final String CERT_FILE = "server-cert.crt";
    private static final String STANDALONE_KEYSTORE_FILE = "server.keystore.jks";
    private static final String STANDALONE_TRUSTSTORE_FILE = "client.truststore.jks";
    private static final String STANDALONE_KEYSTORE_PASSWD_FILE = "server.keystore.jks.passwd";

    private final AtomicBoolean started = new AtomicBoolean(false);

    private final InProcPravegaCluster embeddedPravega;

    private final int controllerPort;

    private final static AtomicInteger servers = new AtomicInteger();

    public EmbeddedPravega() throws Exception
    {
        int server = servers.getAndIncrement();
        this.controllerPort = 9090 + server;
        int zkPort = 2181 + server;
        int hostPort = 12345 + server;
        int restPort = 8080 + server;

        this.embeddedPravega = InProcPravegaCluster.builder()
                .isInProcZK(true)
                .secureZK(false)
                .zkUrl("localhost:" + zkPort)
                .zkPort(zkPort)
                .isInMemStorage(true)
                .isInProcController(true)
                .controllerCount(1)
                .restServerPort(restPort)
                .enableRestServer(true)
                .isInProcSegmentStore(true)
                .segmentStoreCount(1)
                .containerCount(4)
                .enableMetrics(false)
                .enableAuth(false)
                .enableTls(false)
                .certFile(getFileFromResource(CERT_FILE))
                .keyFile(getFileFromResource(KEY_FILE))
                .jksKeyFile(getFileFromResource(STANDALONE_KEYSTORE_FILE))
                .jksTrustFile(getFileFromResource(STANDALONE_TRUSTSTORE_FILE))
                .keyPasswordFile(getFileFromResource(STANDALONE_KEYSTORE_PASSWD_FILE))
                .passwdFile(getFileFromResource(PASSWD_FILE))
                .userName(PRAVEGA_USERNAME)
                .passwd(PRAVEGA_PASSWORD)
                .build();

        this.embeddedPravega.setControllerPorts(new int[]{controllerPort});
        this.embeddedPravega.setSegmentStorePorts(new int[]{hostPort});
        this.embeddedPravega.start();
    }

    public URI getController()
    {
        return URI.create("tcp://localhost:" + controllerPort);
    }

    static String getFileFromResource(String resourceName)
    {
        try {
            Path tempPath = Files.createTempFile("test-", ".tmp");
            tempPath.toFile().deleteOnExit();
            try (InputStream stream = EmbeddedPravega.class.getClassLoader().getResourceAsStream(resourceName)) {
                Files.copy(stream, tempPath, StandardCopyOption.REPLACE_EXISTING);
            }
            return tempPath.toFile().getAbsolutePath();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void stop()
    {
        if (!this.started.compareAndSet(true, false)) {
            log.warn("Services not yet started or already stopped, not attempting to stop");
            return;
        }

        try {
            embeddedPravega.close();
        }
        catch (Exception e) {
            log.warn("Services did not stop cleanly (" + e.getMessage() + ")", e);
        }
    }

    @Override
    public void close()
    {
        try {
            stop();
        }
        catch (Exception quiet) {}
    }
}