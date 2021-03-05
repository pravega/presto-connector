/*
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
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.KeyValueTableFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.batch.StreamSegmentsIterator;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.serializer.shared.impl.SerializerConfig;

import javax.inject.Inject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * Manages segments and iterators. It uses the Pravega batch API to obtain iterators for
 * a stream segment. One call obtains an iterator for segment ranges, where a segment
 * range is a segment with start and end offsets. Given a segment range, a call to read
 * the segment returns an iterator for the events between start and end offsets.
 */
public class PravegaSegmentManager
{
    private static final Logger log = Logger.get(PravegaSegmentManager.class);
    private ClientConfig clientConfig;
    private Map<String, BatchClientFactory> clientFactoryMap;

    private Map<String, KeyValueTableFactory> tableFactoryMap;

    private Map<String, EventStreamClientFactory> eventStreamClientFactoryMap;

    private Map<String, ReaderGroupManager> scopedReaderGroupManagerMap;

    private SchemaRegistryClientConfig registryConfig;
    private SchemaRegistryClient registryClient;

    @Inject
    public PravegaSegmentManager(PravegaConnectorConfig connectorConfig)
    {
        requireNonNull(connectorConfig, "pravegaConnectorConfig is null");
        clientFactoryMap = new ConcurrentHashMap<>();
        tableFactoryMap = new ConcurrentHashMap<>();
        eventStreamClientFactoryMap = new ConcurrentHashMap<>();
        scopedReaderGroupManagerMap = new ConcurrentHashMap<>();
        clientConfig = ClientConfig.builder().controllerURI(connectorConfig.getControllerURI()).build();
        registryConfig = SchemaRegistryClientConfig.builder()
                .schemaRegistryUri(connectorConfig.getSchemaRegistryURI()).build();
        registryClient = SchemaRegistryClientFactory.withDefaultNamespace(registryConfig);
    }

    private BatchClientFactory batchClientFactory(String scope)
    {
        BatchClientFactory batchClientFactory = clientFactoryMap.get(scope);
        if (batchClientFactory == null) {
            synchronized (this) {
                batchClientFactory = clientFactoryMap.get(scope);
                if (batchClientFactory == null) {
                    batchClientFactory = BatchClientFactory.withScope(scope, clientConfig);
                    if (clientFactoryMap.putIfAbsent(scope, batchClientFactory) != null) {
                        throw new RuntimeException("unexpected concurrent create of batch client factory");
                    }
                }
            }
        }
        return batchClientFactory;
    }

    /**
     * Returns a list of {@link SegmentRange} instances.
     *
     * @param scope  scope where stream lives
     * @param stream Stream to read from
     * @param start  The initial position in the stream
     * @param end    The end position in the stream
     * @return A {@link SegmentRange} iterator
     */
    StreamSegmentsIterator getSegments(String scope, String stream, StreamCut start, StreamCut end)
    {
        return batchClientFactory(scope).getSegments(Stream.of(scope, stream), start, end);
    }

    /**
     * Returns an iterator for the given segment range. It returns the bytes of the event
     * and makes no attempt to deserialize it here.
     *
     * @param segmentRange The segment range to iterate over
     * @param serializer   The basic serializer to return the event bytes
     * @return An iterator for the
     */
    SegmentIterator getSegmentIterator(SegmentRange segmentRange, ByteBufferSerializer serializer)
    {
        return batchClientFactory(segmentRange.getScope()).readSegment(segmentRange, serializer);
    }

    KeyValueTableFactory getKeyValueTableFactory(String scope)
    {
        KeyValueTableFactory factory = tableFactoryMap.get(scope);
        if (factory == null) {
            synchronized (this) {
                factory = tableFactoryMap.get(scope);
                if (factory == null) {
                    factory = KeyValueTableFactory.withScope(scope, clientConfig);
                    if (tableFactoryMap.putIfAbsent(scope, factory) != null) {
                        throw new RuntimeException("unexpected concurrent create of table factory");
                    }
                }
            }
        }
        return factory;
    }

    EventStreamClientFactory getEventStreamClientFactory(String scope)
    {
        EventStreamClientFactory factory = eventStreamClientFactoryMap.get(scope);
        if (factory == null) {
            synchronized (this) {
                factory = eventStreamClientFactoryMap.get(scope);
                if (factory == null) {
                    factory = EventStreamClientFactory.withScope(scope, clientConfig);
                    if (eventStreamClientFactoryMap.putIfAbsent(scope, factory) != null) {
                        throw new RuntimeException("unexpected concurrent create of event stream factory");
                    }
                }
            }
        }
        return factory;
    }

    ReaderGroupManager readerGroupManager(String scope)
    {
        ReaderGroupManager readerGroupManager = scopedReaderGroupManagerMap.get(scope);
        if (readerGroupManager == null) {
            synchronized (this) {
                readerGroupManager = scopedReaderGroupManagerMap.get(scope);
                if (readerGroupManager == null) {
                    readerGroupManager = ReaderGroupManager.withScope(scope, clientConfig);
                    if (scopedReaderGroupManagerMap.putIfAbsent(scope, readerGroupManager) != null) {
                        throw new RuntimeException("unexpected concurrent create of reader group manager");
                    }
                }
            }
        }
        return readerGroupManager;
    }

    SerializerConfig serializerConfig(String groupId)
    {
        return SerializerConfig.builder()
                .registryClient(registryClient)
                .groupId(groupId).build();
    }

    public boolean streamExists(String scope, String stream)
    {
        try (StreamManager streamManager = StreamManager.create(clientConfig.getControllerURI())) {
            return streamManager.checkStreamExists(scope, stream);
        }
    }
}
