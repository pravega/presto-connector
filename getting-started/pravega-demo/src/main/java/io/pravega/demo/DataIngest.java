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
package io.pravega.demo;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;

import java.io.PrintWriter;
import java.net.URI;
import java.util.Random;
import java.util.function.Supplier;

public class DataIngest<T> {

    private final Random random = new Random();

    private final ClientConfig clientConfig;

    public DataIngest(URI controllerUri) {
        this.clientConfig = ClientConfig.builder().controllerURI(controllerUri).build();
    }

    public void ingest(PrintWriter log, String scope, String stream, Serializer<T> serializer, Supplier<T> data) {
        // write object/event data to pravega stream
        // note this class/method doesn't know anything about schema

        log.println("start data ingestion to " + scope + "." + stream);
        log.flush();

        try (StreamManager streamManager = StreamManager.create(clientConfig)) {
            if (!streamManager.checkScopeExists(scope)) {
                streamManager.createScope(scope);
            }

            if (!streamManager.checkStreamExists(scope, stream)) {
                StreamConfiguration streamConfiguration =
                        StreamConfiguration.builder()
                                .scalingPolicy(ScalingPolicy.fixed(10)) // number of segments
                                .build();
                streamManager.createStream(scope, stream, streamConfiguration);
            }

            try (EventStreamClientFactory factory = EventStreamClientFactory.withScope(scope, clientConfig);
                 EventStreamWriter<T> streamWriter = factory.createEventWriter(stream,
                         serializer,
                         EventWriterConfig.builder().automaticallyNoteTime(true).build())) {

                int flushInterval = 100;
                int events = 0;

                T next = data.get();
                while (next != null) {
                    // 1st param is routing key.  use random routing key so that events are spread
                    // evenly amongst the segments (10, ScalingPolicy from above)
                    // in a real world scenario routing key may be from the data itself
                    streamWriter.writeEvent(String.valueOf(random.nextInt(999)), next);

                    if (++events % flushInterval == 0) {
                        streamWriter.flush();
                        log.println("wrote " + events + " events");
                        log.flush();
                    }

                    next = data.get();
                }

                streamWriter.flush();
                log.println("wrote " + events + " events");
            }
        }

        log.println("done");
        log.flush();
    }
}
