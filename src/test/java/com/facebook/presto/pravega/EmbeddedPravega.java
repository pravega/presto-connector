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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.Closeable;
import java.net.URI;

public class EmbeddedPravega
        implements Closeable
{
    private final DockerContainer dockerContainer;

    public EmbeddedPravega()
    {
        this.dockerContainer = new DockerContainer(
                "pravega/pravega:latest",
                "standalone",
                ImmutableList.of(9090, 12345),
                ImmutableMap.of(
                        "HOST_IP", "127.0.0.1"),
                EmbeddedPravega::healthCheck);
    }

    public URI getController()
    {
        return URI.create("tcp://localhost:" + dockerContainer.getHostPort(9090));
    }

    private static void healthCheck(DockerContainer.HostPortProvider hostPortProvider)
    {
    }

    @Override
    public void close()
    {
        dockerContainer.close();
    }
}
