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
 * limitations under the License.Copyright (c) Pravega Authors.
 */

package com.facebook.presto.pravega;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableMap;

import static com.facebook.airlift.testing.Assertions.assertInstanceOf;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertNotNull;

public class TestPravegaPlugin
{
    public void testSpinup()
    {
        PravegaPlugin plugin = new PravegaPlugin();

        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertInstanceOf(factory, PravegaConnectorFactory.class);

        Connector c = factory.create(
                "test-connector",
                ImmutableMap.<String, String>builder()
                        .put("pravega.table-names", "test")
                        .put("pravega.nodes", "localhost:9092")
                        .build(),
                new TestingConnectorContext());
        assertNotNull(c);
    }
}
