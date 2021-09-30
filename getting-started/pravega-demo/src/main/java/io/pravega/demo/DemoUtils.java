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

import com.google.common.collect.ImmutableMap;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.client.exceptions.RegistryExceptions;
import io.pravega.schemaregistry.contract.data.Compatibility;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import javax.ws.rs.ProcessingException;
import java.io.PrintWriter;

public class DemoUtils {

    private DemoUtils() {}

    public static String groupId(String scope, String stream) {
        // scope.stream which translates to presto schema.table
        return scope + "." + stream;
    }

    public static void help(Options options) {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("-", options);
    }

    public static void waitForSchemaRegistry(PrintWriter log, SchemaRegistryClientConfig config) {
        SchemaRegistryClient client =
                SchemaRegistryClientFactory.withDefaultNamespace(config);
        boolean failed = false;
        do {
            try {
                // some APIs return empty result set when cannot connect
                // so look for explicit ResourceNotFoundException exception here to ensure up+running
                // groupId that won't exist
                client.getSchemas("__abcdefghijklmnopqrstuvwxyz__");
                break;
            } catch (ProcessingException e) {
                failed = true;
                log.println(e);
                log.println("schema registry (" + config.getSchemaRegistryUri() + ") not ready, wait (ctrl+c to quit)");
                log.flush();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(interrupted);
                }
            } catch (RegistryExceptions.ResourceNotFoundException e) {
                if (failed) {
                    log.println("schema registry (" + config.getSchemaRegistryUri() + ") ready");
                    log.flush();
                }
                break;
            }
        } while (true);
    }

    public static void addSchemaRegistryGroupIfAbsent(SchemaRegistryClientConfig schemaRegistryConfig,
                                                      SerializationFormat serializationFormat,
                                                      String scope, String stream,
                                                      ImmutableMap<String, String> properties) {
        // note our SR group format - scope.stream which translates to presto schema.table
        String groupId = groupId(scope, stream);

        SchemaRegistryClient schemaRegistryClient =
                SchemaRegistryClientFactory.withDefaultNamespace(schemaRegistryConfig);
        try {
            schemaRegistryClient.getSchemas(groupId);
        }
        catch (RegistryExceptions.ResourceNotFoundException e) {
            if (properties == null) {
                properties = ImmutableMap.of();
            }
            GroupProperties groupProperties =
                    new GroupProperties(serializationFormat, Compatibility.allowAny(), false, properties);
            schemaRegistryClient.addGroup(groupId, groupProperties);
        }
    }
}
