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
package io.pravega.connectors.presto.schemamanagement;

import com.facebook.presto.spi.SchemaTableName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.connectors.presto.ObjectType;
import io.pravega.connectors.presto.PravegaStreamDescription;
import io.pravega.connectors.presto.PravegaStreamFieldGroup;
import io.pravega.connectors.presto.PravegaTableHandle;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.client.exceptions.RegistryExceptions;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.SerializationFormat;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static io.pravega.connectors.presto.ProtobufCommon.encodeSchema;
import static io.pravega.connectors.presto.util.PravegaNameUtils.groupId;
import static io.pravega.connectors.presto.util.PravegaNameUtils.internalObject;
import static io.pravega.connectors.presto.util.PravegaNameUtils.internalStream;
import static io.pravega.connectors.presto.util.PravegaNameUtils.kvFieldMapping;
import static io.pravega.connectors.presto.util.PravegaNameUtils.temp_streamNameToTableName;
import static io.pravega.connectors.presto.util.PravegaNameUtils.temp_tableNameToStreamName;
import static io.pravega.connectors.presto.util.PravegaSchemaUtils.GROUP_PROPERTIES_INLINE_KEY;
import static io.pravega.connectors.presto.util.PravegaSchemaUtils.GROUP_PROPERTIES_INLINE_KV_KEY;
import static io.pravega.connectors.presto.util.PravegaSchemaUtils.GROUP_PROPERTIES_INLINE_KV_VALUE;
import static io.pravega.connectors.presto.util.PravegaSchemaUtils.INLINE_SUFFIX;
import static io.pravega.connectors.presto.util.PravegaStreamDescUtils.mapFieldsFromSchema;

public class PravegaSchemaRegistry
        implements SchemaSupplier, SchemaRegistry
{
    private final SchemaRegistryClient registryClient;

    private final StreamManager streamManager;

    public PravegaSchemaRegistry(URI controllerURI, URI schemaRegistryURI)
    {
        SchemaRegistryClientConfig registryConfig =
                SchemaRegistryClientConfig.builder().schemaRegistryUri(schemaRegistryURI).build();
        this.registryClient = SchemaRegistryClientFactory.withDefaultNamespace(registryConfig);
        this.streamManager = StreamManager.create(ClientConfig.builder().controllerURI(controllerURI).build());
    }

    @VisibleForTesting
    public PravegaSchemaRegistry(SchemaRegistryClient registryClient, StreamManager streamManager)
    {
        this.registryClient = registryClient;
        this.streamManager = streamManager;
    }

    @Override
    public List<String> listSchemas()
    {
        List<String> schemas = new ArrayList<>();
        Streams.stream(streamManager.listScopes()).filter(s -> !internalObject(s)).forEach(schemas::add);
        return schemas;
    }

    @Override
    public List<PravegaTableHandle> listTables(String schema)
    {
        // (underlying streams used by kv table are seen as internal and thus are skipped)
        List<PravegaTableHandle> tables = new ArrayList<>();
        Streams.stream(streamManager.listStreams(schema))
                .filter(stream -> !internalStream(stream))
                .forEach(stream -> {
                    tables.add(new PravegaTableHandle(schema,
                            temp_streamNameToTableName(stream.getStreamName()),
                            stream.getStreamName(),
                            ObjectType.STREAM,
                            Optional.empty()));
                });
        return tables;
    }

    @Override
    public List<PravegaStreamFieldGroup> getSchema(SchemaTableName schemaTableName) {
        String groupName = groupId(schemaTableName.getSchemaName(),
                temp_tableNameToStreamName(schemaTableName.getTableName()));

        GroupProperties properties;
        List<SchemaWithVersion> schemas;

        try {
            properties = registryClient.getGroupProperties(groupName);
            schemas = registryClient.getSchemas(groupName);
        }
        catch (RegistryExceptions.ResourceNotFoundException e) {
            return null;
        }

        if (schemas.size() == 0 || schemas.size() > 2) {
            throw new IllegalStateException(schemaTableName + " has " + schemas.size() + " registered schemas.  expecting either 1 or 2");
        }

        // kv table will have > 1 schema.  key+value likely different types
        boolean kv = schemas.size() > 1;
        List<PravegaStreamFieldGroup> fieldGroups = new ArrayList<>(2);
        for (int i = 0; i < schemas.size(); i++) {
            // colPrefix used for display so can differentiate between fields from key or value
            String colPrefix = kv ? kvFieldMapping(i) : "";

            SerializationFormat format = schemas.get(i).getSchemaInfo().getSerializationFormat();
            fieldGroups.add(new PravegaStreamFieldGroup(
                    dataFormat(properties.getProperties(), format, kv, i),
                    Optional.of(colPrefix),
                    dataSchema(format, schemas.get(i)),
                    Optional.of(mapFieldsFromSchema(colPrefix, format, schemas.get(i)))));
        }

        return fieldGroups;
    }

    @Override
    public PravegaStreamDescription getTable(SchemaTableName schemaTableName)
    {
        List<PravegaStreamFieldGroup> schema = getSchema(schemaTableName);
        if (schema == null) {
            return null;
        }

        return new PravegaStreamDescription(
                schemaTableName.getTableName(),
                Optional.of(schemaTableName.getSchemaName()),
                temp_tableNameToStreamName(schemaTableName.getTableName()),
                Optional.of(ObjectType.STREAM),
                Optional.empty() /* args */,
                Optional.of(schema));
    }

    private static String dataFormat(ImmutableMap<String, String> groupProperties,
                                     SerializationFormat format,
                                     boolean kvTable,
                                     int kvIdx)
    {
        /*
            TODO: auto-detect https://github.com/pravega/presto-connector/issues/20

            (1) no schema registry.
            (2) Register and evolve schemas in registry but do not use registry client while writing data
            (3) Register schemas in the registry and use registry client to encode schema Id with payload
            "inline" is for #3.  for e.g. "avro" -> "avro-inline".  PravegaRecordSetProvider is interested in this

            hopefully this can all go away (see linked issue 58 above)

            but for now the following is our convention
            if "inline" exists in our properties, all data uses SR
            else if it is a kv table key+value may be different.  both, neither, or either may use SR
            look for "inlinekey" / "inlinevalue"
         */

        String key = GROUP_PROPERTIES_INLINE_KEY;

        if (kvTable && !groupProperties.containsKey(key)) {
            key = kvIdx == 0 ? GROUP_PROPERTIES_INLINE_KV_KEY : GROUP_PROPERTIES_INLINE_KV_VALUE;
        }

        String finalFormat = format == SerializationFormat.Custom
                ? format.getFullTypeName().toLowerCase(Locale.ENGLISH)
                : format.name().toLowerCase(Locale.ENGLISH);
        return finalFormat + (groupProperties.containsKey(key) ? INLINE_SUFFIX : "");
    }

    public static Optional<String> dataSchema(SerializationFormat format, SchemaWithVersion schemaWithVersion)
    {
        // it is intentional that nothing is returned for Custom
        // pass schema to row decoders.  refer to PravegaRecordSetProvider
        switch (format) {
            case Protobuf:
                return Optional.of(encodeSchema(schemaWithVersion));
            case Avro:
                return Optional.of(new String(schemaWithVersion.getSchemaInfo().getSchemaData().array(), StandardCharsets.UTF_8));
            default:
                return Optional.empty();
        }
    }
}
