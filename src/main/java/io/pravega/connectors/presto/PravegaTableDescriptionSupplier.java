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

package io.pravega.connectors.presto;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import com.google.protobuf.Descriptors;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.Stream;
import io.pravega.schemaregistry.client.SchemaRegistryClient;
import io.pravega.schemaregistry.client.SchemaRegistryClientConfig;
import io.pravega.schemaregistry.client.SchemaRegistryClientFactory;
import io.pravega.schemaregistry.contract.data.GroupProperties;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.json.schemas.JSONSchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static io.pravega.connectors.presto.ProtobufCommon.encodeSchema;
import static io.pravega.connectors.presto.util.PravegaNameUtils.groupId;
import static io.pravega.connectors.presto.util.PravegaNameUtils.kvFieldMapping;
import static io.pravega.connectors.presto.util.PravegaNameUtils.kvTable;
import static io.pravega.connectors.presto.util.PravegaNameUtils.multiSourceStream;
import static io.pravega.connectors.presto.util.PravegaNameUtils.temp_streamNameToTableName;
import static io.pravega.connectors.presto.util.PravegaNameUtils.temp_tableNameToStreamName;
import static io.pravega.connectors.presto.util.PravegaSchemaUtils.AVRO;
import static io.pravega.connectors.presto.util.PravegaSchemaUtils.GROUP_PROPERTIES_INLINE_KEY;
import static io.pravega.connectors.presto.util.PravegaSchemaUtils.GROUP_PROPERTIES_INLINE_KV_KEY;
import static io.pravega.connectors.presto.util.PravegaSchemaUtils.GROUP_PROPERTIES_INLINE_KV_VALUE;
import static io.pravega.connectors.presto.util.PravegaSchemaUtils.INLINE_SUFFIX;
import static io.pravega.connectors.presto.util.PravegaSchemaUtils.NESTED_RECORD_SEPARATOR;
import static io.pravega.connectors.presto.util.PravegaSchemaUtils.readSchema;
import static java.nio.file.Files.readAllBytes;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.apache.avro.Schema.Type.RECORD;

// pravega scope is a namespace for streams.  stream is unique within scope.
// presto schema is like a database, with collection of tables.
// we will map scope->schema and stream->table
// scope will be our database and streams will be tables in the database
//
// a stream schema could be "local". local definitions take precedence.
// .json file in known directory with naming format <schema>.<table>.json
// there could be any number of local schemas.
//
// additionally a local stream schema could be a composite table.  this is called 'multi-source'.
// stream name will be a regex.  it can match 1 or more source streams.  when this single table is
// queried we will consider events from all source streams
public class PravegaTableDescriptionSupplier
{
    private static final Logger log = Logger.get(PravegaTableDescriptionSupplier.class);

    private final PravegaConnectorConfig pravegaConnectorConfig;

    private Cache<String, Object> schemaCache;

    private Cache<PravegaTableName, Optional<PravegaStreamDescription>> tableCache;

    private JsonCodec<PravegaStreamDescription> streamDescriptionCodec;

    // "inline" means that event was written using schema registry wrapped client and schema encoding id
    // is within the raw event data in pravega
    @Inject
    PravegaTableDescriptionSupplier(PravegaConnectorConfig pravegaConnectorConfig,
                                    JsonCodec<PravegaStreamDescription> streamDescriptionCodec)
    {
        requireNonNull(pravegaConnectorConfig, "pravegaConfig is null");
        this.pravegaConnectorConfig = pravegaConnectorConfig;
        this.streamDescriptionCodec = streamDescriptionCodec;

        // there will be many successive calls to listSchemas + listTables in short time period
        // do not reach out to pravega each time as it is unlikely things would have changed
        // enhancement issue - can we determine if there are changes/removals and selectively update?
        // https://github.com/StreamingDataPlatform/pravega-sql/issues/101
        this.schemaCache = CacheBuilder.newBuilder()
                .expireAfterWrite(pravegaConnectorConfig.getTableCacheExpireSecs(), TimeUnit.SECONDS)
                .build();

        this.tableCache = CacheBuilder.newBuilder()
                .expireAfterWrite(pravegaConnectorConfig.getTableCacheExpireSecs(), TimeUnit.SECONDS)
                .build();
    }

    public List<String> listSchemas()
    {
        // if any expired, retrieve list again from pravega
        // they are inserted to cache at same time so will all be same state
        final List<String> schemas = schemaCache.asMap().keySet().stream().collect(Collectors.toList());
        if (schemas.isEmpty()) {
            listLocalSchemas().forEach(schema -> schemaCache.put(schema, new Object()));

            try (StreamManager streamManager =
                         StreamManager.create(ClientConfig.builder().controllerURI(
                                 pravegaConnectorConfig.getControllerURI()).build())) {
                Streams.stream(streamManager.listScopes()).filter(s -> !internalObject(s)).forEach(schema -> {
                    schemas.add(schema);
                    schemaCache.put(schema, new Object());
                });
            }
        }
        else {
            log.info("serving listSchemas() from cache");
        }
        return schemas;
    }

    public List<PravegaTableName> listTables(Optional<String> schema)
    {
        List<String> schemas = schema.isPresent() ? Collections.singletonList(schema.get()) : listSchemas();

        StreamManager streamManager = null;

        try {
            List<PravegaTableName> tableList = new ArrayList<>();

            for (String s : schemas) {
                List<PravegaTableName> tableListForSchema =
                        tableCache.asMap().keySet().stream()
                                .filter(streamDesc -> streamDesc.getSchemaTableName().getSchemaName().startsWith(s))
                                .collect(Collectors.toList());

                // not all tables inserted to cache at same time
                if (tableListForSchema.isEmpty()) {
                    if (streamManager == null) {
                        streamManager = StreamManager.create(
                                ClientConfig.builder().controllerURI(pravegaConnectorConfig.getControllerURI()).build());
                    }

                    List<Pattern> compositeStreams = new ArrayList<>();

                    // local takes precedence.  list before pravega.  ifAbsent used later to not clobber.
                    listLocalTables(s).forEach(table -> {
                        PravegaTableName pravegaTableName = new PravegaTableName(s, table);

                        // don't clobber existing entry
                        if (tableCache.getIfPresent(pravegaTableName) == null ||
                                !tableCache.getIfPresent(pravegaTableName).isPresent()) {
                            tableCache.put(pravegaTableName, Optional.empty());
                        }

                        // load .json def to get stream name in order to determine type
                        PravegaStreamDescription localTable = getLocalTable(pravegaTableName.getSchemaTableName());
                        if (multiSourceStream(localTable)) {
                            compositeStreams.add(Pattern.compile(localTable.getObjectName()));
                        }
                    });

                    // (underlying streams used by kv table are seen as internal and thus are skipped)
                    Streams.stream(streamManager.listStreams(s))
                            .filter(stream -> !internalStream(stream))
                            .forEach(stream -> {
                                boolean hidden =
                                        compositeStreams.stream().anyMatch(p -> p.matcher(stream.getStreamName()).matches());
                                // ifAbsent - don't clobber table description if we have it
                                PravegaTableName tableName = new PravegaTableName(s, temp_streamNameToTableName(stream.getStreamName()), hidden);
                                if (tableCache.getIfPresent(tableName) == null ||
                                        !tableCache.getIfPresent(tableName).isPresent()) {
                                    tableCache.put(tableName, Optional.empty());
                                }
                            });
                }
                else {
                    log.info("serving listTables(%s) from cache", s);
                }

                tableList.addAll(tableCache.asMap().keySet().stream()
                        .filter(pravegaStreamDescription ->
                                pravegaStreamDescription.getSchemaTableName().getSchemaName().startsWith(s))
                        .collect(Collectors.toList()));
            }
            return tableList;
        }
        finally {
            if (streamManager != null) {
                streamManager.close();
            }
        }
    }

    public PravegaStreamDescription getTable(SchemaTableName schemaTableName)
    {
        PravegaTableName pravegaTableName = new PravegaTableName(schemaTableName);
        Optional<PravegaStreamDescription> cachedTable = tableCache.getIfPresent(pravegaTableName);
        if (cachedTable != null && cachedTable.isPresent()) {
            log.info("serving getTable(%s) from cache", schemaTableName);
            return cachedTable.get();
        }

        PravegaStreamDescription table = getLocalTable(schemaTableName);
        if (table != null) {
            log.info("found local schema for '%s'", schemaTableName);

            // kv this is list of key family (defined in local schema file)
            // for multi source stream this is list of composite streams (empty here, to be filled in later)
            Optional<List<String>> objectArgs = table.getObjectArgs();

            // field definitions can come from 1 of 4 places
            // (1) defined in local .json schema ("event/fields")
            // (2) uri in "dataSchema" field
            // (3) lookup from a source stream (if multi source stream)
            // (4) lookup directly in schema registry (if kv table)

            Optional<List<PravegaStreamFieldGroup>> fieldGroups = Optional.empty();

            if (fieldsDefined(table)) {
                // case (1) - no-op
                log.info("fields defined in schema file %s", schemaTableName);
                fieldGroups = Optional.of(new LinkedList<>(table.getEvent().get()));
            }
            else if (table.getEvent().isPresent() &&
                    table.getEvent().get().get(0).getDataSchema().isPresent()) {
                fieldGroups = Optional.of(new LinkedList<>());

                // case (2) uri containing schema
                List<PravegaStreamFieldGroup> finalFieldGroups = fieldGroups.get();
                for (int i = 0; i < table.getEvent().get().size(); i++) {
                    PravegaStreamFieldGroup event = table.getEvent().get().get(i);
                    String colPrefix = event.getMapping().orElse(
                            table.getEvent().get().size() > 1 ? kvFieldMapping(i) : "");
                    Optional<String> dataSchema = Optional.of(readSchema(event.getDataSchema().get()));
                    PravegaStreamFieldGroup fieldGroup =
                            new PravegaStreamFieldGroup(event.getDataFormat(),
                                    Optional.empty(),
                                    dataSchema,
                                    Optional.of(
                                            mapFieldsFromSchema(colPrefix, event.getDataFormat(), dataSchema.get())));
                    finalFieldGroups.add(fieldGroup);
                }
            }
            else if (kvTable(table)) {
                fieldGroups = fieldGroupsFromSchemaRegistry(schemaTableName);
            }

            if (multiSourceStream(table)) {
                // stream name will be some regex.
                // find all the possible source streams.
                Pattern pattern = Pattern.compile(table.getObjectName());

                List<PravegaTableName> sourceTableNames =
                        listTables(Optional.of(schemaTableName.getSchemaName())).stream()
                                .filter(t -> pattern.matcher(t.getSchemaTableName().getTableName()).matches())
                                .collect(Collectors.toList());

                objectArgs = Optional.of(sourceTableNames.stream()
                        .map(PravegaTableName::getSchemaTableName)
                        .map(SchemaTableName::getTableName)
                        .collect(Collectors.toList()));

                if (!fieldGroups.isPresent()) {
                    // case (3) schema not already defined, look one up
                    // lookup actual schema from any of them - implies all sources are the same
                    PravegaStreamDescription sourceTable = sourceTableNames.isEmpty()
                            ? null
                            : getTable(sourceTableNames.get(0).getSchemaTableName());
                    if (sourceTable == null) {
                        throw new IllegalArgumentException("no stream found for multi source");
                    }
                    fieldGroups = Optional.of(new LinkedList<>());
                    fieldGroups.get().add(new PravegaStreamFieldGroup(
                            sourceTable.getEvent().get().get(0).getDataFormat(),
                            Optional.empty(),
                            sourceTable.getEvent().get().get(0).getDataSchema(),
                            Optional.of(sourceTable.getEvent().get().get(0).getFields())));
                }
            }

            fieldGroups.orElseThrow(() ->
                    new IllegalArgumentException("unable to determine schema for " + schemaTableName));

            // our final table definition.  use schema that we looked up, and set all source stream names here
            table = new PravegaStreamDescription(schemaTableName.getTableName(),
                    Optional.of(schemaTableName.getSchemaName()),
                    table.getObjectName(),
                    Optional.of(table.getObjectType()),
                    objectArgs,
                    fieldGroups);

            tableCache.put(pravegaTableName, Optional.of(table));
            return table;
        }

        Optional<List<PravegaStreamFieldGroup>> fieldGroups = fieldGroupsFromSchemaRegistry(schemaTableName);

        table = new PravegaStreamDescription(
                schemaTableName.getTableName(),
                Optional.of(schemaTableName.getSchemaName()),
                temp_tableNameToStreamName(schemaTableName.getTableName()),
                Optional.of(ObjectType.STREAM),
                Optional.empty() /* args */,
                fieldGroups);
        tableCache.put(pravegaTableName, Optional.of(table));
        return table;
    }

    private Optional<List<PravegaStreamFieldGroup>> fieldGroupsFromSchemaRegistry(final SchemaTableName schemaTableName)
    {
        log.info("look up description of '%s' from pravega", schemaTableName);
        String groupName = groupId(schemaTableName.getSchemaName(), temp_tableNameToStreamName(schemaTableName.getTableName()));

        SchemaRegistryClientConfig registryConfig =
                SchemaRegistryClientConfig.builder()
                        .schemaRegistryUri(pravegaConnectorConfig.getSchemaRegistryURI()).build();
        SchemaRegistryClient registryClient = SchemaRegistryClientFactory.withDefaultNamespace(registryConfig);

        List<PravegaStreamFieldGroup> fieldGroups = new ArrayList<>(2);

        GroupProperties properties =
                registryClient.getGroupProperties(groupName);

        List<SchemaWithVersion> schemas =
                registryClient.getSchemas(groupName);
        if (schemas.size() == 0 || schemas.size() > 2) {
            throw new IllegalStateException(schemaTableName + " has " + schemas.size() + " registered schemas.  expecting either 1 or 2");
        }

        for (int i = 0; i < schemas.size(); i++) {
            SerializationFormat format = schemas.get(i).getSchemaInfo().getSerializationFormat();

            // kv table will have > 1 schema.  key+value likely different types
            // colPrefix used for display so can differentiate between fields from key or value
            boolean kv = schemas.size() > 1;
            String colPrefix = kv ? kvFieldMapping(i) : "";
            fieldGroups.add(new PravegaStreamFieldGroup(
                    dataFormat(properties.getProperties(), format, kv, i),
                    Optional.of(colPrefix),
                    dataSchema(format, schemas.get(i)),
                    Optional.of(mapFieldsFromSchema(colPrefix, format, schemas.get(i)))));
        }

        return Optional.of(fieldGroups);
    }

    private static boolean fieldsDefined(PravegaStreamDescription table)
    {
        // event is optional, fields within event is also optional
        // for kv table - 0 or 2 schemas.  so fine to just check for 1.
        return table.getEvent().isPresent() && (table.getEvent().get().get(0).getFields() != null);
    }

    private List<String> listLocalSchemas()
    {
        return localSchemaStream()
                .map(file -> file.getName().split("\\.")[0])
                .collect(Collectors.toList());
    }

    // scope.stream -> schema.table
    private List<String> listLocalTables(String schema)
    {
        return localSchemaStream()
                .filter(file -> file.getName().endsWith(".json"))
                .filter(file -> file.getName().startsWith(schema))
                .filter(file -> file.getName().split("\\.").length == 3)
                .map(file -> file.getName().split("\\.")[1])
                .collect(Collectors.toList());
    }

    private PravegaStreamDescription getLocalTable(SchemaTableName schemaTableName)
    {
        try {
            File file = new File(pravegaConnectorConfig.getTableDescriptionDir(),
                    String.format("%s.%s.json", schemaTableName.getSchemaName(), schemaTableName.getTableName()));
            if (!file.exists()) {
                return null;
            }
            return streamDescriptionCodec.fromJson(readAllBytes(file.toPath()));
        }
        catch (IOException e) {
            log.error("%s", e);
            throw new UncheckedIOException(e);
        }
        catch (RuntimeException e) {
            log.error("%s", e);
            throw e;
        }
    }

    private java.util.stream.Stream<File> localSchemaStream()
    {
        return listFiles(pravegaConnectorConfig.getTableDescriptionDir()).stream()
                .filter(file -> file.isFile() && file.getName().endsWith(".json"));
    }

    private static List<File> listFiles(File dir)
    {
        if ((dir != null) && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                log.debug("Considering files: %s", asList(files));
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }

    private static String dataFormat(ImmutableMap<String, String> groupProperties,
                                     SerializationFormat format,
                                     boolean kvTable,
                                     int kvIdx)
    {
        /*
            TODO: auto-detect https://github.com/pravega/pravega-sql/issues/58

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

    /**
     * map protobuf java type -> presto sql type
     *
     * @param fieldDescriptor
     * @return
     */
    private static Type typeFromSchema(Descriptors.FieldDescriptor fieldDescriptor)
    {
        switch (fieldDescriptor.getJavaType()) {
            case STRING:
                return createUnboundedVarcharType();

            case INT:
            case LONG:
                return BIGINT;

            case FLOAT:
            case DOUBLE:
                return DOUBLE;

            case BOOLEAN:
                return BOOLEAN;

            case BYTE_STRING:
                return VARBINARY;

            default:
                throw new RuntimeException("unsupported type " + fieldDescriptor);
        }
    }

    /**
     * map json schema type -> presto sql type
     *
     * @param schema
     * @return
     */
    private static Type typeFromSchema(Schema schema)
    {
        if (schema instanceof NumberSchema) {
            return ((NumberSchema) schema).requiresInteger()
                    ? BIGINT
                    : DOUBLE;
        }
        else if (schema instanceof BooleanSchema) {
            return BOOLEAN;
        }
        else if (schema instanceof StringSchema) {
            return createUnboundedVarcharType();
        }
        else {
            throw new RuntimeException("unsupported schema " + schema);
        }
    }

    /**
     * map avro schema type to presto sql type
     *
     * @param schema
     * @return
     */
    private static Type typeFromSchema(org.apache.avro.Schema schema)
    {
        // refer to AvroColumnDecoder#isSupportedType

        switch (schema.getType()) {
            case FIXED:
            case STRING:
                return createUnboundedVarcharType();

            case INT:
            case LONG:
                return BIGINT;

            case FLOAT:
            case DOUBLE:
                return DOUBLE;

            case BOOLEAN:
                return BOOLEAN;

            case BYTES:
                return VARBINARY;

            case MAP:
            case ARRAY:
                // TODO: ^^ handle these https://github.com/pravega/pravega-sql/issues/65

            case RECORD:
            case ENUM:
            case UNION:
            default:
                throw new RuntimeException("unexpected type " + schema);
        }
    }

    /**
     * return lists of common field definitions
     * uses list of fields from provided schema; schema is different depending on serialization format
     *
     * @param format
     * @param schemaWithVersion
     * @return
     */
    private static List<PravegaStreamFieldDescription> mapFieldsFromSchema(
            String namePrefix,
            SerializationFormat format,
            SchemaWithVersion schemaWithVersion)
    {
        switch (format) {
            case Json:
                ObjectSchema objectSchema =
                        (ObjectSchema) JSONSchema.from(schemaWithVersion.getSchemaInfo()).getSchema();
                return mapTable(namePrefix, new JsonSchema(objectSchema));

            case Avro:
            case Custom: // re: Custom - definition for schema itself Custom is always Avro (only custom impl. is csv)
                org.apache.avro.Schema schema =
                        new org.apache.avro.Schema.Parser().parse(
                                new String(schemaWithVersion.getSchemaInfo().getSchemaData().array(), StandardCharsets.UTF_8));
                return mapTable(namePrefix, new AvroSchema(schema, format == SerializationFormat.Custom));

            case Protobuf:
                return mapTable(namePrefix, new ProtobufSchema(ProtobufCommon.descriptorFor(schemaWithVersion)));

            default:
                throw new IllegalArgumentException("unexpected format " + format);
        }
    }

    private static List<PravegaStreamFieldDescription> mapFieldsFromSchema(String namePrefix, String format, String schemaString)
    {
        // schemaString defined as human-readable string in local file.  only avro supported now.
        switch (format) {
            case AVRO:
                org.apache.avro.Schema schema =
                        new org.apache.avro.Schema.Parser().parse(schemaString);
                return mapTable(namePrefix, new AvroSchema(schema, false));

            default:
                throw new UnsupportedOperationException("unexpected format " + format);
        }
    }

    private static class SchemaColumn
    {
        String name;
        String mapping;
        Type type;

        SchemaColumn(String name, String mapping, Type type)
        {
            this.name = name;
            this.mapping = mapping;
            this.type = type;
        }
    }

    static class SchemaWrapper
    {
        List<SchemaField> fields = new ArrayList<>();
    }

    static class SchemaField
    {
        String name;
        Type type;
        boolean record;
        SchemaWrapper schema;
        int ordinalPosition;

        SchemaField(String name, Type type, boolean record, SchemaWrapper schema)
        {
            this(name, type, record, schema, -1);
        }

        SchemaField(String name, Type type, boolean record, SchemaWrapper schema, int ordinalPosition)
        {
            this.name = name;
            this.type = type;
            this.record = record;
            this.schema = schema;
            this.ordinalPosition = ordinalPosition;
        }
    }

    static class JsonSchema
            extends SchemaWrapper
    {
        JsonSchema(ObjectSchema schema)
        {
            schema.getPropertySchemas().forEach((key, value) -> {
                boolean record = value instanceof ObjectSchema;
                fields.add(new SchemaField(key,
                        record ? null : typeFromSchema(value),
                        record,
                        record ? new JsonSchema((ObjectSchema) value) : null));
            });
        }
    }

    static class ProtobufSchema
            extends SchemaWrapper
    {
        ProtobufSchema(Descriptors.Descriptor schema)
        {
            schema.getFields().forEach(f -> {
                boolean record = f.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE;
                fields.add(new SchemaField(f.getJsonName(),
                        record ? null : typeFromSchema(f),
                        record,
                        record ? new ProtobufSchema(f.getMessageType()) : null));
            });
        }
    }

    static class AvroSchema
            extends SchemaWrapper
    {
        AvroSchema(org.apache.avro.Schema schema, boolean customCsv)
        {
            final AtomicInteger position = new AtomicInteger();
            schema.getFields().forEach(f -> {
                boolean record = f.schema().getType() == RECORD;
                fields.add(new SchemaField(f.name(),
                        record ? null : typeFromSchema(f.schema()),
                        record,
                        record ? new AvroSchema(f.schema(), customCsv) : null,
                        customCsv ? position.getAndIncrement() : -1));
            });
        }
    }

    private static List<PravegaStreamFieldDescription> mapTable(String namePrefix, SchemaWrapper schema)
    {
        return mapFieldsFromSchema(mapColumns(namePrefix, null /* mappingPrefix */, schema));
    }

    private static List<SchemaColumn> mapColumns(String namePrefix, String mappingPrefix, SchemaWrapper schema)
    {
        List<SchemaColumn> columnList = new ArrayList<>();
        schema.fields.forEach(field -> {
            String name = nestedPrefixFor(namePrefix, field.name);
            // for csv we use only position.  for avro, json, etc, can be path into nested object
            String mapping = field.ordinalPosition >= 0
                    ? String.valueOf(field.ordinalPosition)
                    : nestedPrefixFor(mappingPrefix, field.name);
            if (field.record) {
                columnList.addAll(mapColumns(name, mapping, field.schema));
            }
            else {
                columnList.add(new SchemaColumn(name, mapping, field.type));
            }
        });
        return columnList;
    }

    private static String nestedPrefixFor(String prefix, String name)
    {
        // (record1, field1) -> record1/field1
        return prefix == null || prefix.isEmpty()
                ? name
                : prefix + NESTED_RECORD_SEPARATOR + name;
    }

    /**
     * create field description from list of name,mapping,type tuples.  each pair is a field in the schema.
     * @param schemaColumns
     * @return
     */
    static List<PravegaStreamFieldDescription> mapFieldsFromSchema(List<SchemaColumn> schemaColumns)
    {
        List<PravegaStreamFieldDescription> fields = new ArrayList<>();
        schemaColumns.forEach(sc -> {
            fields.add(new PravegaStreamFieldDescription(sc.name,
                    sc.type,
                    sc.mapping,
                    "",
                    null,
                    null,
                    false));
        });
        return fields;
    }

    private static Optional<String> dataSchema(SerializationFormat format, SchemaWithVersion schemaWithVersion)
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

    private static boolean internalStream(Stream stream)
    {
        return internalObject(stream.getStreamName());
    }

    private static boolean internalObject(String object)
    {
        return object.startsWith("_") /* pravega internal */ ||
                object.endsWith("-SC") /* application internal - stream cuts */;
    }
}
