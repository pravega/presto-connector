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
package com.facebook.presto.pravega.util;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.pravega.PravegaStreamFieldDescription;
import com.facebook.presto.pravega.ProtobufCommon;
import com.google.protobuf.Descriptors;
import io.pravega.schemaregistry.contract.data.SchemaWithVersion;
import io.pravega.schemaregistry.contract.data.SerializationFormat;
import io.pravega.schemaregistry.serializer.json.schemas.JSONSchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.pravega.util.PravegaSchemaUtils.AVRO;
import static com.facebook.presto.pravega.util.PravegaSchemaUtils.NESTED_RECORD_SEPARATOR;
import static org.apache.avro.Schema.Type.RECORD;

/**
 * a collection of methods to help create PravegaStreamDescription's
 * including mapping
 */
public class PravegaStreamDescUtils
{
    private PravegaStreamDescUtils() {}

    // SchemaColumn, SchemaField, SchemaWrapper,
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
    public static List<PravegaStreamFieldDescription> mapFieldsFromSchema(
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

    public static List<PravegaStreamFieldDescription> mapFieldsFromSchema(String namePrefix, String format, String schemaString)
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

    /**
     * create field description from list of name,mapping,type tuples.  each pair is a field in the schema.
     *
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

    /**
     * simply appends 'name' to an existing prefix (if any) using separator
     *
     * @param prefix starting prefix
     * @param name name of the column to prefix
     * @return prefix
     */
    private static String nestedPrefixFor(String prefix, String name)
    {
        // (record1, field1) -> record1/field1
        return prefix == null || prefix.isEmpty()
                ? name
                : prefix + NESTED_RECORD_SEPARATOR + name;
    }
}
