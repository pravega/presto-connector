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

package io.trino.plugin.pravega;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Pravega specific {@link ConnectorTableHandle}.
 */
public final class PravegaTableHandle
        implements ConnectorTableHandle
{
    /**
     * connector id
     */
    private final String connectorId;

    /**
     * The schema name for this table. Is set through configuration and read
     */
    private final String schemaName;

    private final ObjectType objectType;

    /**
     * The table name used by presto.
     */
    private final String tableName;

    /**
     * The stream or kv table name that is read from Pravega.
     */
    private final String objectName;

    private final Optional<List<String>> objectArgs;

    private final List<PravegaObjectSchema> schema;

    private final String schemaRegistryGroupId;

    @JsonCreator
    public PravegaTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("objectName") String objectName,
            @JsonProperty("objectType") ObjectType objectType,
            @JsonProperty("objectArgs") Optional<List<String>> objectArgs,
            @JsonProperty("schema") List<PravegaObjectSchema> schema,
            @JsonProperty("schemaRegistryGroupId") String schemaRegistryGroupId)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.objectName = requireNonNull(objectName, "objectName is null");
        this.objectType = requireNonNull(objectType, "objectType is null");
        this.objectArgs = requireNonNull(objectArgs, "objectArgs is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.schemaRegistryGroupId = requireNonNull(schemaRegistryGroupId, "schemaRegistryGroupId is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public ObjectType getObjectType()
    {
        return objectType;
    }

    @JsonProperty
    public String getObjectName()
    {
        return objectName;
    }

    @JsonProperty
    public Optional<List<String>> getOjectArgs()
    {
        return objectArgs;
    }

    @JsonProperty
    public List<PravegaObjectSchema> getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getSchemaRegistryGroupId()
    {
        return schemaRegistryGroupId;
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, schemaName, tableName, objectName, objectType, schema, schemaRegistryGroupId);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        PravegaTableHandle other = (PravegaTableHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId)
                && Objects.equals(this.schemaName, other.schemaName)
                && Objects.equals(this.tableName, other.tableName)
                && Objects.equals(this.objectName, other.objectName)
                && Objects.equals(this.objectType, other.objectType)
                && Objects.equals(this.schema, other.schema)
                && Objects.equals(this.schemaRegistryGroupId, other.schemaRegistryGroupId);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .add("objectName", objectName)
                .add("objectType", objectType)
                .add("schema", schema)
                .toString();
    }
}
