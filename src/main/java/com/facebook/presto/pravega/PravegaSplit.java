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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Represents a Pravega specific {@link ConnectorSplit}. Each split is
 * maps to a SegmentRange, which is a Pravega stream segment with a
 * begin and end offsets.
 */
public class PravegaSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final ObjectType objectType;
    private final List<PravegaObjectSchema> schema;
    private final ReaderType readerType;
    private final byte[] readerArgs;
    private final String schemaRegistryGroupId;

    @JsonCreator
    public PravegaSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("objectType") ObjectType objectType,
            @JsonProperty("schema") List<PravegaObjectSchema> schema,
            @JsonProperty("readerType") ReaderType readerType,
            @JsonProperty("readerArgs") byte[] readerArgs,
            @JsonProperty("schemaRegistryGroupId") String schemaRegistryGroupId)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.objectType = requireNonNull(objectType, "objectType is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.readerType = requireNonNull(readerType, "readerType is null");
        this.readerArgs = requireNonNull(readerArgs, "readerArgs is null");
        this.schemaRegistryGroupId = requireNonNull(schemaRegistryGroupId, "schemaRegistryGroupId is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public ObjectType getObjectType()
    {
        return objectType;
    }

    @JsonProperty
    public ReaderType getReaderType()
    {
        return readerType;
    }

    @JsonProperty
    public byte[] getReaderArgs()
    {
        return readerArgs;
    }

    @JsonProperty
    public List<PravegaObjectSchema> getSchema()
    {
        return schema;
    }

    @Override
    public NodeSelectionStrategy getNodeSelectionStrategy()
    {
        return NO_PREFERENCE;
    }

    @Override
    public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
    {
        return ImmutableList.of();
    }

    @JsonProperty
    public String getschemaRegistryGroupId()
    {
        return schemaRegistryGroupId;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("objectType", objectType)
                .add("schema", schema)
                .add("readerType", readerType)
                .add("readerArgs", readerArgs)
                .toString();
    }
}
