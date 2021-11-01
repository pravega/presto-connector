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

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

/**
 * Json description to parse a row on a Pravega stream.
 */
public class PravegaStreamDescription
{
    private final String tableName;
    private final Optional<String> schemaName;
    private final String objectName;
    private final ObjectType objectType;
    private final Optional<List<String>> objectArgs;
    private final Optional<List<PravegaStreamFieldGroup>> event;

    @JsonCreator
    public PravegaStreamDescription(
            @JsonProperty("tableName") String tableName,
            @JsonProperty("schemaName") Optional<String> schemaName,
            @JsonProperty("objectName") String objectName,
            @JsonProperty("objectType") Optional<ObjectType> objectType,
            @JsonProperty("objectArgs") Optional<List<String>> objectArgs,
            @JsonProperty("event") Optional<List<PravegaStreamFieldGroup>> event)
    {
        checkArgument(!isNullOrEmpty(tableName), "tableName is null or is empty");
        this.tableName = tableName;
        this.objectName = objectName;
        this.objectType = objectType.orElse(ObjectType.STREAM);
        this.objectArgs = requireNonNull(objectArgs, "objectArgs is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.event = requireNonNull(event, "message is null");
    }

    public PravegaStreamDescription(PravegaStreamDescription streamDescription, List<PravegaStreamFieldGroup> event)
    {
        this.tableName = streamDescription.tableName;
        this.schemaName = streamDescription.schemaName;
        this.objectName = streamDescription.objectName;
        this.objectType = streamDescription.objectType;
        this.objectArgs = streamDescription.objectArgs;
        this.event = Optional.of(event);
    }


    public PravegaStreamDescription(PravegaStreamDescription streamDescription, List<PravegaStreamFieldGroup> event, List<String> objectArgs)
    {
        this.tableName = streamDescription.tableName;
        this.schemaName = streamDescription.schemaName;
        this.objectName = streamDescription.objectName;
        this.objectType = streamDescription.objectType;
        this.objectArgs = Optional.of(objectArgs);
        this.event = Optional.of(event);
    }

    @JsonProperty
    public Optional<String> getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String getObjectName()
    {
        return objectName;
    }

    @JsonProperty
    public ObjectType getObjectType()
    {
        return objectType;
    }

    @JsonProperty
    public Optional<List<String>> getObjectArgs()
    {
        return objectArgs;
    }

    @JsonProperty
    public Optional<List<PravegaStreamFieldGroup>> getEvent()
    {
        return event;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .add("objectName", objectName)
                .add("objectType", objectType)
                .add("objectArgs", objectArgs)
                .add("event", event)
                .toString();
    }
}
