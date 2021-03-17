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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Groups the field descriptions for message or key.
 */
public class PravegaStreamFieldGroup
{
    private final String dataFormat;
    private final Optional<String> dataSchema;
    private final Optional<List<PravegaStreamFieldDescription>> fields;
    private final Optional<String> mapping;

    @JsonCreator
    public PravegaStreamFieldGroup(
            @JsonProperty("dataFormat") String dataFormat,
            @JsonProperty("mapping") Optional<String> mapping,
            @JsonProperty("dataSchema") Optional<String> dataSchema,
            @JsonProperty("fields") Optional<List<PravegaStreamFieldDescription>> fields)
    {
        this.dataFormat = requireNonNull(dataFormat, "dataFormat is null");
        this.mapping = mapping;
        this.dataSchema = requireNonNull(dataSchema, "dataSchema is null");
        this.fields = fields;
    }

    @JsonProperty
    public String getDataFormat()
    {
        return dataFormat;
    }

    @JsonProperty
    public Optional<String> getMapping()
    {
        return mapping;
    }

    @JsonProperty
    public List<PravegaStreamFieldDescription> getFields()
    {
        return fields.orElse(null);
    }

    @JsonProperty
    public Optional<String> getDataSchema()
    {
        return dataSchema;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("dataFormat", dataFormat)
                .add("mapping", mapping)
                .add("dataSchema", dataSchema)
                .add("fields", fields)
                .toString();
    }
}
