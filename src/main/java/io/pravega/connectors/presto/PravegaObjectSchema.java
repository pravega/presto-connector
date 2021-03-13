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

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class PravegaObjectSchema
{
    private final String format;

    private final Optional<String> schemaLocation;

    @JsonCreator
    public PravegaObjectSchema(@JsonProperty("format") String format,
                               @JsonProperty("schemaLocation") Optional<String> schemaLocation)
    {
        this.format = requireNonNull(format, "format is null");
        this.schemaLocation = requireNonNull(schemaLocation, "schemaLocation is null");
    }

    @JsonProperty
    public String getFormat()
    {
        return format;
    }

    @JsonProperty
    public Optional<String> getSchemaLocation()
    {
        return schemaLocation;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("format", format)
                .add("schemaLocation", schemaLocation)
                .toString();
    }
}
