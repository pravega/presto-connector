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

package com.facebook.presto.pravega;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnMetadata;

import java.util.Map;

import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

/**
 * Describes an internal (managed by the connector) field which is added to each table row. The definition itself makes the row
 * show up in the tables (the columns are hidden by default, so they must be explicitly selected) but unless the field is hooked in using the
 * forBooleanValue/forLongValue/forBytesValue methods and the resulting FieldValueProvider is then passed into the appropriate row decoder, the fields
 * will be null. Most values are assigned in the {@link com.facebook.presto.pravega.PravegaRecordSet}.
 */
public enum PravegaInternalFieldDescription
{
    /**
     * <tt>_segment_id</tt> - Pravega segment id.
     */
    SEGMENT_ID_FIELD("_segment_id", BigintType.BIGINT, "Segment Id"),

    /**
     * <tt>_segment_offset</tt> - The current offset of the event in the segment.
     */
    SEGMENT_OFFSET_FIELD("_segment_offset", BigintType.BIGINT, "Offset for the event within the segment"),

    /**
     * <tt>_event_corrupt</tt> - True if the row converter could not read the event. May be null if the row
     * converter does not set a value (e.g. the dummy row converter does not).
     */
    EVENT_CORRUPT_FIELD("_event_corrupt", BooleanType.BOOLEAN, "Event data is corrupt"),

    /**
     * <tt>_event</tt> - Represents the full stream as a text column. Format is UTF-8 which may be wrong
     * for some stream. TODO: make charset configurable.
     */
    EVENT_FIELD("_message", createUnboundedVarcharType(), "Event text"),

    /**
     * <tt>_event_length</tt> - length in bytes of the mevent.
     */
    EVENT_LENGTH_FIELD("_event_length", BigintType.BIGINT, "Total number of event bytes");

    private static final Map<String, PravegaInternalFieldDescription> BY_COLUMN_NAME =
            stream(PravegaInternalFieldDescription.values())
                    .collect(toImmutableMap(PravegaInternalFieldDescription::getColumnName, identity()));

    public static PravegaInternalFieldDescription forColumnName(String columnName)
    {
        PravegaInternalFieldDescription description = BY_COLUMN_NAME.get(columnName);
        checkArgument(description != null, "Unknown internal column name %s", columnName);
        return description;
    }

    private final String columnName;
    private final Type type;
    private final String comment;

    PravegaInternalFieldDescription(
            String columnName,
            Type type,
            String comment)
    {
        checkArgument(!isNullOrEmpty(columnName), "name is null or is empty");
        this.columnName = columnName;
        this.type = requireNonNull(type, "type is null");
        this.comment = requireNonNull(comment, "comment is null");
    }

    public String getColumnName()
    {
        return columnName;
    }

    public Type getType()
    {
        return type;
    }

    PravegaColumnHandle getColumnHandle(String connectorId, int index, boolean hidden)
    {
        return new PravegaColumnHandle(connectorId,
                index,
                getColumnName(),
                getType(),
                null,
                null,
                null,
                false,
                hidden,
                true,
                1);
    }

    ColumnMetadata getColumnMetadata(boolean hidden)
    {
        return new ColumnMetadata(columnName, type, comment, hidden);
    }
}
