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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;

import java.util.LinkedList;
import java.util.List;

public class PravegaProperties
{
    private static final String SESSION_READER_TYPE = "reader_type";

    private static final String SESSION_CURSOR_DELIM_CHAR = "cursor_delim_char";

    private static final String SESSION_GROUPED_EVENT_SPLITS = "grouped_event_splits";

    private static final String SESSION_EVENT_READ_TIMEOUT_MS = "event_read_timeout_ms";

    private final ConnectorSession session;

    public PravegaProperties(final ConnectorSession session)
    {
        this.session = session;
    }

    public static List<PropertyMetadata<?>> buildSessionProperties()
    {
        List<PropertyMetadata<?>> propertyMetadataList = new LinkedList<>();

        propertyMetadataList.add(
                PropertyMetadata.stringProperty(
                        SESSION_CURSOR_DELIM_CHAR,
                        "character used as field separator for delimited formats",
                        ",",
                        false));

        propertyMetadataList.add(
                PropertyMetadata.stringProperty(
                        SESSION_READER_TYPE,
                        "reader type [event|grouped_event|segment_range|segment_range_per_split]",
                        "segment_range_per_split",
                        false));

        propertyMetadataList.add(
                PropertyMetadata.integerProperty(
                        SESSION_GROUPED_EVENT_SPLITS,
                        "number of splits when using grouped readers",
                        63,
                        false));

        propertyMetadataList.add(
                PropertyMetadata.integerProperty(
                        SESSION_EVENT_READ_TIMEOUT_MS,
                        "timeout in ms to readNextEvent()",
                        10000,
                        false));

        return propertyMetadataList;
    }

    public String getCursorDelimChar()
    {
        return session.getProperty(SESSION_CURSOR_DELIM_CHAR, String.class);
    }

    public String getReaderType()
    {
        return session.getProperty(SESSION_READER_TYPE, String.class);
    }

    public int getGroupedEventSplits()
    {
        return session.getProperty(SESSION_GROUPED_EVENT_SPLITS, Integer.class);
    }

    public int getEventReadTimeoutMs()
    {
        return session.getProperty(SESSION_EVENT_READ_TIMEOUT_MS, Integer.class);
    }
}
