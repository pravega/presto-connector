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

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.LinkedList;
import java.util.List;

public class PravegaProperties
{
    private static final String SESSION_CURSOR_DELIM_CHAR = "cursor_delim_char";

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

        return propertyMetadataList;
    }

    public String getCursorDelimChar()
    {
        return session.getProperty(SESSION_CURSOR_DELIM_CHAR, String.class);
    }
}
