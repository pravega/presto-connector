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

import io.trino.spi.connector.SchemaTableName;

import java.util.Objects;

public class PravegaTableName
{
    private final SchemaTableName schemaTableName;
    private final boolean hidden;

    public PravegaTableName(String schema, String table)
    {
        this(new SchemaTableName(schema, table), false);
    }

    // ADR: string 'stream name' for mapping table name to stream.  can be ==
    public PravegaTableName(String schema, String table, boolean hidden)
    {
        this(new SchemaTableName(schema, table), hidden);
    }

    public PravegaTableName(SchemaTableName schemaTableName)
    {
        this(schemaTableName, false);
    }

    public PravegaTableName(SchemaTableName schemaTableName, boolean hidden)
    {
        this.schemaTableName = schemaTableName;
        this.hidden = hidden;
    }

    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    public boolean getHidden()
    {
        return hidden;
    }

    @Override
    public int hashCode()
    {
        return schemaTableName.hashCode();
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
        final PravegaTableName other = (PravegaTableName) obj;
        return Objects.equals(this.schemaTableName, other.schemaTableName);
    }

    @Override
    public String toString()
    {
        return schemaTableName.toString() + "(" + hidden + ")";
    }
}
