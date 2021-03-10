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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

import static java.util.Objects.requireNonNull;

public class PravegaKVTable
        implements Serializable
{
    private final String scope;

    private final String table;

    private final String keyFamily;

    @JsonCreator
    public PravegaKVTable(@JsonProperty("scope") String scope,
                          @JsonProperty("table") String table,
                          @JsonProperty("keyFamily") String keyFamily)
    {
        this.scope = requireNonNull(scope, "scope is null");
        this.table = requireNonNull(table, "table is null");
        this.keyFamily = requireNonNull(keyFamily, "keyFamily is null");
    }

    @JsonProperty
    public String getScope()
    {
        return scope;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonProperty
    public String getKeyFamily()
    {
        return keyFamily;
    }
}
