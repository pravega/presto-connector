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

import io.airlift.slice.Slice;

public interface PravegaRecordValue
{
    void decode();

    boolean decoded();

    boolean isNull(int field, int ordinalPosition);

    long getLong(int field, int ordinalPosition);

    double getDouble(int field, int ordinalPosition);

    boolean getBoolean(int field, int ordinalPosition);

    Slice getSlice(int field, int ordinalPosition);
}
