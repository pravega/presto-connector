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
 * limitations under the License.Copyright (c) Pravega Authors.
 */

package com.facebook.presto.pravega;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

// avro, protobuf, json
public class TypedRecordValue
        implements PravegaRecordValue
{
    private final List<ColumnHandle> columnHandles;

    private final FieldValueProvider[] currentRowValues;

    private final Map<ColumnHandle, FieldValueProvider> currentRowValuesMap = new HashMap<>();

    private Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedValue;

    private boolean decoded;

    public TypedRecordValue(List<ColumnHandle> columnHandles)
    {
        this.columnHandles = columnHandles;
        this.currentRowValues = new FieldValueProvider[columnHandles.size()];
    }

    public void setDecodedValue(Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodedValue)
    {
        this.decodedValue = decodedValue;
        this.decoded = false;
    }

    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> getDecodedValue()
    {
        return decodedValue;
    }

    public void decode()
    {
        currentRowValuesMap.clear();
        decodedValue.ifPresent(currentRowValuesMap::putAll);

        for (int i = 0; i < columnHandles.size(); i++) {
            ColumnHandle columnHandle = columnHandles.get(i);
            currentRowValues[i] = currentRowValuesMap.get(columnHandle);
        }

        decoded = true;
    }

    public boolean decoded()
    {
        return decoded;
    }

    public boolean isNull(int field, int ordinalPosition)
    {
        return currentRowValues[field] == null || currentRowValues[field].isNull();
    }

    public long getLong(int field, int ordinalPosition)
    {
        Preconditions.checkState(decoded);
        return currentRowValues[field].getLong();
    }

    public double getDouble(int field, int ordinalPosition)
    {
        Preconditions.checkState(decoded);
        return currentRowValues[field].getDouble();
    }

    public boolean getBoolean(int field, int ordinalPosition)
    {
        Preconditions.checkState(decoded);
        return currentRowValues[field].getBoolean();
    }

    public Slice getSlice(int field, int ordinalPosition)
    {
        Preconditions.checkState(decoded);
        return currentRowValues[field].getSlice();
    }
}
