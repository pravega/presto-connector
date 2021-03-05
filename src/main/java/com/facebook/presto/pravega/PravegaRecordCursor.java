/*
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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.pravega.decoder.DecodableEvent;
import com.facebook.presto.pravega.decoder.EventDecoder;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.RecordCursor;
import io.airlift.slice.Slice;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.facebook.presto.pravega.util.PravegaSchemaUtils.CSV;
import static com.google.common.base.Preconditions.checkArgument;

public class PravegaRecordCursor
        implements RecordCursor
{
    private static final Logger log = Logger.get(PravegaRecordCursor.class);
    private long totalBytes;
    private final Iterator<DecodableEvent> iterator;
    private final List<PravegaColumnHandle> columnHandles;
    private final EventDecoder eventDecoder;

    private static final AtomicInteger CURSOR_ID = new AtomicInteger();
    private final int cursorId;

    private final int[] fieldToColumnIndex;

    private final PravegaRecordValue record;

    PravegaRecordCursor(Iterator<DecodableEvent> iterator,
                        List<PravegaColumnHandle> columnHandles,
                        EventDecoder eventDecoder,
                        PravegaProperties properties,
                        String format)
    {
        this.iterator = iterator;
        this.columnHandles = columnHandles;
        this.eventDecoder = eventDecoder;
        this.record = createRecordValue(format, properties, columnHandles);
        this.cursorId = CURSOR_ID.getAndIncrement();
        log.debug("open record cursor " + cursorId);

        this.fieldToColumnIndex = new int[columnHandles.size()];

        for (int i = 0; i < columnHandles.size(); i++) {
            PravegaColumnHandle columnHandle = columnHandles.get(i);
            this.fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        return iterator.hasNext() && nextRow(iterator.next());
    }

    private boolean nextRow(DecodableEvent event)
    {
        totalBytes += event.totalSize();

        return eventDecoder.decodeEvent(event, record);
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkField(field, boolean.class);
        return record.getBoolean(field, fieldToColumnIndex[field]);
    }

    @Override
    public long getLong(int field)
    {
        checkField(field, long.class);
        return record.getLong(field, fieldToColumnIndex[field]);
    }

    @Override
    public double getDouble(int field)
    {
        checkField(field, double.class);
        return record.getDouble(field, fieldToColumnIndex[field]);
    }

    @Override
    public Slice getSlice(int field)
    {
        checkField(field, Slice.class);
        return record.getSlice(field, fieldToColumnIndex[field]);
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        if (!record.decoded()) {
            record.decode();
        }
        return record.isNull(field, fieldToColumnIndex[field]);
    }

    private void checkField(int field, Class<?> expected)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        Class<?> actual = getType(field).getJavaType();
        checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
        log.debug("cursorId " + cursorId + " closed");
    }

    // set decoded event into this object.  access values for given fields.
    static PravegaRecordValue createRecordValue(String format,
                                                PravegaProperties properties,
                                                List<PravegaColumnHandle> columnHandles)
    {
        if (format.equals(CSV)) {
            return new DelimRecordValue(properties.getCursorDelimChar().charAt(0));
        }
        else {
            return new TypedRecordValue(columnHandles.stream().map(
                    PravegaRecordCursor::convertColumnHandle).collect(Collectors.toList()));
        }
    }

    static ColumnHandle convertColumnHandle(PravegaColumnHandle columnHandle)
    {
        return columnHandle;
    }
}
