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

import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.nio.ByteBuffer;

// csv, tsv, etc
public class DelimRecordValue
        implements PravegaRecordValue
{
    int positions;
    int[] position;

    char fieldSep;

    boolean decoded;

    private ByteBuffer buf;

    private int len;
    private int offset;

    public DelimRecordValue(char fieldSep)
    {
        this.fieldSep = fieldSep;
        this.position = new int[1024];
    }

    public void setBuf(ByteBuffer buf)
    {
        this.buf = buf;
        this.len = buf.limit();
        this.offset = buf.arrayOffset();
        this.decoded = false;
    }

    public void decode()
    {
        position[0] = 0;
        positions = 1;

        int pos = 0;
        while (pos < len) {
            if (buf.get(pos++) == fieldSep) {
                position[positions++] = pos;
            }
        }

        decoded = true;
    }

    public boolean decoded()
    {
        return decoded;
    }

    private int fieldLen(int ordinalPosition)
    {
        return ordinalPosition + 1 == positions
                ? len - position[ordinalPosition]
                : position[ordinalPosition + 1] - position[ordinalPosition] - 1;
    }

    private String toString(int ordinalPosition)
    {
        return new String(buf.array(), offset + position[ordinalPosition], fieldLen(ordinalPosition));
    }

    public boolean isNull(int unused, int ordinalPosition)
    {
        if (!decoded) {
            decode();
        }
        return ordinalPosition >= positions || fieldLen(ordinalPosition) == 0;
    }

    public long getLong(int unused, int ordinalPosition)
    {
        Preconditions.checkState(decoded);
        return Long.parseLong(toString(ordinalPosition));
    }

    public double getDouble(int unused, int ordinalPosition)
    {
        Preconditions.checkState(decoded);
        return Double.parseDouble(toString(ordinalPosition));
    }

    public boolean getBoolean(int unused, int ordinalPosition)
    {
        Preconditions.checkState(decoded);
        return Boolean.getBoolean(toString(ordinalPosition));
    }

    public Slice getSlice(int unused, int ordinalPosition)
    {
        Preconditions.checkState(decoded);
        return Slices.wrappedBuffer(buf.array(), offset + position[ordinalPosition], fieldLen(ordinalPosition));
    }
}
