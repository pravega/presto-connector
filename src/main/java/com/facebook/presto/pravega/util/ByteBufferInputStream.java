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

package com.facebook.presto.pravega.util;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream
        extends InputStream
{
    private final ByteBuffer buffer;

    public ByteBufferInputStream(ByteBuffer buffer)
    {
        this.buffer = buffer;
    }

    @Override
    public int read()
    {
        return buffer.hasRemaining() ? buffer.get() & 0xff : -1;
    }

    @Override
    public int read(byte[] bytes, int offset, int length)
    {
        if (!buffer.hasRemaining()) {
            return -1;
        }

        length = Math.min(length, buffer.remaining());
        buffer.get(bytes, offset, length);
        return length;
    }
}
