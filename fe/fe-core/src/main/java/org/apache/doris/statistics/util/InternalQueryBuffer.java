// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.statistics.util;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Parse the MySQL protocol result data returned by BE,
 * only simple parsing operations are performed here (parsed as String).
 * For more, @see `be/src/runtime/mysql_result_writer.cpp`.
 */
public class InternalQueryBuffer {
    private static final long NULL_LENGTH = -1;
    private static final byte[] EMPTY_BYTES = new byte[0];

    private final ByteBuffer buffer;

    public InternalQueryBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public byte[] data() {
        return buffer.array();
    }

    public int length() {
        return buffer.capacity();
    }

    public int position() {
        return buffer.position();
    }

    public void clear() {
        buffer.clear();
    }

    private byte read() {
        return buffer.get();
    }

    private int readUB2() {
        int i = read() & 0xff;
        i |= (read() & 0xff) << 8;
        return i;
    }

    private int readUB3() {
        int i = read() & 0xff;
        i |= (read() & 0xff) << 8;
        i |= (read() & 0xff) << 16;
        return i;
    }

    private int readUB4() {
        int i = read() & 0xff;
        i |= (read() & 0xff) << 8;
        i |= (read() & 0xff) << 16;
        i |= (read() & 0xff) << 24;
        return i;
    }

    private long readLong() {
        long i = read() & 0xff;
        i |= (long) (read() & 0xff) << 8;
        i |= (long) (read() & 0xff) << 16;
        i |= (long) (read() & 0xff) << 24;
        i |= (long) (read() & 0xff) << 32;
        i |= (long) (read() & 0xff) << 40;
        i |= (long) (read() & 0xff) << 48;
        i |= (long) (read() & 0xff) << 56;
        return i;
    }

    /**
     * The length of the data is not fixed, the length value is determined by the 1-9 bytes
     * before the data, and the number of bytes occupied by the length value is not fixed,
     * and the number of bytes is determined by the first byte. (@see `be/src/runtime/mysql_row_buffer.cpp`)
     *
     * @return Length coded binary
     */
    private long readLength() {
        int length = read() & 0xff;
        switch (length) {
            case 251:
                return NULL_LENGTH;
            case 252:
                return readUB2();
            case 253:
                return readUB3();
            case 254:
                return readLong();
            default:
                return length;
        }
    }

    public byte[] readBytesWithLength() {
        int length = (int) readLength();
        if (length == NULL_LENGTH) {
            return null;
        }
        if (length <= 0) {
            return EMPTY_BYTES;
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }

    public String readStringWithLength() {
        byte[] bytes = readBytesWithLength();
        if (bytes != null) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return null;
    }

    public String readStringWithLength(String charset)
            throws UnsupportedEncodingException {
        byte[] bytes = readBytesWithLength();
        if (bytes != null) {
            return new String(bytes, charset);
        }
        return null;
    }

    public Integer readInt() {
        String src = readStringWithLength();
        return src == null ? null : new Integer(src);
    }

    public Float readFloat() {
        String src = readStringWithLength();
        return src == null ? null : new Float(src);
    }

    public Double readDouble() {
        String src = readStringWithLength();
        return src == null ? null : new Double(src);
    }
}
