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

package org.apache.doris.mysql;

import java.nio.ByteBuffer;

/**
 * MySQL protocol utility functions.
 *
 * <p>This class provides low-level byte reading functions for parsing
 * MySQL protocol packets. These are the building blocks for reading
 * integers, strings, and other data types from the wire protocol.
 *
 * <p>Reference: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_dt_integers.html
 */
public final class MysqlProto {

    private MysqlProto() {
        // Utility class
    }

    /**
     * Reads a single byte from the buffer.
     *
     * @param buffer byte buffer
     * @return byte value
     */
    public static byte readByte(ByteBuffer buffer) {
        return buffer.get();
    }

    /**
     * Reads a byte at a specific index without advancing position.
     *
     * @param buffer byte buffer
     * @param index  index to read from
     * @return byte value
     */
    public static byte readByteAt(ByteBuffer buffer, int index) {
        return buffer.get(index);
    }

    /**
     * Reads a 1-byte unsigned integer (int<1>).
     *
     * @param buffer byte buffer
     * @return integer value (0-255)
     */
    public static int readInt1(ByteBuffer buffer) {
        return readByte(buffer) & 0XFF;
    }

    /**
     * Reads a 2-byte little-endian unsigned integer (int<2>).
     *
     * @param buffer byte buffer
     * @return integer value
     */
    public static int readInt2(ByteBuffer buffer) {
        return (readByte(buffer) & 0xFF) | ((readByte(buffer) & 0xFF) << 8);
    }

    /**
     * Reads a 3-byte little-endian unsigned integer (int<3>).
     *
     * @param buffer byte buffer
     * @return integer value
     */
    public static int readInt3(ByteBuffer buffer) {
        return (readByte(buffer) & 0xFF)
            | ((readByte(buffer) & 0xFF) << 8)
            | ((readByte(buffer) & 0xFF) << 16);
    }

    /**
     * Reads the lowest 4 bytes from buffer start without advancing position.
     *
     * @param buffer byte buffer
     * @return integer value
     */
    public static int readLowestInt4(ByteBuffer buffer) {
        return (readByteAt(buffer, 0) & 0xFF)
            | ((readByteAt(buffer, 1) & 0xFF) << 8)
            | ((readByteAt(buffer, 2) & 0xFF) << 16)
            | ((readByteAt(buffer, 3) & 0XFF) << 24);
    }

    /**
     * Reads a 4-byte little-endian integer (int<4>).
     *
     * @param buffer byte buffer
     * @return integer value
     */
    public static int readInt4(ByteBuffer buffer) {
        return (readByte(buffer) & 0xFF)
            | ((readByte(buffer) & 0xFF) << 8)
            | ((readByte(buffer) & 0xFF) << 16)
            | ((readByte(buffer) & 0XFF) << 24);
    }

    /**
     * Reads a 6-byte little-endian integer (int<6>).
     *
     * @param buffer byte buffer
     * @return long value
     */
    public static long readInt6(ByteBuffer buffer) {
        return (readInt4(buffer) & 0XFFFFFFFFL) | (((long) readInt2(buffer)) << 32);
    }

    /**
     * Reads an 8-byte little-endian integer (int<8>).
     *
     * @param buffer byte buffer
     * @return long value
     */
    public static long readInt8(ByteBuffer buffer) {
        return (readInt4(buffer) & 0XFFFFFFFFL) | (((long) readInt4(buffer)) << 32);
    }

    /**
     * Reads a length-encoded integer (int<lenenc>).
     *
     * @param buffer byte buffer
     * @return long value
     */
    public static long readVInt(ByteBuffer buffer) {
        int b = readInt1(buffer);

        if (b < 251) {
            return b;
        }
        if (b == 252) {
            return readInt2(buffer);
        }
        if (b == 253) {
            return readInt3(buffer);
        }
        if (b == 254) {
            return readInt8(buffer);
        }
        if (b == 251) {
            throw new NullPointerException("NULL value in length-encoded integer");
        }
        return 0;
    }

    /**
     * Reads a fixed-length string (string<fix>).
     *
     * @param buffer byte buffer
     * @param len    string length
     * @return byte array containing the string
     */
    public static byte[] readFixedString(ByteBuffer buffer, int len) {
        byte[] buf = new byte[len];
        buffer.get(buf);
        return buf;
    }

    /**
     * Reads an EOF-terminated string (string<EOF>).
     *
     * @param buffer byte buffer
     * @return byte array containing the string
     */
    public static byte[] readEofString(ByteBuffer buffer) {
        byte[] buf = new byte[buffer.remaining()];
        buffer.get(buf);
        return buf;
    }

    /**
     * Reads a length-encoded string (string<lenenc>).
     *
     * @param buffer byte buffer
     * @return byte array containing the string
     */
    public static byte[] readLenEncodedString(ByteBuffer buffer) {
        long length = readVInt(buffer);
        byte[] buf = new byte[(int) length];
        buffer.get(buf);
        return buf;
    }

    /**
     * Reads a NULL-terminated string (string<NUL>).
     *
     * @param buffer byte buffer
     * @return byte array containing the string (without the NULL terminator)
     */
    public static byte[] readNulTerminateString(ByteBuffer buffer) {
        int oldPos = buffer.position();
        int nullPos = oldPos;
        for (nullPos = oldPos; nullPos < buffer.limit(); ++nullPos) {
            if (buffer.get(nullPos) == 0) {
                break;
            }
        }
        byte[] buf = new byte[nullPos - oldPos];
        buffer.get(buf);
        // skip null byte
        buffer.get();
        return buf;
    }
}
