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

package org.apache.doris.protocol.mysql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * MySQL protocol serializer.
 * 
 * <p>This class provides methods for serializing data to the MySQL protocol
 * byte stream format. It handles the encoding of various MySQL data types
 * including integers, strings, and length-encoded values.
 * 
 * <p>Reference: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_dt_integers.html
 * 
 * @since 2.0.0
 */
public class MysqlSerializer {
    private static final Logger LOG = LogManager.getLogger(MysqlSerializer.class);
    
    protected ByteArrayOutputStream out;
    protected MysqlCapability capability;

    /**
     * Creates a new serializer instance with default capability.
     * 
     * @return new MysqlSerializer instance
     */
    public static MysqlSerializer newInstance() {
        return new MysqlSerializer(new ByteArrayOutputStream(), MysqlCapability.DEFAULT_CAPABILITY);
    }

    /**
     * Creates a new serializer instance with specified capability.
     * 
     * @param capability the MySQL capability flags
     * @return new MysqlSerializer instance
     */
    public static MysqlSerializer newInstance(MysqlCapability capability) {
        return new MysqlSerializer(new ByteArrayOutputStream(), capability);
    }

    protected MysqlSerializer(ByteArrayOutputStream out, MysqlCapability capability) {
        this.out = out;
        this.capability = capability;
    }

    /**
     * Sets the capability after successful handshake.
     * 
     * @param capability the MySQL capability flags
     */
    public void setCapability(MysqlCapability capability) {
        this.capability = capability;
    }

    /**
     * Gets the current capability.
     * 
     * @return MySQL capability flags
     */
    public MysqlCapability getCapability() {
        return capability;
    }

    /**
     * Writes a single byte.
     * 
     * @param value byte value
     */
    public void writeByte(byte value) {
        out.write(value);
    }

    /**
     * Writes a NULL indicator (0xFB).
     */
    public void writeNull() {
        writeByte((byte) (251 & 0xff));
    }

    /**
     * Writes bytes from an array.
     * 
     * @param value byte array
     * @param offset start offset
     * @param length number of bytes to write
     */
    public void writeBytes(byte[] value, int offset, int length) {
        out.write(value, offset, length);
    }

    /**
     * Resets the serializer for reuse.
     */
    public void reset() {
        out.reset();
    }

    /**
     * Gets the serialized data as byte array.
     * 
     * @return byte array
     */
    public byte[] toArray() {
        return out.toByteArray();
    }

    /**
     * Gets the serialized data as ByteBuffer.
     * 
     * @return ByteBuffer
     */
    public ByteBuffer toByteBuffer() {
        return ByteBuffer.wrap(out.toByteArray());
    }

    /**
     * Writes a byte array.
     * 
     * @param value byte array
     */
    public void writeBytes(byte[] value) {
        writeBytes(value, 0, value.length);
    }

    /**
     * Writes a 1-byte integer (int<1>).
     * 
     * @param value integer value
     */
    public void writeInt1(int value) {
        writeByte((byte) (value & 0XFF));
    }

    /**
     * Writes a 2-byte little-endian integer (int<2>).
     * 
     * @param value integer value
     */
    public void writeInt2(int value) {
        writeByte((byte) value);
        writeByte((byte) (value >> 8));
    }

    /**
     * Writes a 3-byte little-endian integer (int<3>).
     * 
     * @param value integer value
     */
    public void writeInt3(int value) {
        writeByte((byte) value);
        writeByte((byte) (value >> 8));
        writeByte((byte) (value >> 16));
    }

    /**
     * Writes a 4-byte little-endian integer (int<4>).
     * 
     * @param value integer value
     */
    public void writeInt4(int value) {
        writeByte((byte) value);
        writeByte((byte) (value >> 8));
        writeByte((byte) (value >> 16));
        writeByte((byte) (value >> 24));
    }

    /**
     * Writes a 6-byte little-endian integer (int<6>).
     * 
     * @param value long value
     */
    public void writeInt6(long value) {
        writeInt4((int) value);
        writeInt2((byte) (value >> 32));
    }

    /**
     * Writes an 8-byte little-endian integer (int<8>).
     * 
     * @param value long value
     */
    public void writeInt8(long value) {
        writeInt4((int) value);
        writeInt4((int) (value >> 32));
    }

    /**
     * Writes a length-encoded integer (int<lenenc>).
     * 
     * @param value long value
     */
    public void writeVInt(long value) {
        if (value < 251) {
            writeByte((byte) value);
        } else if (value < 0x10000) {
            writeInt1(252);
            writeInt2((int) value);
        } else if (value < 0x1000000) {
            writeInt1(253);
            writeInt3((int) value);
        } else {
            writeInt1(254);
            writeInt8(value);
        }
    }

    /**
     * Writes a length-encoded string (string<lenenc>).
     * 
     * @param value string value
     */
    public void writeLenEncodedString(String value) {
        try {
            byte[] buf = value.getBytes("UTF-8");
            writeVInt(buf.length);
            writeBytes(buf);
        } catch (UnsupportedEncodingException e) {
            LOG.warn("", e);
        }
    }

    /**
     * Writes length-encoded raw bytes without charset transcoding.
     * 
     * @param buf byte array
     */
    public void writeLenEncodedBytes(byte[] buf) {
        writeVInt(buf.length);
        writeBytes(buf);
    }

    /**
     * Writes an EOF-terminated string (string<EOF>).
     * 
     * @param value string value
     */
    public void writeEofString(String value) {
        try {
            byte[] buf = value.getBytes("UTF-8");
            writeBytes(buf);
        } catch (UnsupportedEncodingException e) {
            LOG.warn("", e);
        }
    }

    /**
     * Writes a NULL-terminated string (string<NUL>).
     * 
     * @param value string value
     */
    public void writeNulTerminateString(String value) {
        try {
            byte[] buf = value.getBytes("UTF-8");
            writeBytes(buf);
            writeByte((byte) 0);
        } catch (UnsupportedEncodingException e) {
            LOG.warn("", e);
        }
    }

    /**
     * Writes a field definition with FieldInfo metadata.
     * 
     * <p>This method writes the column definition in ColumnDefinition41 format.
     * 
     * @param fieldInfo field metadata
     * @param mysqlTypeCode MySQL type code
     * @param charsetIndex character set index
     * @param columnLength column display length
     * @param flags column flags
     * @param decimals number of decimals
     */
    public void writeField(FieldInfo fieldInfo, int mysqlTypeCode, int charsetIndex, 
            int columnLength, int flags, int decimals) {
        // Catalog Name: length encoded string
        writeLenEncodedString("def");
        // Schema: length encoded string
        writeLenEncodedString(fieldInfo.getSchema());
        // Table: length encoded string
        writeLenEncodedString(fieldInfo.getTable());
        // Origin Table: length encoded string
        writeLenEncodedString(fieldInfo.getOriginalTable());
        // Name: length encoded string
        writeLenEncodedString(fieldInfo.getName());
        // Original Name: length encoded string
        writeLenEncodedString(fieldInfo.getOriginalName());
        // length of the following fields (always 0x0c)
        writeVInt(0x0c);
        // Character set: two byte integer
        writeInt2(charsetIndex);
        // Column length: four byte integer
        writeInt4(columnLength);
        // Column type: one byte integer
        writeInt1(mysqlTypeCode);
        // Flags: two byte integer
        writeInt2(flags);
        // Decimals: one byte integer
        writeInt1(decimals);
        // filler: two byte integer
        writeInt2(0);
    }
    
    /**
     * Writes a simple field definition with name only.
     * 
     * @param colName column name
     * @param mysqlTypeCode MySQL type code
     * @param charsetIndex character set index
     * @param columnLength column display length
     * @param flags column flags
     * @param decimals number of decimals
     */
    public void writeField(String colName, int mysqlTypeCode, int charsetIndex,
            int columnLength, int flags, int decimals) {
        // Catalog Name: length encoded string
        writeLenEncodedString("def");
        // Schema: length encoded string
        writeLenEncodedString("");
        // Table: length encoded string
        writeLenEncodedString("");
        // Origin Table: length encoded string
        writeLenEncodedString("");
        // Name: length encoded string
        writeLenEncodedString(colName);
        // Original Name: length encoded string
        writeLenEncodedString(colName);
        // length of the following fields (always 0x0c)
        writeVInt(0x0c);
        // Character set: two byte integer
        writeInt2(charsetIndex);
        // Column length: four byte integer
        writeInt4(columnLength);
        // Column type: one byte integer
        writeInt1(mysqlTypeCode);
        // Flags: two byte integer
        writeInt2(flags);
        // Decimals: one byte integer
        writeInt1(decimals);
        // filler: two byte integer
        writeInt2(0);
    }
}
