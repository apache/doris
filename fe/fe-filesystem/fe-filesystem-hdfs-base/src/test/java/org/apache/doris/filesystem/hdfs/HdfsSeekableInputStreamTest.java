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

package org.apache.doris.filesystem.hdfs;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

class HdfsSeekableInputStreamTest {

    /**
     * A seekable InputStream that wraps a byte array, implementing the Hadoop
     * interfaces required by FSDataInputStream.
     */
    private static class SeekableByteArrayInputStream extends InputStream
            implements Seekable, PositionedReadable {
        private final byte[] data;
        private int pos;

        SeekableByteArrayInputStream(byte[] data) {
            this.data = data;
            this.pos = 0;
        }

        @Override
        public int read() {
            return pos < data.length ? data[pos++] & 0xFF : -1;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (pos >= data.length) {
                return -1;
            }
            int toRead = Math.min(len, data.length - pos);
            System.arraycopy(data, pos, b, off, toRead);
            pos += toRead;
            return toRead;
        }

        @Override
        public void seek(long targetPos) throws IOException {
            if (targetPos < 0 || targetPos > data.length) {
                throw new IOException("Seek out of range: " + targetPos);
            }
            pos = (int) targetPos;
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public boolean seekToNewSource(long targetPos) {
            return false;
        }

        @Override
        public int read(long position, byte[] buffer, int offset, int length) throws IOException {
            if (position >= data.length) {
                return -1;
            }
            int toRead = Math.min(length, data.length - (int) position);
            System.arraycopy(data, (int) position, buffer, offset, toRead);
            return toRead;
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
            int bytesRead = read(position, buffer, offset, length);
            if (bytesRead < length) {
                throw new IOException("Premature EOF");
            }
        }

        @Override
        public void readFully(long position, byte[] buffer) throws IOException {
            readFully(position, buffer, 0, buffer.length);
        }

        @Override
        public int available() {
            return data.length - pos;
        }
    }

    private static final byte[] TEST_DATA = "ABCDEFGHIJ".getBytes(StandardCharsets.UTF_8);

    private FSDataInputStream realStream;
    private HdfsSeekableInputStream stream;

    @BeforeEach
    void setUp() {
        realStream = new FSDataInputStream(new SeekableByteArrayInputStream(TEST_DATA));
        stream = new HdfsSeekableInputStream("/test/path", realStream);
    }

    @Test
    void getPosReturnsCorrectPosition() throws IOException {
        Assertions.assertEquals(0L, stream.getPos());
        stream.read();
        Assertions.assertEquals(1L, stream.getPos());
    }

    @Test
    void seekChangesPosition() throws IOException {
        stream.seek(5L);
        Assertions.assertEquals(5L, stream.getPos());
        Assertions.assertEquals('F', (char) stream.read());
    }

    @Test
    void readSingleByteReturnsCorrectValue() throws IOException {
        Assertions.assertEquals('A', (char) stream.read());
        Assertions.assertEquals('B', (char) stream.read());
    }

    @Test
    void readArrayReturnsCorrectData() throws IOException {
        byte[] buf = new byte[5];
        int bytesRead = stream.read(buf, 0, 5);
        Assertions.assertEquals(5, bytesRead);
        Assertions.assertEquals("ABCDE", new String(buf, StandardCharsets.UTF_8));
    }

    @Test
    void skipMovesForward() throws IOException {
        long skipped = stream.skip(3L);
        Assertions.assertTrue(skipped > 0);
        Assertions.assertEquals('D', (char) stream.read());
    }

    @Test
    void availableReturnsRemainingBytes() throws IOException {
        Assertions.assertEquals(TEST_DATA.length, stream.available());
        stream.read();
        Assertions.assertEquals(TEST_DATA.length - 1, stream.available());
    }

    @Test
    void closeDoesNotThrow() throws IOException {
        stream.close();
    }

    @Test
    void readAfterCloseThrowsIOException() throws IOException {
        stream.close();
        Assertions.assertThrows(IOException.class, () -> stream.read());
    }

    @Test
    void seekAfterCloseThrowsIOException() throws IOException {
        stream.close();
        Assertions.assertThrows(IOException.class, () -> stream.seek(0));
    }

    @Test
    void getPosAfterCloseThrowsIOException() throws IOException {
        stream.close();
        Assertions.assertThrows(IOException.class, () -> stream.getPos());
    }

    @Test
    void seekOutOfRangeWrapsException() throws IOException {
        IOException ex = Assertions.assertThrows(IOException.class, () -> stream.seek(-1L));
        Assertions.assertTrue(ex.getMessage().contains("seek(-1) failed"));
    }
}
