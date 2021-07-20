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

package org.apache.doris.common.parquet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * Modified version of
 * https://github.com/tideworks/arvo2parquet/blob/master/src/main/java/com/tideworks/data_load/io/InputFile.java
 */
public class LocalInputFile implements InputFile {
    private static final Logger LOG = LogManager.getLogger(LocalInputFile.class);

    private static final int COPY_BUFFER_SIZE = 8192;
    private final RandomAccessFile input;

    public LocalInputFile(File file) throws FileNotFoundException {
        this.input = new RandomAccessFile(file, "r");
    }

    private static int readDirectBuffer(ByteBuffer byteBufr, byte[] tmpBuf, ByteBufReader rdr)
            throws IOException {
        // copy all the bytes that return immediately, stopping at the first
        // read that doesn't return a full buffer.
        int nextReadLength = Math.min(byteBufr.remaining(), tmpBuf.length);
        int totalBytesRead = 0;
        int bytesRead;

        while ((bytesRead = rdr.read(tmpBuf, 0, nextReadLength)) == tmpBuf.length) {
            byteBufr.put(tmpBuf);
            totalBytesRead += bytesRead;
            nextReadLength = Math.min(byteBufr.remaining(), tmpBuf.length);
        }

        if (bytesRead < 0) {
            // return -1 if nothing was read
            return totalBytesRead == 0 ? -1 : totalBytesRead;
        } else {
            // copy the last partial buffer
            byteBufr.put(tmpBuf, 0, bytesRead);
            totalBytesRead += bytesRead;
            return totalBytesRead;
        }
    }

    private static void readFullyDirectBuffer(ByteBuffer byteBufr, byte[] tmpBuf, ByteBufReader rdr)
            throws IOException {
        int nextReadLength = Math.min(byteBufr.remaining(), tmpBuf.length);
        int bytesRead = 0;

        while (nextReadLength > 0 && (bytesRead = rdr.read(tmpBuf, 0, nextReadLength)) >= 0) {
            byteBufr.put(tmpBuf, 0, bytesRead);
            nextReadLength = Math.min(byteBufr.remaining(), tmpBuf.length);
        }

        if (bytesRead < 0 && byteBufr.remaining() > 0) {
            throw new EOFException(
                    "Reached the end of stream with " + byteBufr.remaining() + " bytes left to read");
        }
    }

    @Override
    public long getLength() throws IOException {
        return input.length();
    }

    @Override
    public SeekableInputStream newStream() {
        return new SeekableInputStream() {
            private final byte[] tmpBuf = new byte[COPY_BUFFER_SIZE];
            private long markPos = 0;

            @Override
            public int read() throws IOException {
                return input.read();
            }

            @SuppressWarnings("NullableProblems")
            @Override
            public int read(byte[] b) throws IOException {
                return input.read(b);
            }

            @SuppressWarnings("NullableProblems")
            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                return input.read(b, off, len);
            }

            @Override
            public long skip(long n) throws IOException {
                final long savPos = input.getFilePointer();
                final long amtLeft = input.length() - savPos;
                n = Math.min(n, amtLeft);
                final long newPos = savPos + n;
                input.seek(newPos);
                final long curPos = input.getFilePointer();
                return curPos - savPos;
            }

            @Override
            public int available() throws IOException {
                return 0;
            }

            @Override
            public void close() throws IOException {
                input.close();
            }

            @SuppressWarnings({"unchecked", "unused", "UnusedReturnValue"})
            private <T extends Throwable, R> R uncheckedExceptionThrow(Throwable t) throws T {
                throw (T) t;
            }

            @Override
            public synchronized void mark(int readlimit) {
                try {
                    markPos = input.getFilePointer();
                } catch (IOException e) {
                    uncheckedExceptionThrow(e);
                }
            }

            @Override
            public synchronized void reset() throws IOException {
                input.seek(markPos);
            }

            @Override
            public boolean markSupported() {
                return true;
            }

            @Override
            public long getPos() throws IOException {
                return input.getFilePointer();
            }

            @Override
            public void seek(long l) throws IOException {
                input.seek(l);
            }

            @Override
            public void readFully(byte[] bytes) throws IOException {
                input.readFully(bytes);
            }

            @Override
            public void readFully(byte[] bytes, int i, int i1) throws IOException {
                input.readFully(bytes, i, i1);
            }

            @Override
            public int read(ByteBuffer byteBuffer) throws IOException {
                return readDirectBuffer(byteBuffer, tmpBuf, input::read);
            }

            @Override
            public void readFully(ByteBuffer byteBuffer) throws IOException {
                readFullyDirectBuffer(byteBuffer, tmpBuf, input::read);
            }
        };
    }

    private interface ByteBufReader {

        int read(byte[] b, int off, int len) throws IOException;
    }
}

