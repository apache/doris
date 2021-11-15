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

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.common.util.BrokerReader;
import org.apache.doris.thrift.TBrokerFD;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

public class BrokerInputFile implements InputFile {
    private static final Logger LOG = LogManager.getLogger(BrokerInputFile.class);

    private static final int COPY_BUFFER_SIZE = 1024 * 1024;

    private BrokerDesc brokerDesc;
    private String filePath;
    private long fileLength = 0;
    private BrokerReader reader;
    private TBrokerFD fd;

    public static BrokerInputFile create(String filePath, BrokerDesc brokerDesc) throws IOException {
        BrokerInputFile inputFile = new BrokerInputFile(filePath, brokerDesc);
        inputFile.init();
        return inputFile;
    }

    // For test only. ip port is broker ip port
    public static BrokerInputFile create(String filePath, BrokerDesc brokerDesc, String ip, int port) throws IOException {
        BrokerInputFile inputFile = new BrokerInputFile(filePath, brokerDesc);
        inputFile.init(ip, port);
        return inputFile;
    }

    private BrokerInputFile(String filePath, BrokerDesc brokerDesc) {
        this.filePath = filePath;
        this.brokerDesc = brokerDesc;
    }

    private void init() throws IOException {
        this.reader = BrokerReader.create(this.brokerDesc);
        this.fileLength = this.reader.getFileLength(filePath);
        this.fd = this.reader.open(filePath);
    }

    // For test only. ip port is broker ip port
    private void init(String ip, int port) throws IOException {
        this.reader = BrokerReader.create(this.brokerDesc, ip, port);
        this.fileLength = this.reader.getFileLength(filePath);
        this.fd = this.reader.open(filePath);
    }

    @Override
    public long getLength() throws IOException {
        return fileLength;
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        return new SeekableInputStream() {
            private final byte[] tmpBuf = new byte[COPY_BUFFER_SIZE];
            private long currentPos = 0;
            private long markPos = 0;

            private long bufferOffset = 0;
            private long bufferLimit = 0;

            @Override
            public int read() throws IOException {
                try {
                    if (currentPos < bufferOffset || currentPos > bufferLimit || bufferOffset >= bufferLimit) {
                        bufferOffset = currentPos;
                        fill();
                    }
                    if (currentPos > bufferLimit) {
                        LOG.warn("current pos {} is larger than buffer limit {}. should not happen.", currentPos, bufferLimit);
                        return -1;
                    }

                    int pos = (int) (currentPos - bufferOffset);
                    int res = Byte.toUnsignedInt(tmpBuf[pos]);
                    ++currentPos;
                    return res;
                } catch (BrokerReader.EOFException e) {
                    return -1;
                }
            }

            private void fill() throws IOException, BrokerReader.EOFException {
                byte[] data = reader.pread(fd, bufferOffset, COPY_BUFFER_SIZE);
                System.arraycopy(data, 0, tmpBuf, 0, data.length);
                bufferLimit = bufferOffset + data.length;
            }

            @SuppressWarnings("NullableProblems")
            @Override
            public int read(byte[] b) throws IOException {
                try {
                    byte[] data = reader.pread(fd, currentPos, b.length);
                    System.arraycopy(data, 0, b, 0, data.length);
                    currentPos += data.length;
                    return data.length;
                } catch (BrokerReader.EOFException e) {
                    return -1;
                }
            }

            @SuppressWarnings("NullableProblems")
            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                try {
                    if (currentPos < bufferOffset || currentPos > bufferLimit || currentPos + len > bufferLimit) {
                        if (len > COPY_BUFFER_SIZE) {
                            // the data to be read is larger then max size of buffer.
                            // read it directly.
                            byte[] data = reader.pread(fd, currentPos, len);
                            System.arraycopy(data, 0, b, off, data.length);
                            currentPos += data.length;
                            return data.length;
                        }
                        // fill the buffer first
                        bufferOffset = currentPos;
                        fill();
                    }

                    if (currentPos > bufferLimit) {
                        LOG.warn("current pos {} is larger than buffer limit {}. should not happen.", currentPos, bufferLimit);
                        return -1;
                    }

                    int start = (int) (currentPos - bufferOffset);
                    int readLen = Math.min(len, (int) (bufferLimit - bufferOffset));
                    System.arraycopy(tmpBuf, start, b, off, readLen);
                    currentPos += readLen;
                    return readLen;
                } catch (BrokerReader.EOFException e) {
                    return -1;
                }
            }

            @Override
            public long skip(long n) throws IOException {
                final long left = fileLength - currentPos;
                long min = Math.min(n, left);
                currentPos += min;
                return min;
            }

            @Override
            public int available() throws IOException {
                return 0;
            }

            @Override
            public void close() throws IOException {
                reader.close(fd);
            }

            @SuppressWarnings({"unchecked", "unused", "UnusedReturnValue"})
            private <T extends Throwable, R> R uncheckedExceptionThrow(Throwable t) throws T {
                throw (T) t;
            }

            @Override
            public synchronized void mark(int readlimit) {
                markPos = currentPos;
            }

            @Override
            public synchronized void reset() throws IOException {
                currentPos = markPos;
            }

            @Override
            public boolean markSupported() {
                return true;
            }

            @Override
            public long getPos() throws IOException {
                return currentPos;
            }

            @Override
            public void seek(long l) throws IOException {
                currentPos = l;
            }

            @Override
            public void readFully(byte[] bytes) throws IOException {
                try {
                    byte[] data = reader.pread(fd, currentPos, bytes.length);
                    System.arraycopy(data, 0, bytes, 0, data.length);
                    currentPos += data.length;
                    if (data.length < bytes.length) {
                        throw new EOFException("Reach the end of file with " + (bytes.length - data.length)
                                + " bytes left to read");
                    }
                } catch (BrokerReader.EOFException e) {
                    throw new EOFException("Reach the end of file");
                }
            }

            @Override
            public void readFully(byte[] bytes, int offset, int len) throws IOException {
                try {
                    byte[] data = reader.pread(fd, currentPos, len);
                    System.arraycopy(data, 0, bytes, offset, data.length);
                    currentPos += data.length;
                    if (data.length < len) {
                        throw new EOFException("Reach the end of file with " + (len - data.length)
                                + " bytes left to read");
                    }
                } catch (BrokerReader.EOFException e) {
                    throw new EOFException("Reach the end of file");
                }
            }

            @Override
            public int read(ByteBuffer byteBuffer) throws IOException {
                try {
                    byte[] data = reader.pread(fd, currentPos, byteBuffer.remaining());
                    byteBuffer.put(data);
                    currentPos += data.length;
                    return data.length;
                } catch (BrokerReader.EOFException e) {
                    return -1;
                }
            }

            @Override
            public void readFully(ByteBuffer byteBuffer) throws IOException {
                long markCurPos = currentPos;
                while (byteBuffer.remaining() > 0) {
                    try {
                        byte[] data = reader.pread(fd, currentPos, byteBuffer.remaining());
                        byteBuffer.put(data);
                        currentPos += data.length;
                    } catch (BrokerReader.EOFException e) {
                        if (byteBuffer.remaining() > 0) {
                            throw new EOFException("Reach the end of file with " + byteBuffer.remaining() + " bytes left to read. "
                                + "read len: " + (currentPos - markCurPos));
                        }
                    }
                }
            }
        }; // end of new SeekableInputStream
    } // end of newStream
}

