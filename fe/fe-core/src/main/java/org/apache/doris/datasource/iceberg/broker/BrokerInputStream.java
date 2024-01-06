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

package org.apache.doris.datasource.iceberg.broker;

import org.apache.doris.common.util.BrokerReader;
import org.apache.doris.thrift.TBrokerFD;

import org.apache.iceberg.io.SeekableInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class BrokerInputStream extends SeekableInputStream {
    private static final Logger LOG = LogManager.getLogger(BrokerInputStream.class);
    private static final int COPY_BUFFER_SIZE = 1024 * 1024; // 1MB

    private final byte[] tmpBuf = new byte[COPY_BUFFER_SIZE];
    private long currentPos = 0;
    private long markPos = 0;

    private long bufferOffset = 0;
    private long bufferLimit = 0;
    private final BrokerReader reader;
    private final TBrokerFD fd;
    private final long fileLength;

    public BrokerInputStream(BrokerReader reader, TBrokerFD fd, long fileLength) {
        this.fd = fd;
        this.reader = reader;
        this.fileLength = fileLength;
    }

    @Override
    public long getPos() throws IOException {
        return currentPos;
    }

    @Override
    public void seek(long newPos) throws IOException {
        currentPos = newPos;
    }

    @Override
    public int read() throws IOException {
        try {
            if (currentPos < bufferOffset || currentPos > bufferLimit || bufferOffset >= bufferLimit) {
                bufferOffset = currentPos;
                fill();
            }
            if (currentPos > bufferLimit) {
                LOG.warn("current pos {} is larger than buffer limit {}."
                        + " should not happen.", currentPos, bufferLimit);
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
                LOG.warn("current pos {} is larger than buffer limit {}."
                        + " should not happen.", currentPos, bufferLimit);
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

    private void fill() throws IOException, BrokerReader.EOFException {
        if (bufferOffset == this.fileLength) {
            throw new BrokerReader.EOFException();
        }
        byte[] data = reader.pread(fd, bufferOffset, COPY_BUFFER_SIZE);
        System.arraycopy(data, 0, tmpBuf, 0, data.length);
        bufferLimit = bufferOffset + data.length;
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
}
