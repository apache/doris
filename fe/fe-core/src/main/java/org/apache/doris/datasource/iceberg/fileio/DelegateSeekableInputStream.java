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

package org.apache.doris.datasource.iceberg.fileio;

import org.apache.doris.fs.io.DorisInputStream;

import org.apache.iceberg.io.SeekableInputStream;

import java.io.IOException;
import java.util.Objects;

/**
 * DelegateSeekableInputStream is an implementation of Iceberg's SeekableInputStream.
 * It wraps a DorisInputStream and delegates all stream and seek operations to it,
 * providing integration between Doris file system and Iceberg's seekable input abstraction.
 */
public class DelegateSeekableInputStream extends SeekableInputStream {
    /**
     * The underlying DorisInputStream used for all operations.
     */
    private final DorisInputStream stream;

    /**
     * Constructs a DelegateSeekableInputStream with the specified DorisInputStream.
     * @param stream the DorisInputStream to delegate operations to
     */
    public DelegateSeekableInputStream(DorisInputStream stream) {
        this.stream = Objects.requireNonNull(stream, "stream is null");
    }

    // ===================== Position and Seek Methods =====================

    /**
     * Returns the current position in the stream.
     * @return the current byte position
     * @throws IOException if an I/O error occurs
     */
    @Override
    public long getPos() throws IOException {
        return stream.getPosition();
    }

    /**
     * Seeks to the specified position in the stream.
     * @param pos the position to seek to
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void seek(long pos) throws IOException {
        stream.seek(pos);
    }

    // ===================== Read Methods =====================

    /**
     * Reads a single byte from the stream.
     * @return the byte read, or -1 if end of stream
     * @throws IOException if an I/O error occurs
     */
    @Override
    public int read() throws IOException {
        return stream.read();
    }

    /**
     * Reads bytes into the specified array.
     * @param b the buffer into which the data is read
     * @return the number of bytes read, or -1 if end of stream
     * @throws IOException if an I/O error occurs
     */
    @Override
    public int read(byte[] b) throws IOException {
        return stream.read(b);
    }

    /**
     * Reads up to len bytes of data from the stream into an array of bytes.
     * @param b the buffer into which the data is read
     * @param off the start offset in array b at which the data is written
     * @param len the maximum number of bytes to read
     * @return the number of bytes read, or -1 if end of stream
     * @throws IOException if an I/O error occurs
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return stream.read(b, off, len);
    }

    // ===================== Skip and Availability Methods =====================

    /**
     * Skips over and discards n bytes of data from the stream.
     * @param n the number of bytes to skip
     * @return the actual number of bytes skipped
     * @throws IOException if an I/O error occurs
     */
    @Override
    public long skip(long n) throws IOException {
        return stream.skip(n);
    }

    /**
     * Returns an estimate of the number of bytes that can be read from the stream.
     * @return the number of bytes that can be read
     * @throws IOException if an I/O error occurs
     */
    @Override
    public int available() throws IOException {
        return stream.available();
    }

    // ===================== Mark, Reset, and Close Methods =====================

    /**
     * Closes the stream and releases any system resources associated with it.
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        stream.close();
    }

    /**
     * Marks the current position in the stream.
     * @param readlimit the maximum limit of bytes that can be read before the mark position becomes invalid
     */
    @Override
    public void mark(int readlimit) {
        stream.mark(readlimit);
    }

    /**
     * Resets the stream to the most recent mark.
     * @throws IOException if the stream has not been marked or the mark has been invalidated
     */
    @Override
    public void reset() throws IOException {
        stream.reset();
    }

    /**
     * Tests if this input stream supports the mark and reset methods.
     * @return true if mark and reset are supported; false otherwise
     */
    @Override
    public boolean markSupported() {
        return stream.markSupported();
    }
}
