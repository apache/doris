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

package org.apache.doris.fs.io.hdfs;

import org.apache.doris.fs.io.DorisInputStream;
import org.apache.doris.fs.io.ParsedPath;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Objects;

/**
 * HdfsInputStream provides an input stream implementation for reading data from HDFS
 * using ParsedPath and FSDataInputStream.
 * It extends DorisInputStream and wraps Hadoop's FSDataInputStream, providing additional checks and error handling.
 */
public class HdfsInputStream extends DorisInputStream {
    // The ParsedPath representing the file location in HDFS.
    private final ParsedPath path;
    // The underlying Hadoop FSDataInputStream used for reading.
    private final FSDataInputStream stream;
    // Indicates whether the stream has been closed.
    private boolean closed;

    /**
     * Constructs a HdfsInputStream with the given ParsedPath and FSDataInputStream.
     *
     * @param path the ParsedPath representing the file location
     * @param stream the underlying Hadoop FSDataInputStream
     */
    HdfsInputStream(ParsedPath path, FSDataInputStream stream) {
        this.path = Objects.requireNonNull(path, "path is null");
        this.stream = Objects.requireNonNull(stream, "stream is null");
    }

    /**
     * Checks if the stream is closed and throws an IOException if it is.
     * Used internally before performing any operation.
     *
     * @throws IOException if the stream is closed
     */
    private void checkClosed() throws IOException {
        if (closed) {
            throw new IOException("Input stream is closed: " + path);
        }
    }

    /**
     * Returns the number of bytes that can be read from this input stream without blocking.
     *
     * @return the number of available bytes
     * @throws IOException if an I/O error occurs or the stream is closed
     */
    @Override
    public int available() throws IOException {
        checkClosed();
        try {
            return stream.available();
        } catch (IOException e) {
            throw new IOException(String.format("Failed to get available status for file %s.", path), e);
        }
    }

    /**
     * Returns the current position in the input stream.
     *
     * @return the current position
     * @throws IOException if an I/O error occurs or the stream is closed
     */
    @Override
    public long getPosition() throws IOException {
        checkClosed();
        try {
            return stream.getPos();
        } catch (IOException e) {
            throw new IOException(String.format("Failed to get position for file %s.", path), e);
        }
    }

    /**
     * Seeks to the specified position in the input stream.
     *
     * @param position the position to seek to
     * @throws IOException if an I/O error occurs or the stream is closed
     */
    @Override
    public void seek(long position) throws IOException {
        checkClosed();
        try {
            stream.seek(position);
        } catch (IOException e) {
            throw new IOException(
                    String.format("Failed to seek to position %d for file %s: %s", position, path, e.getMessage()), e);
        }
    }

    /**
     * Reads the next byte of data from the input stream.
     *
     * @return the next byte of data, or -1 if the end of the stream is reached
     * @throws IOException if an I/O error occurs or the stream is closed
     */
    @Override
    public int read() throws IOException {
        checkClosed();
        try {
            return stream.read();
        } catch (FileNotFoundException e) {
            throw new FileNotFoundException(String.format("File %s not found: %s", path, e.getMessage()));
        } catch (IOException e) {
            throw new IOException(String.format("Read of file %s failed: %s", path, e.getMessage()), e);
        }
    }

    /**
     * Reads up to len bytes of data from the input stream into an array of bytes.
     *
     * @param b the buffer into which the data is read
     * @param off the start offset in array b at which the data is written
     * @param len the maximum number of bytes to read
     * @return the total number of bytes read into the buffer, or -1 if there is no more data
     * @throws IOException if an I/O error occurs or the stream is closed
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        checkClosed();
        try {
            return stream.read(b, off, len);
        } catch (FileNotFoundException e) {
            throw new FileNotFoundException(String.format("File %s not found: %s", path, e.getMessage()));
        } catch (IOException e) {
            throw new IOException(String.format("Read of file %s failed: %s", path, e.getMessage()), e);
        }
    }

    /**
     * Skips over and discards n bytes of data from this input stream.
     *
     * @param n the number of bytes to skip
     * @return the actual number of bytes skipped
     * @throws IOException if an I/O error occurs or the stream is closed
     */
    @Override
    public long skip(long n) throws IOException {
        checkClosed();
        try {
            return stream.skip(n);
        } catch (IOException e) {
            throw new IOException(String.format("Skip in file %s failed: %s", path, e.getMessage()), e);
        }
    }

    /**
     * Closes this input stream and releases any system resources associated with it.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        closed = true;
        stream.close();
    }
}
