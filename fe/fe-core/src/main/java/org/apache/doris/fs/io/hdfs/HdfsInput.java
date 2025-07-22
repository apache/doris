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

import org.apache.doris.fs.io.DorisInput;
import org.apache.doris.fs.io.DorisInputFile;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Objects;

/**
 * HdfsInput provides an implementation of DorisInput for reading data from HDFS at a low level.
 * It wraps an FSDataInputStream and a DorisInputFile, providing random access read functionality.
 */
public class HdfsInput implements DorisInput {
    // The underlying Hadoop FSDataInputStream used for reading.
    private final FSDataInputStream stream;
    // The DorisInputFile representing the file.
    private final DorisInputFile inputFile;
    // Indicates whether the input has been closed.
    private boolean closed;

    /**
     * Constructs a HdfsInput with the given FSDataInputStream and DorisInputFile.
     *
     * @param stream the underlying Hadoop FSDataInputStream
     * @param inputFile the DorisInputFile representing the file
     */
    public HdfsInput(FSDataInputStream stream, DorisInputFile inputFile) {
        this.stream = Objects.requireNonNull(stream, "stream is null");
        this.inputFile = Objects.requireNonNull(inputFile, "inputFile is null");
    }

    /**
     * Checks if the input is closed and throws an IOException if it is.
     * Used internally before performing any operation.
     *
     * @throws IOException if the input is closed
     */
    private void checkClosed() throws IOException {
        if (closed) {
            throw new IOException("Input is closed: " + inputFile.toString());
        }
    }

    /**
     * Reads bytes from the file at the specified position into the buffer.
     *
     * @param position the position in the file to start reading
     * @param buffer the buffer into which the data is read
     * @param bufferOffset the start offset in the buffer
     * @param bufferLength the number of bytes to read
     * @throws IOException if an I/O error occurs or the input is closed
     */
    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength) throws IOException {
        checkClosed();
        try {
            stream.readFully(position, buffer, bufferOffset, bufferLength);
        } catch (FileNotFoundException e) {
            throw new FileNotFoundException(
                    String.format("File %s not found: %s", inputFile.toString(), e.getMessage()));
        } catch (IOException e) {
            throw new IOException(String.format("Read %d bytes at position %d failed for file %s: %s",
                    bufferLength, position, inputFile.toString(), e.getMessage()), e);
        }
    }

    /**
     * Closes this input and releases any system resources associated with it.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        closed = true;
        stream.close();
    }

    /**
     * Returns the string representation of the input file.
     *
     * @return the file path as a string
     */
    @Override
    public String toString() {
        return inputFile.toString();
    }
}
