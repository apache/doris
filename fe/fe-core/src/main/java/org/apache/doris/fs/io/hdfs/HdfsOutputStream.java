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

import org.apache.doris.fs.io.ParsedPath;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

/**
 * HdfsOutputStream provides an output stream implementation for writing data to HDFS
 * using ParsedPath and FSDataOutputStream.
 * It extends FSDataOutputStream and adds additional checks and Kerberos authentication handling.
 */
public class HdfsOutputStream extends FSDataOutputStream {
    // The ParsedPath representing the file location in HDFS.
    private final ParsedPath path;
    // The Hadoop Path object corresponding to the file.
    private final Path hadoopPath;
    // The DFSFileSystem used to interact with HDFS.
    private final DFSFileSystem dfs;
    // Indicates whether the stream has been closed.
    private boolean closed;

    /**
     * Constructs a HdfsOutputStream with the given ParsedPath, FSDataOutputStream, and DFSFileSystem.
     *
     * @param path the ParsedPath representing the file location
     * @param out the underlying Hadoop FSDataOutputStream
     * @param dfs the DFSFileSystem used to interact with HDFS
     */
    public HdfsOutputStream(ParsedPath path, FSDataOutputStream out, DFSFileSystem dfs) {
        super(out, null, out.getPos());
        this.path = Objects.requireNonNull(path, "path is null");
        this.hadoopPath = path.toHadoopPath();
        this.dfs = dfs;
    }

    /**
     * Checks if the stream is closed and throws an IOException if it is.
     * Used internally before performing any operation.
     *
     * @throws IOException if the stream is closed
     */
    private void checkClosed() throws IOException {
        if (closed) {
            throw new IOException("Output stream is closed: " + path);
        }
    }

    /**
     * Returns the originally wrapped OutputStream, not the delegate.
     *
     * @return the wrapped OutputStream
     */
    @Override
    public OutputStream getWrappedStream() {
        return ((FSDataOutputStream) super.getWrappedStream()).getWrappedStream();
    }

    /**
     * Writes the specified byte to this output stream, handling Kerberos ticket refresh if needed.
     *
     * @param b the byte to write
     * @throws IOException if an I/O error occurs or the stream is closed
     */
    @Override
    public void write(int b) throws IOException {
        checkClosed();
        dfs.getAuthenticator().doAs(() -> {
            super.write(b);
            return null;
        });
    }

    /**
     * Writes len bytes from the specified byte array starting at offset off to this output stream,
     * handling Kerberos ticket refresh if needed.
     *
     * @param b the data
     * @param off the start offset in the data
     * @param len the number of bytes to write
     * @throws IOException if an I/O error occurs or the stream is closed
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        checkClosed();
        dfs.getAuthenticator().doAs(() -> {
            super.write(b, off, len);
            return null;
        });
    }

    /**
     * Flushes this output stream and forces any buffered output bytes to be written out.
     *
     * @throws IOException if an I/O error occurs or the stream is closed
     */
    @Override
    public void flush() throws IOException {
        checkClosed();
        super.flush();
    }

    /**
     * Closes this output stream and releases any system resources associated with it.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        closed = true;
        super.close();
    }
}
