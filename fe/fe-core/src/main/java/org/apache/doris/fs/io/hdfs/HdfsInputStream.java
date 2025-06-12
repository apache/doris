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
import org.apache.doris.fs.io.DorisPath;

import static java.util.Objects.requireNonNull;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.FileNotFoundException;
import java.io.IOException;

public class HdfsInputStream extends DorisInputStream {
    private final DorisPath path;
    private final FSDataInputStream stream;
    private boolean closed;

    HdfsInputStream(DorisPath path, FSDataInputStream stream) {
        this.path = requireNonNull(path, "path is null");
        this.stream = requireNonNull(stream, "stream is null");
    }

    @Override
    public int available() throws IOException {
        checkClosed();
        try {
            return stream.available();
        } catch (IOException e) {
            throw new IOException(String.format("Failed to get available status for file %s.", path), e);
        }
    }

    @Override
    public long getPosition() throws IOException {
        checkClosed();
        try {
            return stream.getPos();
        } catch (IOException e) {
            throw new IOException(String.format("Failed to get position for file %s.", path), e);
        }
    }

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

    @Override
    public long skip(long n) throws IOException {
        checkClosed();
        try {
            return stream.skip(n);
        } catch (IOException e) {
            throw new IOException(String.format("Skip in file %s failed: %s", path, e.getMessage()), e);
        }
    }

    @Override
    public void close() throws IOException {
        closed = true;
        stream.close();
    }

    private void checkClosed() throws IOException {
        if (closed) {
            throw new IOException("Input stream is closed: " + path);
        }
    }
}
