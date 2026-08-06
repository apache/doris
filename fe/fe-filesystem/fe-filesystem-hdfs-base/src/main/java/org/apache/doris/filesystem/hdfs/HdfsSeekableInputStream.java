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

import org.apache.doris.filesystem.DorisInputStream;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

/**
 * A {@link DorisInputStream} backed by Hadoop's {@link FSDataInputStream}.
 *
 * <p>HDFS's {@code FSDataInputStream} natively supports seeking,
 * so this wrapper is a thin delegation layer.
 */
class HdfsSeekableInputStream extends DorisInputStream {

    private final String path;
    private final FSDataInputStream stream;
    private boolean closed;

    HdfsSeekableInputStream(String path, FSDataInputStream stream) {
        this.path = path;
        this.stream = stream;
    }

    private void checkOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream already closed: " + path);
        }
    }

    @Override
    public long getPos() throws IOException {
        checkOpen();
        return stream.getPos();
    }

    @Override
    public void seek(long pos) throws IOException {
        checkOpen();
        try {
            stream.seek(pos);
        } catch (IOException e) {
            throw new IOException("seek(" + pos + ") failed for " + path + ": " + e.getMessage(), e);
        }
    }

    @Override
    public int read() throws IOException {
        checkOpen();
        return stream.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        checkOpen();
        return stream.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        checkOpen();
        return stream.skip(n);
    }

    @Override
    public int available() throws IOException {
        checkOpen();
        return stream.available();
    }

    @Override
    public void close() throws IOException {
        closed = true;
        stream.close();
    }
}
