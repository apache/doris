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

import org.apache.doris.fs.io.DorisPath;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;

import static java.util.Objects.requireNonNull;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;

public class HdfsOutputStream extends FSDataOutputStream {
    private final DorisPath path;
    private final Path hadoopPath;
    private final DFSFileSystem dfs;
    private boolean closed;

    public HdfsOutputStream(DorisPath path, FSDataOutputStream out, DFSFileSystem dfs) {
        super(out, null, out.getPos());
        this.path = requireNonNull(path, "path is null");
        this.hadoopPath = path.toHadoopPath();
        this.dfs = dfs;
    }

    @Override
    public OutputStream getWrappedStream() {
        // return the originally wrapped stream, not the delegate
        return ((FSDataOutputStream) super.getWrappedStream()).getWrappedStream();
    }

    @Override
    public void write(int b) throws IOException {
        checkClosed();
        // handle Kerberos ticket refresh during long write operations
        dfs.getAuthenticator().doAs(() -> {
            super.write(b);
            return null;
        });
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        checkClosed();
        // handle Kerberos ticket refresh during long write operations
        dfs.getAuthenticator().doAs(() -> {
            super.write(b, off, len);
            return null;
        });
    }

    @Override
    public void flush() throws IOException {
        checkClosed();
        super.flush();
    }

    @Override
    public void close() throws IOException {
        closed = true;
        super.close();
    }


    private void checkClosed() throws IOException {
        if (closed) {
            throw new IOException("Output stream is closed: " + path);
        }
    }
}
