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

package org.apache.doris.datasource.iceberg.hadoop;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.iceberg.io.DelegatingOutputStream;
import org.apache.iceberg.io.PositionOutputStream;

import java.io.IOException;
import java.io.OutputStream;

public class IcebergPositionOutputStream extends PositionOutputStream implements DelegatingOutputStream {

    private final FSDataOutputStream stream;

    IcebergPositionOutputStream(FSDataOutputStream stream) {
        this.stream = stream;
    }

    public OutputStream getDelegate() {
        return this.stream;
    }

    public long getPos() throws IOException {
        return this.stream.getPos();
    }

    public void write(int b) throws IOException {
        this.stream.write(b);
    }

    public void write(byte[] b) throws IOException {
        this.stream.write(b);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        this.stream.write(b, off, len);
    }

    public void flush() throws IOException {
        this.stream.flush();
    }

    public void close() throws IOException {
        this.stream.close();
    }
}
