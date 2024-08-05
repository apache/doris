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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.iceberg.io.SeekableInputStream;

import java.io.IOException;

public class IcebergSeekableInputStream extends SeekableInputStream {
    private final FSDataInputStream stream;

    public IcebergSeekableInputStream(FSDataInputStream stream) {
        this.stream = stream;
    }

    @Override
    public long getPos() throws IOException {
        return stream.getPos();
    }

    @Override
    public void seek(long position) throws IOException {
        stream.seek(position);
    }

    @Override
    public int read() throws IOException {
        return stream.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return stream.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return stream.read(b, off, len);
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }

}
