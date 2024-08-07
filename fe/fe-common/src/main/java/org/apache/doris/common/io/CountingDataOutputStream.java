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

package org.apache.doris.common.io;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * This class adds a long type counter for DataOutputStream to
 * count the bytes written to the data output stream so far.
 */

public class CountingDataOutputStream extends DataOutputStream {
    private static final long FSYNC_SIZE_IN_BYTES = 1024 * 1024 * 8;
    /**
     * The number of bytes written to the data output stream so far. If this counter overflows,
     * it will be wrapped to Long.MAX_VALUE.
     */
    private long count;
    /**
     * The number of bytes since from last force sync so far. If the fsyncDelta greater than fsyncSize,
     * it will force sync
     */
    private long fsyncDelta;
    protected long fsyncSize;

    public CountingDataOutputStream(OutputStream out) {
        this(out, 0, FSYNC_SIZE_IN_BYTES);
    }

    public CountingDataOutputStream(OutputStream out, long fsyncSize) {
        this(out, 0, fsyncSize);
    }

    public CountingDataOutputStream(OutputStream out, long count, long fsyncSize) {
        super(out);
        this.count = count;
        this.fsyncSize = fsyncSize;
        this.fsyncDelta = 0;
    }

    public long getCount() {
        return this.count;
    }

    public void write(byte[] b, int off, int len) throws IOException {
        super.write(b, off, len);
        incCount(len);
        partialFsync(len);
    }

    /**
     * see {@link java.io.DataOutputStream#write(int)} or {@link java.io.OutputStream#write(int)}
     */
    @Override
    public synchronized void write(int b) throws IOException {
        super.write(b);
        incCount(1);
        partialFsync(1);
    }

    /**
     * Increases the written counter by the specified value until it reaches Long.MAX_VALUE.
     */
    private void incCount(int value) {
        long temp = count + value;
        if (temp < 0) {
            temp = Long.MAX_VALUE;
        }
        count = temp;
    }

    /**
     * image file force sync partial，avoid excessive sync size that causes disk IO throughput to be full，so we force sync
     * partial ahead
     */
    private void partialFsync(int deltaBytes) throws IOException {
        this.fsyncDelta += deltaBytes;
        // if this.out is FileOutputStream we should force sync disk in a batch (eg:8MB)
        if (this.fsyncDelta >= fsyncSize && this.out instanceof FileOutputStream) {
            ((FileOutputStream) this.out).getChannel().force(true);
            this.fsyncDelta = 0;
        }
    }
}
