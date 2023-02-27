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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ByteBufferNetworkInputStream extends InputStream {
    private ArrayBlockingQueue<ByteArrayInputStream> queue;
    private ByteArrayInputStream currentInputStream;
    private volatile boolean finished = false;
    private volatile boolean closed = false;

    public ByteBufferNetworkInputStream() {
        this(32);
    }

    public ByteBufferNetworkInputStream(int capacity) {
        this.queue = new ArrayBlockingQueue<>(capacity);
    }

    public void fillByteBuffer(ByteBuffer buffer) throws IOException, InterruptedException {
        if (closed) {
            throw new IOException("Stream is already closed.");
        }
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytesCopy(buffer));
        queue.offer(inputStream, 300, TimeUnit.SECONDS);
    }

    public byte[] bytesCopy(ByteBuffer buffer) {
        byte[] result = new byte[buffer.limit() - buffer.position()];
        System.arraycopy(buffer.array(), buffer.position(), result, 0, result.length);
        return result;
    }

    public void markFinished() {
        this.finished = true;
    }

    private ByteArrayInputStream getNextByteArrayStream() throws IOException {
        if (currentInputStream == null || currentInputStream.available() == 0) {
            // No any byte array stream will come
            while (!finished || !queue.isEmpty()) {
                try {
                    currentInputStream = queue.poll(1, TimeUnit.SECONDS);
                    if (currentInputStream != null) {
                        return currentInputStream;
                    }
                } catch (InterruptedException e) {
                    throw new IOException("Failed to get next stream");
                }
            }
            return null;
        }
        return currentInputStream;
    }

    @Override
    public int read() throws IOException {
        ByteArrayInputStream stream = getNextByteArrayStream();
        if (stream == null) {
            return -1;
        }

        return stream.read();
    }

    public int read(byte[] b, int off, int len) throws IOException {
        ByteArrayInputStream stream = getNextByteArrayStream();
        if (stream == null) {
            return -1;
        }
        return stream.read(b, off, len);
    }

    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    public void close() throws IOException {
        closed = true;
        ByteArrayInputStream stream = getNextByteArrayStream();
        if (stream == null) {
            return;
        }
        stream.close();

        while (!queue.isEmpty()) {
            queue.poll().close();
        }
    }
}
