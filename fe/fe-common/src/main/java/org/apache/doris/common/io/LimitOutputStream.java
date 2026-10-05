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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A LimitOutputStream that the OutputStream is limited .
 */
public class LimitOutputStream extends OutputStream {
    private static final Logger LOG = LoggerFactory.getLogger(LimitOutputStream.class.getName());

    /**
     * The output stream to be limited.
     */
    protected final OutputStream out;
    protected final int speed;
    protected long bytesWriteTotal;
    protected long startTime;
    protected boolean bstart;

    /**
     * A output stream that writes the limited bytes to the given stream.
     *
     * @param out
     *            The stream to be limited
     * @param limitspeed
     *            The limit of speed: bytes per second
     */
    public LimitOutputStream(OutputStream out, int limitspeed)
            throws IOException {
        if (out == null) {
            throw new IOException("OutputStream is null");
        }
        speed = limitspeed;
        if (LOG.isDebugEnabled()) {
            LOG.debug("LimitOutputStream limit speed: {}", speed);
        }
        this.out = out;
        bytesWriteTotal = 0;
        bstart = false;
    }

    public void close() throws IOException {
        out.close();
    }

    public void flush() throws IOException {
        out.flush();
    }

    /**
     * Write limited bytes to the stream
     */
    public void write(byte[] b, int off, int len) throws IOException {
        long sleepTime = 0;
        long curTime = 0;
        if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }

        if (speed > 0 && !bstart) {
            startTime = System.currentTimeMillis();
            bstart = true;
        }
        long resetTime = System.currentTimeMillis();
        if (resetTime - startTime > 1000) {
            bytesWriteTotal = 0;
            startTime = resetTime;
        }
        out.write(b, off, len);
        if (len >= 0) {
            bytesWriteTotal += len;
            if (speed > 0) {
                curTime = System.currentTimeMillis();
                sleepTime = bytesWriteTotal / speed * 1000 - (curTime - startTime);
                if (sleepTime > 0) {
                    try {
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException ie) {
                        LOG.warn("Thread sleep is interrupted");
                    }
                }
            }
        }
    }

    byte[] oneByte = new byte[1];

    public void write(int b) throws IOException {
        oneByte[0] = (byte) (b & 0xff);
        write(oneByte, 0, oneByte.length);
    }
}
