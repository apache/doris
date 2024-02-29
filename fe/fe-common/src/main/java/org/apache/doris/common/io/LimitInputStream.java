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
import java.io.InputStream;

/**
 * A LimitInputStream that the InputStream is limited .
 */
public class LimitInputStream extends InputStream {
    private static final Logger LOG = LoggerFactory.getLogger(LimitInputStream.class);

    /**
     * The input stream to be limited.
     */
    protected final InputStream in;
    protected final int speed;
    protected long bytesReadTotal;
    protected long startTime;
    protected boolean bstart;

    /**
     * An input stream that reads the limited bytes to the given stream.
     *
     * @param in
     *            The stream to be limited
     * @param limitspeed
     *            The limit of speed
     */
    public LimitInputStream(InputStream in, int limitspeed) throws IOException {
        if (in == null) {
            throw new IOException("InputStream is null");
        }
        speed = limitspeed;
        if (LOG.isDebugEnabled()) {
            LOG.debug("LimitinputStream limit speed: {}", speed);
        }
        this.in = in;
        bytesReadTotal = 0;
        bstart = false;
    }

    public void close() throws IOException {
        in.close();
    }

    /**
     * Read limited bytes from the stream.
     */
    public int read(byte[] b, int off, int len) throws IOException {
        long sleepTime = 0;
        long curTime = 0;
        int bytesRead = -1;
        IOException ioe = null;
        try {
            if (speed > 0 && !bstart) {
                startTime = System.currentTimeMillis();
                bstart = true;
            }
            long resetTime = System.currentTimeMillis();
            if (resetTime - startTime > 1000) {
                bytesReadTotal = 0;
                startTime = resetTime;
            }
            bytesRead = in.read(b, off, len);
            if (bytesRead >= 0) {
                bytesReadTotal += bytesRead;
                if (speed > 0) {
                    curTime = System.currentTimeMillis();
                    sleepTime = bytesReadTotal / speed * 1000
                            - (curTime - startTime);
                    if (sleepTime > 0) {
                        try {
                            Thread.sleep(sleepTime);
                        } catch (InterruptedException ie) {
                            LOG.warn("Thread sleep is interrupted");
                        }
                    }
                }
            }
        } catch (IOException ex) {
            ioe = ex;
        } finally {
            if (bytesRead == -1 && ioe != null) {
                throw ioe;
            }
            return bytesRead;
        }

    }

    byte[] oneByte = new byte[1];

    public int read() throws IOException {
        return (read(oneByte, 0, oneByte.length) == -1) ? -1 : (oneByte[0] & 0xff);
    }
}
