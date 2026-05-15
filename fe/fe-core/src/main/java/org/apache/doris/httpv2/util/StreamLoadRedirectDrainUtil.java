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

package org.apache.doris.httpv2.util;

import com.google.common.base.Preconditions;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public final class StreamLoadRedirectDrainUtil {
    private static final Logger LOG = LogManager.getLogger(StreamLoadRedirectDrainUtil.class);

    private static final int BUFFER_SIZE = 8 * 1024;
    private static final int IDLE_SLEEP_MS = 5;
    private static final int MAX_IDLE_LOOPS = 3;

    private StreamLoadRedirectDrainUtil() {
    }

    public static DrainResult drainRequestBodyAfterRedirect(HttpServletRequest request, long maxBytes) {
        try {
            return drainRequestBodyAfterRedirect(request.getInputStream(), maxBytes);
        } catch (IOException e) {
            LOG.warn("failed to get request input stream for stream load redirect drain", e);
            return new DrainResult(0, 0, ExitReason.ERROR);
        }
    }

    static DrainResult drainRequestBodyAfterRedirect(ServletInputStream inputStream, long maxBytes) {
        Preconditions.checkArgument(maxBytes > 0, "maxBytes must be positive");

        long startNanos = System.nanoTime();
        long drainedBytes = 0;
        int idleLoops = 0;
        byte[] buffer = new byte[(int) Math.min(BUFFER_SIZE, maxBytes)];

        try {
            while (drainedBytes < maxBytes) {
                int availableBytes = inputStream.available();
                if (availableBytes <= 0) {
                    idleLoops++;
                    if (idleLoops >= MAX_IDLE_LOOPS) {
                        return new DrainResult(drainedBytes, elapsedMillis(startNanos), ExitReason.IDLE_TIMEOUT);
                    }
                    if (!sleepForIdleWindow()) {
                        return new DrainResult(drainedBytes, elapsedMillis(startNanos), ExitReason.ERROR);
                    }
                    continue;
                }

                idleLoops = 0;
                int readLimit = (int) Math.min(Math.min(maxBytes - drainedBytes, buffer.length), availableBytes);
                int readBytes = inputStream.read(buffer, 0, readLimit);
                if (readBytes < 0) {
                    return new DrainResult(drainedBytes, elapsedMillis(startNanos), ExitReason.EOF);
                }
                if (readBytes == 0) {
                    continue;
                }
                drainedBytes += readBytes;
            }
            return new DrainResult(drainedBytes, elapsedMillis(startNanos), ExitReason.MAX_BYTES);
        } catch (IOException e) {
            LOG.warn("failed while draining request body after stream load redirect", e);
            return new DrainResult(drainedBytes, elapsedMillis(startNanos), ExitReason.ERROR);
        }
    }

    private static boolean sleepForIdleWindow() {
        try {
            Thread.sleep(IDLE_SLEEP_MS);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static long elapsedMillis(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }

    public enum ExitReason {
        EOF,
        MAX_BYTES,
        IDLE_TIMEOUT,
        ERROR
    }

    public static final class DrainResult {
        private final long drainedBytes;
        private final long elapsedMillis;
        private final ExitReason exitReason;

        public DrainResult(long drainedBytes, long elapsedMillis, ExitReason exitReason) {
            this.drainedBytes = drainedBytes;
            this.elapsedMillis = elapsedMillis;
            this.exitReason = exitReason;
        }

        public long getDrainedBytes() {
            return drainedBytes;
        }

        public long getElapsedMillis() {
            return elapsedMillis;
        }

        public ExitReason getExitReason() {
            return exitReason;
        }
    }
}
