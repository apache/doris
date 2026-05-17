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

import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletInputStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

public class StreamLoadRedirectDrainUtilTest {

    @Test
    public void testDrainRequestBodyWithinMaxBytes() {
        StreamLoadRedirectDrainUtil.DrainResult drainResult =
                StreamLoadRedirectDrainUtil.drainRequestBodyAfterRedirect(
                        new QueueAvailableServletInputStream("hello".getBytes(), 5, 0, 0, 0), 16);

        Assertions.assertEquals(5, drainResult.getDrainedBytes());
        Assertions.assertEquals(StreamLoadRedirectDrainUtil.ExitReason.EOF, drainResult.getExitReason());
    }

    @Test
    // Verify delayed body chunks are still drained when they arrive within the bounded idle window.
    public void testDrainRequestBodyAllowsDelayedArrivalWithinIdleWindow() {
        StreamLoadRedirectDrainUtil.DrainResult drainResult =
                StreamLoadRedirectDrainUtil.drainRequestBodyAfterRedirect(
                        new QueueAvailableServletInputStream("hello".getBytes(), 0, 0, 0, 0, 0, 5), 16);

        Assertions.assertEquals(5, drainResult.getDrainedBytes());
        Assertions.assertEquals(StreamLoadRedirectDrainUtil.ExitReason.EOF, drainResult.getExitReason());
    }

    @Test
    public void testDrainRequestBodyStopsAtMaxBytes() {
        StreamLoadRedirectDrainUtil.DrainResult drainResult =
                StreamLoadRedirectDrainUtil.drainRequestBodyAfterRedirect(
                        new QueueAvailableServletInputStream("hello world".getBytes(), 11), 5);

        Assertions.assertEquals(5, drainResult.getDrainedBytes());
        Assertions.assertEquals(StreamLoadRedirectDrainUtil.ExitReason.MAX_BYTES, drainResult.getExitReason());
    }

    @Test
    public void testDrainRequestBodyIdleTimeout() {
        StreamLoadRedirectDrainUtil.DrainResult drainResult =
                StreamLoadRedirectDrainUtil.drainRequestBodyAfterRedirect(new NeverReadyServletInputStream(), 8);

        Assertions.assertEquals(0, drainResult.getDrainedBytes());
        Assertions.assertEquals(StreamLoadRedirectDrainUtil.ExitReason.IDLE_TIMEOUT, drainResult.getExitReason());
    }

    @Test
    public void testDrainRequestBodyReadError() {
        StreamLoadRedirectDrainUtil.DrainResult drainResult =
                StreamLoadRedirectDrainUtil.drainRequestBodyAfterRedirect(new ErrorServletInputStream(), 8);

        Assertions.assertEquals(0, drainResult.getDrainedBytes());
        Assertions.assertEquals(StreamLoadRedirectDrainUtil.ExitReason.ERROR, drainResult.getExitReason());
    }

    @Test
    public void testDrainRequestBodyEof() {
        StreamLoadRedirectDrainUtil.DrainResult drainResult =
                StreamLoadRedirectDrainUtil.drainRequestBodyAfterRedirect(new EofServletInputStream(), 8);

        Assertions.assertEquals(0, drainResult.getDrainedBytes());
        Assertions.assertEquals(StreamLoadRedirectDrainUtil.ExitReason.EOF, drainResult.getExitReason());
    }

    private static class QueueAvailableServletInputStream extends ServletInputStream {
        private final byte[] data;
        private final Queue<Integer> availableValues = new ArrayDeque<>();
        private int offset = 0;

        QueueAvailableServletInputStream(byte[] data, int... availableValues) {
            this.data = data;
            for (int availableValue : availableValues) {
                this.availableValues.add(availableValue);
            }
        }

        @Override
        public int read() {
            if (offset >= data.length) {
                return -1;
            }
            return data[offset++] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (offset >= data.length) {
                return -1;
            }
            int readBytes = Math.min(len, data.length - offset);
            System.arraycopy(data, offset, b, off, readBytes);
            offset += readBytes;
            return readBytes;
        }

        @Override
        public int available() {
            if (!availableValues.isEmpty()) {
                return availableValues.poll();
            }
            return Math.max(0, data.length - offset);
        }

        @Override
        public boolean isFinished() {
            return offset >= data.length;
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setReadListener(ReadListener readListener) {
        }
    }

    private static class ErrorServletInputStream extends ServletInputStream {
        @Override
        public int read() throws IOException {
            throw new IOException("read error");
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            throw new IOException("read error");
        }

        @Override
        public int available() {
            return 1;
        }

        @Override
        public boolean isFinished() {
            return false;
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setReadListener(ReadListener readListener) {
        }
    }

    private static class EofServletInputStream extends ServletInputStream {
        @Override
        public int read() {
            return -1;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            return -1;
        }

        @Override
        public int available() {
            return 1;
        }

        @Override
        public boolean isFinished() {
            return true;
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setReadListener(ReadListener readListener) {
        }
    }

    // Keep reporting no readable bytes without reaching EOF to simulate a stalled client.
    private static class NeverReadyServletInputStream extends ServletInputStream {
        @Override
        public int read() {
            return -1;
        }

        @Override
        public int available() {
            return 0;
        }

        @Override
        public boolean isFinished() {
            return false;
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setReadListener(ReadListener readListener) {
        }
    }
}
