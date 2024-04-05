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

package org.apache.doris.qe;

import org.apache.doris.mysql.BytesChannel;
import org.apache.doris.mysql.ProxyProtocolHandler;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;

public class ProxyProtocolHandlerTest {

    public static class TestChannel implements BytesChannel {
        private byte[] data;
        private int pos;

        public TestChannel(byte[] data) {
            this.data = data;
            this.pos = 0;
        }

        @Override
        public int read(java.nio.ByteBuffer buffer) {
            int len = Math.min(buffer.remaining(), data.length - pos);
            if (len > 0) {
                buffer.put(data, pos, len);
                pos += len;
            }
            return len;
        }
    }

    private TestChannel testChannel;

    @Test
    public void handleV1ProtocolWithValidData() throws IOException {
        byte[] data = "PROXY TCP4 192.168.0.1 192.168.0.2 12345 54321\r\n".getBytes();
        testChannel = new TestChannel(data);
        ProxyProtocolHandler.ProxyProtocolResult result = ProxyProtocolHandler.handle(testChannel);
        Assertions.assertNotNull(result);
        Assertions.assertFalse(result.isUnknown);
        Assertions.assertEquals("192.168.0.1", result.sourceIP);
        Assertions.assertEquals(12345, result.sourcePort);
        Assertions.assertEquals("192.168.0.2", result.destIp);
        Assertions.assertEquals(54321, result.destPort);
    }

    @Test
    public void handleV1ProtocolWithUnknown() throws IOException {
        byte[] data = "PROXY UNKNOWN xxxxxxxxxxxxxxxxxx\r\n".getBytes();
        testChannel = new TestChannel(data);
        ProxyProtocolHandler.ProxyProtocolResult result = ProxyProtocolHandler.handle(testChannel);
        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.isUnknown);
    }

    @Test(expected = IOException.class)
    public void handleV1ProtocolWithInvalidProtocol() throws IOException {
        byte[] data = "PROXY TCP7 xxx\r\n".getBytes();
        testChannel = new TestChannel(data);
        ProxyProtocolHandler.handle(testChannel);
    }

    @Test(expected = IOException.class)
    public void handleV1ProtocolWithInvalidData() throws IOException {
        byte[] data = "INVALID DATA".getBytes();
        testChannel = new TestChannel(data);
        ProxyProtocolHandler.handle(testChannel);
    }

    @Test(expected = IOException.class)
    public void handleV1ProtocolWithIncompleteData() throws IOException {
        byte[] data = "PROXY TCP4 192.168.0.1 192.168.0.2 12345".getBytes();
        testChannel = new TestChannel(data);
        ProxyProtocolHandler.handle(testChannel);
    }

    @Test(expected = IOException.class)
    public void handleV1ProtocolWithExtraData() throws IOException {
        byte[] data = "PROXY TCP4 192.168.0.1 192.168.0.2 12345 54321 EXTRA DATA\r\n".getBytes();
        testChannel = new TestChannel(data);
        ProxyProtocolHandler.handle(testChannel);
    }

    @Test
    public void handleV1ProtocolWithValidIPv6Data() throws IOException {
        byte[] data = "PROXY TCP6 2001:db8:0:1:1:1:1:1 2001:db8:0:1:1:1:1:2 12345 54321\r\n".getBytes();
        testChannel = new TestChannel(data);
        ProxyProtocolHandler.ProxyProtocolResult result = ProxyProtocolHandler.handle(testChannel);
        Assertions.assertNotNull(result);
        Assertions.assertFalse(result.isUnknown);
        Assertions.assertEquals("2001:db8:0:1:1:1:1:1", result.sourceIP);
        Assertions.assertEquals(12345, result.sourcePort);
        Assertions.assertEquals("2001:db8:0:1:1:1:1:2", result.destIp);
        Assertions.assertEquals(54321, result.destPort);
    }

    @Test(expected = IOException.class)
    public void handleV1ProtocolWithInvalidIPv6Data() throws IOException {
        byte[] data = "PROXY TCP6 2001:db8:0:1:1:1:1:1 2001:db8:0:1:1:1:1:2 12345 EXTRA DATA\r\n".getBytes();
        testChannel = new TestChannel(data);
        ProxyProtocolHandler.handle(testChannel);
    }

    @Test(expected = IOException.class)
    public void handleV1ProtocolWithIncompleteIPv6Data() throws IOException {
        byte[] data = "PROXY TCP6 2001:db8:0:1:1:1:1:1 2001:db8:0:1:1:1:1:2 12345".getBytes();
        testChannel = new TestChannel(data);
        ProxyProtocolHandler.handle(testChannel);
    }

    @Test(expected = IOException.class)
    public void handleV2Protocol() throws IOException {
        byte[] data = new byte[] {0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A};
        testChannel = new TestChannel(data);
        ProxyProtocolHandler.handle(testChannel);
    }
}
