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

package org.apache.doris.mysql;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ProxyProtocolTest {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(ProxyProtocolTest.class);

    @Test
    public void testProcessProtocolHeaderV1() throws Exception {
        // test for v4
        ProxyProtocol.ConnectInfo connInfo = new ProxyProtocol.ConnectInfo();
        ByteBuffer buffV4 = ByteBuffer.allocate(108);
        buffV4.put("PROXY TCP4 255.255.255.255 255.255.255.255 65535 65535".getBytes());
        buffV4.put((byte) 0x0D);
        buffV4.put((byte) 0x0A);
        ProxyProtocol.processProtocolHeaderV1(buffV4, connInfo);
        Assert.assertTrue(connInfo.sourceAddress.getHostAddress().compareToIgnoreCase("255.255.255.255") == 0);
        Assert.assertTrue(connInfo.destAddress.getHostAddress().compareToIgnoreCase("255.255.255.255") == 0);
        Assert.assertTrue(connInfo.destPort == 65535);
        Assert.assertTrue(connInfo.sourcePort == 65535);

        // test for v6
        ByteBuffer buffV6 = ByteBuffer.allocate(1000);
        buffV6.put("PROXY TCP6 2001:db8:86a3:8d3:1319:8a2e:370:7344 2001:db8:86a3:8d3:1319:8a2e:370:7324 65534 65535".getBytes());
        buffV6.put((byte) 0x0D);
        buffV6.put((byte) 0x0A);
        ProxyProtocol.processProtocolHeaderV1(buffV6, connInfo);
        Assert.assertTrue(connInfo.sourceAddress.getHostAddress().compareToIgnoreCase("2001:db8:86a3:8d3:1319:8a2e:370:7344") == 0);
        Assert.assertTrue(connInfo.destAddress.getHostAddress().compareToIgnoreCase("2001:db8:86a3:8d3:1319:8a2e:370:7324") == 0);
        Assert.assertTrue(connInfo.destPort == 65535);
        Assert.assertTrue(connInfo.sourcePort == 65534);
    }

    @Test(expected = IOException.class)
    public void testProcessProtocolHeaderV1_v4() throws Exception {
        ProxyProtocol.ConnectInfo connInfo = new ProxyProtocol.ConnectInfo();
        ByteBuffer buffV4 = ByteBuffer.allocate(1000);
        buffV4.put("PROXY TCP4 255.255.255.255 255.255.255.255 65535 65535".getBytes());
        buffV4.put((byte) 0x0A);
        ProxyProtocol.processProtocolHeaderV1(buffV4, connInfo);
        Assert.fail("No exception throws");
    }

    @Test(expected = IOException.class)
    public void testProcessProtocolHeaderV1_v6() throws Exception {
        ProxyProtocol.ConnectInfo connInfo = new ProxyProtocol.ConnectInfo();
        ByteBuffer buffV6 = ByteBuffer.allocate(1000);
        buffV6.put("PROXY TCP6 2001:0db8:86a3:08d3:1319:8a2e:0370:7344 2001:0db8:86a3:08d3:1319:8a2e:0370:7324 65534 65535".getBytes());
        buffV6.put((byte) 0x0A);
        ProxyProtocol.processProtocolHeaderV1(buffV6, connInfo);
        Assert.fail("No exception throws");
    }
}
