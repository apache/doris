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
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class MysqlAuthPacketTest {
    private static final String OIDC_PLUGIN_NAME = "authentication_openid_connect_client";

    private ByteBuffer byteBuffer;

    @Before
    public void setUp() {
        MysqlSerializer serializer = MysqlSerializer.newInstance();

        // capability
        serializer.writeInt4(MysqlCapability.DEFAULT_CAPABILITY.getFlags());
        // max packet size
        serializer.writeInt4(1024000);
        // character set
        serializer.writeInt1(33);
        // reserved
        serializer.writeBytes(new byte[23]);
        // user name
        serializer.writeNulTerminateString("palo-user");
        // plugin data
        serializer.writeLenEncodedBytes("oidc-token".getBytes(StandardCharsets.UTF_8));
        // database
        serializer.writeNulTerminateString("testDb");
        // plugin name
        serializer.writeNulTerminateString(OIDC_PLUGIN_NAME);
        // connect attrs
        MysqlSerializer attrsSerializer = MysqlSerializer.newInstance();
        attrsSerializer.writeLenEncodedString("_client_name");
        attrsSerializer.writeLenEncodedString("mysql");
        attrsSerializer.writeLenEncodedString("_client_version");
        attrsSerializer.writeLenEncodedString("9.2.0");
        byte[] attrs = attrsSerializer.toArray();
        serializer.writeVInt(attrs.length);
        serializer.writeBytes(attrs);

        byteBuffer = serializer.toByteBuffer();
    }

    @Test
    public void testRead() {
        MysqlAuthPacket packet = new MysqlAuthPacket();
        Assert.assertTrue(packet.readFrom(byteBuffer));
        Assert.assertEquals("palo-user", packet.getUser());
        Assert.assertEquals("testDb", packet.getDb());
        Assert.assertEquals("oidc-token", new String(packet.getAuthResponse(), StandardCharsets.UTF_8));
        Assert.assertEquals(OIDC_PLUGIN_NAME, packet.getPluginName());
        Assert.assertEquals("mysql", packet.getConnectAttributes().get("_client_name"));
        Assert.assertEquals("9.2.0", packet.getConnectAttributes().get("_client_version"));
    }
}
