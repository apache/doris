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

public class MysqlCapabilityTest {
    @Test
    public void testToString() {
        MysqlCapability capability = new MysqlCapability(1);
        Assert.assertEquals("CLIENT_LONG_PASSWORD", capability.toString());

        capability = new MysqlCapability(0x3);
        Assert.assertEquals("CLIENT_LONG_PASSWORD | CLIENT_FOUND_ROWS", capability.toString());

        capability = new MysqlCapability(0xfffffff);
        Assert.assertEquals("CLIENT_LONG_PASSWORD | CLIENT_FOUND_ROWS | CLIENT_LONG_FLAG | CLIENT_CONNECT_WITH_DB"
                + " | CLIENT_NO_SCHEMA | CLIENT_COMPRESS | CLIENT_ODBC | CLIENT_LOCAL_FILES"
                + " | CLIENT_IGNORE_SPACE | CLIENT_PROTOCOL_41 | CLIENT_INTERACTIVE | CLIENT_SSL"
                + " | CLIENT_IGNORE_SIGPIPE | CLIENT_TRANSACTIONS | CLIENT_RESERVED | CLIENT_SECURE_CONNECTION"
                + " | CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS | CLIENT_PS_MULTI_RESULTS | CLIENT_PLUGIN_AUTH"
                + " | CLIENT_CONNECT_ATTRS | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA"
                + " | CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS | CLIENT_SESSION_TRACK | CLIENT_DEPRECATE_EOF",
                capability.toString());
    }

    @Test
    public void testDefaultFlags() {
        MysqlCapability capability = MysqlCapability.DEFAULT_CAPABILITY;
        Assert.assertEquals("CLIENT_LONG_FLAG | CLIENT_CONNECT_WITH_DB | CLIENT_LOCAL_FILES | CLIENT_PROTOCOL_41"
                + " | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH",
                capability.toString());
        Assert.assertTrue(capability.supportClientLocalFile());
    }
}
