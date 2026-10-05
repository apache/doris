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

public class MysqlAuthSwitchPacket extends MysqlPacket {
    private static final int STATUS = 0xfe;
    private static final String AUTH_PLUGIN_NAME = "mysql_clear_password";
    private static final String DATA = "";

    @Override
    public void writeTo(MysqlSerializer serializer) {
        serializer.writeInt1(STATUS);
        serializer.writeNulTerminateString(AUTH_PLUGIN_NAME);
        serializer.writeNulTerminateString(DATA);
    }
}
