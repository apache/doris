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
package org.apache.doris.mysql.nio;

import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectProcessor;

import org.xnio.StreamConnection;

import java.io.IOException;

/**
 * connect context based on nio.
 */
public class NConnectContext extends ConnectContext {
    protected NMysqlChannel mysqlChannel;

    public NConnectContext(StreamConnection connection) {
        super();
        mysqlChannel = new NMysqlChannel(connection);
    }

    @Override
    public void cleanup() {
        mysqlChannel.close();
        returnRows = 0;
    }

    @Override
    public NMysqlChannel getMysqlChannel() {
        return mysqlChannel;
    }

    public void startAcceptQuery(ConnectProcessor connectProcessor) {
        mysqlChannel.startAcceptQuery(this, connectProcessor);
    }

    public void suspendAcceptQuery() {
        mysqlChannel.suspendAcceptQuery();
    }

    public void resumeAcceptQuery() {
        mysqlChannel.resumeAcceptQuery();
    }

    public void stopAcceptQuery() throws IOException {
        mysqlChannel.stopAcceptQuery();
    }

    @Override
    public String toString() {
        return "[remote ip: " + mysqlChannel.getRemoteIp() + "]";
    }
}

