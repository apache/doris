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

import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * DummyMysqlChannel do nothing, just for making unit test happy.
 * So that in unit test, we don't need to check if serializer is null.
 * And don't need to allocate a real ByteBuffer.
 */
public class DummyMysqlChannel extends MysqlChannel {

    public void setSequenceId(int sequenceId) {
        this.sequenceId = sequenceId;
    }

    @Override
    public String getRemoteIp() {
        return "";
    }

    @Override
    public String getRemoteHostPortString() {
        return "";
    }

    @Override
    public void close() {
    }

    @Override
    protected int readAll(ByteBuffer dstBuf, boolean isHeader) throws IOException {
        return 0;
    }

    @Override
    public ByteBuffer fetchOnePacket() throws IOException {
        return ByteBuffer.allocate(0);
    }

    @Override
    public void flush() throws IOException {
    }

    @Override
    public void sendOnePacket(ByteBuffer packet) throws IOException {
    }

    @Override
    public void sendAndFlush(ByteBuffer packet) throws IOException {
    }

    @Override
    public void reset() {
    }

    public MysqlSerializer getSerializer() {
        return serializer;
    }
}
