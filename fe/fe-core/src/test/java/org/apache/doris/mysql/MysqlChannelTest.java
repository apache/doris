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


import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.qe.ConnectContext;

import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;
import org.xnio.StreamConnection;

import java.io.IOException;
import java.nio.ByteBuffer;

public class MysqlChannelTest {

    @Mocked
    StreamConnection streamConnection;

    @Test
    public void testSendAfterException() throws IOException {
        // Mock.
        new Expectations() {
            {
                streamConnection.getSinkChannel().write((ByteBuffer) any);
                // The first call to `write()` throws IOException.
                result = new IOException();
                // The second call to `write()` executes normally.
                result = new Delegate() {
                    int fakeRead(ByteBuffer buffer) {
                        int writeLen = buffer.remaining();
                        buffer.position(buffer.limit());
                        return writeLen;
                    }
                };

                streamConnection.getSinkChannel().flush();
                result = true;
            }
        };

        ConnectContext ctx = new ConnectContext(streamConnection);
        MysqlChannel mysqlChannel = new MysqlChannel(streamConnection, ctx);
        Deencapsulation.setField(mysqlChannel, "sendBuffer", ByteBuffer.allocate(5));
        // The first call to `realNetSend()` in `flush()` throws IOException.
        // If `flush()` doesn't consider this exception, `sendBuffer` won't be reset to write mode,
        // which will cause BufferOverflowException at the next calling `sendOnePacket()`.
        ByteBuffer buf = ByteBuffer.allocate(12);
        buf.putInt(1);
        buf.putInt(2);
        // limit=8
        buf.flip();
        try {
            mysqlChannel.sendOnePacket(buf);
            Assert.fail();
        } catch (IOException ignore) {
            // do nothing
        }
        buf.clear();

        buf.putInt(1);
        // limit=4
        buf.flip();
        mysqlChannel.sendOnePacket(buf);
        buf.clear();

        buf.putInt(1);
        buf.putInt(2);
        // limit=8
        buf.flip();
        mysqlChannel.sendOnePacket(buf);
    }
}
