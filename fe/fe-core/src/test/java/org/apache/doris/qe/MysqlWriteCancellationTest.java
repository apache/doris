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

import org.apache.doris.common.ConnectionException;
import org.apache.doris.common.Status;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.protocol.MysqlProtocolAdapter;
import org.apache.doris.thrift.TStatusCode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

public class MysqlWriteCancellationTest {
    @Test
    public void testCommandClearsCancellationOnSuccessAndFailure() throws Exception {
        for (boolean fail : new boolean[] {false, true}) {
            MysqlChannel channel = Mockito.mock(MysqlChannel.class);
            Mockito.when(channel.fetchOnePacket())
                    .thenReturn(ByteBuffer.wrap(new byte[] {14 /* COM_PING */}));
            ConnectContext context = new ConnectContext(new MysqlProtocolAdapter(channel));
            context.getSessionVariable().setWorkloadGroup("normal");
            StmtExecutor cancelled = Mockito.mock(StmtExecutor.class);
            Status reason = new Status(TStatusCode.CANCELLED, "previous command cancelled");
            Mockito.when(cancelled.getPendingCancelReason()).thenReturn(reason);
            MysqlConnectProcessor processor = new MysqlConnectProcessor(context) {
                @Override
                public void finalizeCommand() throws IOException {
                    executor = cancelled;
                    if (fail) {
                        throw new ConnectionException("write failed");
                    }
                }
            };
            if (fail) {
                Assertions.assertThrows(ConnectionException.class, processor::processOnce);
            } else {
                processor.processOnce();
            }
            Mockito.verify(channel).setQueryCancelReason(Mockito.<Supplier<Status>>any());
            Mockito.verify(channel).clearQueryCancelReason();
        }
    }
}
