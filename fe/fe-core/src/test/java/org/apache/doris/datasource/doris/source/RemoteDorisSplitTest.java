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

package org.apache.doris.datasource.doris.source;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

public class RemoteDorisSplitTest {
    @Test
    public void sameLocationWithDifferentTicketsHasDifferentIdentity() {
        RemoteDorisSplit first = new RemoteDorisSplit("grpc://proxy:8815", ByteBuffer.wrap(new byte[] {1, 2}));
        RemoteDorisSplit second = new RemoteDorisSplit("grpc://proxy:8815", ByteBuffer.wrap(new byte[] {1, 3}));
        RemoteDorisSplit sameWork = new RemoteDorisSplit("grpc://proxy:8815", ByteBuffer.wrap(new byte[] {1, 2}));
        RemoteDorisSplit otherLocation = new RemoteDorisSplit("grpc://other:8815", ByteBuffer.wrap(new byte[] {1, 2}));
        Assertions.assertNotEquals(first.getSplitIdentity(), second.getSplitIdentity());
        Assertions.assertEquals(first.getSplitIdentity(), sameWork.getSplitIdentity());
        Assertions.assertNotEquals(first.getSplitIdentity(), otherLocation.getSplitIdentity());
    }

    @Test
    public void identityCapturePreservesExecutionBufferAndRemainsImmutable() {
        ByteBuffer ticket = ByteBuffer.wrap(new byte[] {9, 1, 2, 8});
        ticket.position(1);
        ticket.limit(3);
        ticket.mark();
        RemoteDorisSplit split = new RemoteDorisSplit("grpc://proxy:8815", ticket);
        String identity = split.getSplitIdentity();
        Assertions.assertSame(ticket, split.getTicket());
        Assertions.assertEquals(1, ticket.position());
        Assertions.assertEquals(3, ticket.limit());
        Assertions.assertEquals(identity,
                new RemoteDorisSplit("grpc://proxy:8815", ByteBuffer.wrap(new byte[] {1, 2})).getSplitIdentity());
        ticket.reset();
        ticket.put(1, (byte) 7);
        ticket.position(2);
        Assertions.assertEquals(identity, split.getSplitIdentity());
        Assertions.assertEquals(2, ticket.position());
        Assertions.assertEquals(3, ticket.limit());
    }

    @Test
    public void directReadOnlyTicketUsesTheRemainingBytes() {
        ByteBuffer direct = ByteBuffer.allocateDirect(4);
        direct.put(new byte[] {9, 1, 2, 8});
        direct.flip();
        direct.position(1);
        direct.limit(3);
        ByteBuffer ticket = direct.asReadOnlyBuffer();
        RemoteDorisSplit split = new RemoteDorisSplit("grpc://proxy:8815", ticket);
        Assertions.assertEquals(1, ticket.position());
        Assertions.assertEquals(3, ticket.limit());
        Assertions.assertEquals(split.getSplitIdentity(),
                new RemoteDorisSplit("grpc://proxy:8815", ByteBuffer.wrap(new byte[] {1, 2})).getSplitIdentity());
    }
}
