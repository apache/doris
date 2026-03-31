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

package org.apache.doris.datasource.paimon;

import mockit.Mocked;
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class PaimonTransactionTest {

    @Test
    public void testCommitWithoutMessagesNoop(@Mocked PaimonMetadataOps ops) throws Exception {
        PaimonTransaction transaction = new PaimonTransaction(ops);
        transaction.commit();
    }

    @Test
    public void testDeserializePrefixedEmptyPayload() throws Exception {
        CommitMessageSerializer serializer = new CommitMessageSerializer();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(output);
        serializer.serializeList(Collections.emptyList(), view);
        byte[] raw = output.toByteArray();
        ByteBuffer buffer = ByteBuffer.allocate(12 + raw.length);
        buffer.put((byte) 'D').put((byte) 'P').put((byte) 'C').put((byte) 'M');
        buffer.putInt(serializer.getVersion());
        buffer.putInt(raw.length);
        buffer.put(raw);

        List<CommitMessage> messages = deserialize(buffer.array());
        Assert.assertTrue(messages.isEmpty());
    }

    @Test
    public void testDeserializeInvalidPayloadThrows() throws Exception {
        try {
            deserialize(new byte[] {1, 2, 3, 4});
            Assert.fail("expected IOException");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("Failed to deserialize paimon commit message payload"));
        }
    }

    @SuppressWarnings("unchecked")
    private List<CommitMessage> deserialize(byte[] payload) throws Exception {
        Method method = PaimonTransaction.class.getDeclaredMethod("deserializeCommitMessagePayload", byte[].class);
        method.setAccessible(true);
        try {
            return (List<CommitMessage>) method.invoke(null, payload);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw e;
        }
    }
}
