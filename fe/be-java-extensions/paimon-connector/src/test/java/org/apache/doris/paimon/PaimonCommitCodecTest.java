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

package org.apache.doris.paimon;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PaimonCommitCodecTest {

    @Test
    public void testFrameContainsMagicVersionAndLength() {
        byte[] data = new byte[] {1, 2, 3};

        byte[] payload = PaimonCommitCodec.frame(data, 7);

        Assertions.assertArrayEquals(new byte[] {'D', 'P', 'C', 'M'},
                new byte[] {payload[0], payload[1], payload[2], payload[3]});
        Assertions.assertEquals(7, ByteBuffer.wrap(payload, 4, 4).getInt());
        Assertions.assertEquals(data.length, ByteBuffer.wrap(payload, 8, 4).getInt());
        Assertions.assertArrayEquals(data,
                java.util.Arrays.copyOfRange(payload, PaimonCommitCodec.HEADER_BYTES, payload.length));
    }

    @Test
    public void testEncodeEmptyMessages() throws Exception {
        PaimonCommitCodec codec = new PaimonCommitCodec();

        Assertions.assertEquals(0, codec.encode(Collections.emptyList()).length);
    }

    @Test
    public void testRejectOversizedSinglePayload() {
        PaimonCommitCodec codec = new PaimonCommitCodec(1024, 1);

        Exception exception = Assertions.assertThrows(
                Exception.class,
                () -> codec.encode(Collections.singletonList(
                        commitMessage("x".repeat(2048)))));

        Assertions.assertTrue(exception.getMessage().contains("exceeds"));
    }

    @Test
    public void testAdaptiveChunkingStaysWithinFramedLimit() throws Exception {
        PaimonCommitCodec codec = new PaimonCommitCodec(1024, 2);
        List<CommitMessage> messages = new ArrayList<>();
        messages.add(commitMessage("x".repeat(400)));
        messages.add(commitMessage("y".repeat(400)));

        byte[][] payloads = codec.encode(messages);

        Assertions.assertEquals(2, payloads.length);
        Assertions.assertTrue(payloads[0].length <= 1024);
        Assertions.assertTrue(payloads[1].length <= 1024);
    }

    @Test
    public void testRejectTotalPayloadMemoryLimit() throws Exception {
        PaimonCommitCodec codec = new PaimonCommitCodec(1024, 1);
        List<CommitMessage> messages = new ArrayList<>();
        messages.add(commitMessage("x".repeat(400)));
        messages.add(commitMessage("y".repeat(400)));
        byte[][] encoded = codec.encode(messages);
        long totalBytes = encoded[0].length + encoded[1].length;

        Exception exception = Assertions.assertThrows(
                PaimonCommitCodec.CommitPayloadMemoryException.class,
                () -> codec.encode(messages, totalBytes - 1));

        Assertions.assertTrue(exception.getMessage().contains("total memory limit"));
    }

    @Test
    public void testTotalLimitIncludesSerializationBackingAndFinalCopy() throws Exception {
        PaimonCommitCodec codec = new PaimonCommitCodec(1024, 1);
        List<CommitMessage> messages = Collections.singletonList(commitMessage("x".repeat(400)));
        int payloadBytes = codec.encode(messages)[0].length;

        Assertions.assertThrows(
                PaimonCommitCodec.CommitPayloadMemoryException.class,
                () -> codec.encode(messages, 2L * payloadBytes - 1));
        Assertions.assertEquals(payloadBytes, codec.encode(messages, 2L * payloadBytes)[0].length);
    }

    private static CommitMessage commitMessage(String fileName) {
        DataFileMeta dataFile = DataFileMeta.forAppend(
                fileName,
                1,
                1,
                SimpleStats.EMPTY_STATS,
                0,
                0,
                0,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                null,
                null);
        return new CommitMessageImpl(
                BinaryRow.EMPTY_ROW,
                0,
                1,
                new DataIncrement(
                        Collections.singletonList(dataFile),
                        Collections.emptyList(),
                        Collections.emptyList()),
                CompactIncrement.emptyIncrement());
    }
}
