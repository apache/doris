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
import org.apache.paimon.io.DataInputDeserializer;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
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
    public void testEncodedMessageRoundTripsWithPaimonSerializer() throws Exception {
        CommitMessage expected = commitMessage("data-file");
        byte[] payload = new PaimonCommitCodec().encode(Collections.singletonList(expected))[0];
        int version = ByteBuffer.wrap(payload, 4, 4).getInt();
        int length = ByteBuffer.wrap(payload, 8, 4).getInt();
        byte[] serialized = java.util.Arrays.copyOfRange(
                payload, PaimonCommitCodec.HEADER_BYTES, payload.length);

        List<CommitMessage> decoded = new CommitMessageSerializer()
                .deserializeList(version, new DataInputDeserializer(serialized));

        Assertions.assertEquals(serialized.length, length);
        Assertions.assertEquals(Collections.singletonList(expected), decoded);
    }

    @Test
    public void testCodecRoundTrip() throws Exception {
        CommitMessage expected = commitMessage("data-file");
        PaimonCommitCodec codec = new PaimonCommitCodec();

        List<CommitMessage> decoded = codec.decode(
                codec.encode(Collections.singletonList(expected)));

        Assertions.assertEquals(Collections.singletonList(expected), decoded);
    }

    @Test
    public void testDecodeRejectsMalformedFrame() {
        PaimonCommitCodec codec = new PaimonCommitCodec();

        Assertions.assertThrows(Exception.class,
                () -> codec.decode(new byte[][] {new byte[] {'D', 'P', 'C', 'M'}}));
        Assertions.assertThrows(Exception.class,
                () -> codec.decode(new byte[][] {PaimonCommitCodec.frame(new byte[0], 1)}));
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
