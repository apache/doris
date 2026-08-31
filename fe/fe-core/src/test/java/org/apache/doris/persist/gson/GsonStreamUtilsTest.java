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

package org.apache.doris.persist.gson;

import org.apache.doris.common.io.Text;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class GsonStreamUtilsTest {
    private static final int CHUNK_SIZE = 64 * 1024;

    /**
     * A JSON value larger than one chunk is split into bounded chunks and can round-trip.
     * Chinese characters and emoji exercise multi-byte UTF-8 and UTF-16 surrogate-pair boundaries.
     */
    @Test
    public void testChunkedJsonRoundTrip() throws Exception {
        TestData expected = new TestData(Strings.repeat("测试🙂", 100000));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            GsonStreamUtils.writeJson(out, expected);
        }

        int chunkCount = 0;
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            Assert.assertEquals(-1, in.readInt());
            int chunkLength;
            while ((chunkLength = in.readInt()) != 0) {
                Assert.assertTrue(chunkLength <= CHUNK_SIZE);
                Assert.assertEquals(chunkLength, in.skipBytes(chunkLength));
                chunkCount++;
            }
            Assert.assertEquals(0, in.available());
        }
        Assert.assertTrue(chunkCount > 1);

        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            TestData actual = GsonStreamUtils.readJson(in, TestData.class);
            Assert.assertEquals(expected.value, actual.value);
        }
    }

    /**
     * Chunk boundaries are byte-oriented and may safely split a multi-byte UTF-8 character.
     */
    @Test
    public void testOneByteChunksRoundTripUtf8() throws Exception {
        TestData expected = new TestData("测试🙂");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            GsonStreamUtils.writeJson(out, expected, 1);
        }

        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            Assert.assertEquals(-1, in.readInt());
            int chunkCount = 0;
            int chunkLength;
            while ((chunkLength = in.readInt()) != 0) {
                Assert.assertEquals(1, chunkLength);
                Assert.assertEquals(1, in.skipBytes(chunkLength));
                chunkCount++;
            }
            Assert.assertTrue(chunkCount > expected.value.length());
        }

        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            Assert.assertEquals(expected.value, GsonStreamUtils.readJson(in, TestData.class).value);
            Assert.assertEquals(0, in.available());
        }
    }

    /**
     * The terminator of one chunked JSON record is consumed before reading the next record.
     */
    @Test
    public void testSequentialChunkedJsonRoundTrip() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            GsonStreamUtils.writeJson(out, new TestData("first"));
            GsonStreamUtils.writeJson(out, new TestData("second"));
        }

        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            Assert.assertEquals("first", GsonStreamUtils.readJson(in, TestData.class).value);
            Assert.assertEquals("second", GsonStreamUtils.readJson(in, TestData.class).value);
            Assert.assertEquals(0, in.available());
        }
    }

    /**
     * Gson returning null still consumes the terminator and preserves the next record boundary.
     */
    @Test
    public void testNullDoesNotCorruptNextValue() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            GsonStreamUtils.writeJson(out, null);
            GsonStreamUtils.writeJson(out, new TestData("after-null"));
        }

        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            Assert.assertNull(GsonStreamUtils.readJson(in, TestData.class));
            Assert.assertEquals("after-null", GsonStreamUtils.readJson(in, TestData.class).value);
            Assert.assertEquals(0, in.available());
        }
    }

    /**
     * An I/O error raised during chunk body emission remains an IOException instead of a Gson wrapper.
     */
    @Test(expected = IOException.class)
    public void testWriteFailureRemainsIOException() throws Exception {
        OutputStream failingOutput = new OutputStream() {
            private int remaining = Integer.BYTES;

            @Override
            public void write(int value) throws IOException {
                if (remaining-- <= 0) {
                    throw new IOException("injected write failure");
                }
            }
        };
        GsonStreamUtils.writeJson(new DataOutputStream(failingOutput),
                new TestData(Strings.repeat("a", CHUNK_SIZE * 2)));
    }

    /**
     * A chunk whose declared length exceeds its payload reports an IOException.
     */
    @Test(expected = IOException.class)
    public void testTruncatedChunkRemainsIOException() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(-1);
            out.writeInt(10);
            out.writeBytes("{}");
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            GsonStreamUtils.readJson(in, TestData.class);
        }
    }

    /**
     * Reserved negative values cannot be interpreted as legacy lengths or chunked records.
     */
    @Test(expected = IOException.class)
    public void testInvalidRecordMarkerIsRejected() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(-2);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            GsonStreamUtils.readJson(in, TestData.class);
        }
    }

    /**
     * A negative chunk length is rejected instead of being treated as a terminator.
     */
    @Test(expected = IOException.class)
    public void testNegativeChunkLengthIsRejected() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(-1);
            out.writeInt(-1);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            GsonStreamUtils.readJson(in, TestData.class);
        }
    }

    /**
     * Malformed UTF-8 in a chunk is reported as an IOException.
     */
    @Test(expected = IOException.class)
    public void testMalformedUtf8IsRejected() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(-1);
            out.writeInt(1);
            out.writeByte(0x80);
            out.writeInt(0);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            GsonStreamUtils.readJson(in, TestData.class);
        }
    }

    /**
     * Malformed UTF-16 is rejected, matching the former Text.writeString encoding behavior.
     */
    @Test(expected = IOException.class)
    public void testInvalidSurrogateIsRejected() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            GsonStreamUtils.writeJson(out, new TestData("\uD800"));
        }
    }

    /**
     * Verifies that a legacy Text.writeString JSON record, including multi-byte UTF-8 text, remains readable.
     */
    @Test
    public void testReadLegacyTextJson() throws Exception {
        TestData expected = new TestData("legacy-兼容");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            Text.writeString(out, GsonUtils.GSON.toJson(expected));
        }

        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            TestData actual = GsonStreamUtils.readJson(in, TestData.class);
            Assert.assertEquals(expected.value, actual.value);
        }
    }

    /**
     * Legacy and chunked records can share a stream without losing record boundaries.
     */
    @Test
    public void testLegacyRecordFollowedByChunkedRecord() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            Text.writeString(out, GsonUtils.GSON.toJson(new TestData("legacy")));
            GsonStreamUtils.writeJson(out, new TestData("chunked"));
        }

        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            Assert.assertEquals("legacy", GsonStreamUtils.readJson(in, TestData.class).value);
            Assert.assertEquals("chunked", GsonStreamUtils.readJson(in, TestData.class).value);
            Assert.assertEquals(0, in.available());
        }
    }

    /**
     * A truncated legacy payload is reported as an IOException.
     */
    @Test(expected = IOException.class)
    public void testTruncatedLegacyJsonIsRejected() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(10);
            out.writeBytes("{}");
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            GsonStreamUtils.readJson(in, TestData.class);
        }
    }

    private static class TestData {
        @SerializedName("value")
        private String value;

        private TestData(String value) {
            this.value = value;
        }
    }
}
