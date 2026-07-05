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

package org.apache.doris.transaction;

import org.apache.doris.datasource.hive.HMSTransaction;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TFileContent;
import org.apache.doris.thrift.THivePartitionUpdate;
import org.apache.doris.thrift.TIcebergCommitData;
import org.apache.doris.thrift.TMCCommitData;
import org.apache.doris.thrift.TUpdateMode;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Golden-equivalence tests for {@link CommitDataSerializer} and the write
 * transactions' {@code addCommitData} overrides (W-phase W3 / W6).
 *
 * <p>These pin the contract that the refactored hot path
 * (serialize each Thrift commit fragment with {@link TBinaryProtocol} &rarr;
 * {@link Transaction#addCommitData(byte[])} &rarr; deserialize &rarr; accumulate)
 * produces exactly the same accumulated commit state as the legacy
 * concrete-cast path (whole-list {@code updateXxxCommitData}).</p>
 *
 * <p>The serialization protocol is the red line: the producer
 * ({@link CommitDataSerializer}) and the consumers (each transaction's
 * {@code addCommitData}) must agree on {@link TBinaryProtocol}. A protocol
 * mismatch corrupts the round trip and fails these tests.</p>
 */
public class CommitDataSerializerTest {

    private ConnectContext connectContext;

    @Before
    public void setUp() {
        // HMSTransaction's constructor reads ConnectContext.get(); install one on the thread.
        connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
    }

    @After
    public void tearDown() {
        ConnectContext.remove();
        connectContext = null;
    }

    private static TMCCommitData mcData(String session, long rowCount, String commitMessage) {
        return new TMCCommitData()
                .setSessionId(session)
                .setRowCount(rowCount)
                .setWrittenBytes(rowCount * 8)
                .setCommitMessage(commitMessage);
    }

    private static THivePartitionUpdate hiveData(String name, long rowCount, String... fileNames) {
        return new THivePartitionUpdate()
                .setName(name)
                .setUpdateMode(TUpdateMode.APPEND)
                .setRowCount(rowCount)
                .setFileSize(rowCount * 16)
                .setFileNames(Arrays.asList(fileNames));
    }

    private static TIcebergCommitData icebergData(String filePath, long rowCount) {
        return new TIcebergCommitData()
                .setFilePath(filePath)
                .setRowCount(rowCount)
                .setFileSize(rowCount * 32)
                .setFileContent(TFileContent.DATA)
                .setPartitionValues(Arrays.asList("2026", "06"));
    }

    private static void assertBinaryRoundTrip(TBase<?, ?> original, TBase<?, ?> target)
            throws Exception {
        byte[] bytes = new TSerializer(new TBinaryProtocol.Factory()).serialize(original);
        new TDeserializer(new TBinaryProtocol.Factory()).deserialize(target, bytes);
        Assert.assertEquals(original, target);
    }

    /**
     * The serialization protocol is binary and lossless for every field of each
     * commit-payload struct. This is the contract {@link CommitDataSerializer} and
     * the {@code addCommitData} overrides both depend on.
     */
    @Test
    public void binaryProtocolRoundTripIsLossless() throws Exception {
        assertBinaryRoundTrip(mcData("session-1", 42L, "bWMtcGF5bG9hZA=="), new TMCCommitData());
        assertBinaryRoundTrip(hiveData("dt=2026-06-06", 7L, "f1", "f2"), new THivePartitionUpdate());
        assertBinaryRoundTrip(icebergData("s3://b/data/0.parquet", 11L), new TIcebergCommitData());
    }

    /**
     * Iceberg: {@link CommitDataSerializer#feed} delivers exactly one
     * {@link Transaction#addCommitData(byte[])} call per input fragment, in input
     * order, and each payload deserializes losslessly (via {@link TBinaryProtocol},
     * the same protocol the consuming transactions use) back to the original
     * {@link TIcebergCommitData}.
     */
    @Test
    public void icebergFeedDeliversEachFragmentLosslessly() throws Exception {
        List<TIcebergCommitData> input = Arrays.asList(
                icebergData("s3://b/data/0.parquet", 11L),
                icebergData("s3://b/data/1.parquet", 13L));

        List<byte[]> payloads = new ArrayList<>();
        Transaction collector = new Transaction() {
            @Override
            public void commit() {
                throw new UnsupportedOperationException("commit not expected in this test");
            }

            @Override
            public void rollback() {
                throw new UnsupportedOperationException("rollback not expected in this test");
            }

            @Override
            public void addCommitData(byte[] commitFragment) {
                payloads.add(commitFragment);
            }
        };

        CommitDataSerializer.feed(collector, input);

        Assert.assertEquals(input.size(), payloads.size());
        for (int i = 0; i < input.size(); i++) {
            TIcebergCommitData roundTripped = new TIcebergCommitData();
            new TDeserializer(new TBinaryProtocol.Factory()).deserialize(roundTripped, payloads.get(i));
            Assert.assertEquals(input.get(i), roundTripped);
        }
    }

    /**
     * Hive: feeding each fragment through {@link CommitDataSerializer} accumulates
     * the identical list as the legacy whole-list {@code updateHivePartitionUpdates}.
     */
    @Test
    public void hmsFeedEqualsLegacyUpdate() {
        List<THivePartitionUpdate> input = Arrays.asList(
                hiveData("dt=2026-06-06", 7L, "f1", "f2"),
                hiveData("dt=2026-06-07", 9L, "f3"));

        HMSTransaction legacy = new HMSTransaction(null, null, null);
        legacy.updateHivePartitionUpdates(input);

        HMSTransaction viaFeed = new HMSTransaction(null, null, null);
        CommitDataSerializer.feed(viaFeed, input);

        Assert.assertEquals(legacy.getHivePartitionUpdates(), viaFeed.getHivePartitionUpdates());
        Assert.assertEquals(2, viaFeed.getHivePartitionUpdates().size());
    }

}
