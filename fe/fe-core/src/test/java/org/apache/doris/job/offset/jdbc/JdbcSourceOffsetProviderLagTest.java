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

package org.apache.doris.job.offset.jdbc;

import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.cdc.response.FetchEndOffsetResult;
import org.apache.doris.job.cdc.split.BinlogSplit;
import org.apache.doris.job.cdc.split.SnapshotSplit;
import org.apache.doris.job.common.DataSourceType;
import org.apache.doris.job.exception.JobException;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class JdbcSourceOffsetProviderLagTest {

    @Test
    public void testPostgresSnapshotDoesNotSendFeReferenceOffset() {
        JdbcSourceOffsetProvider provider = provider(DataSourceType.POSTGRES, DataSourceConfigKeys.OFFSET_INITIAL);
        provider.finishedSplits.add(snapshotSplit("split-1", offset("lsn", "300")));
        provider.finishedSplits.add(snapshotSplit("split-2", offset("lsn", "100")));

        Assert.assertNull(provider.getLagReferenceOffset());
    }

    @Test
    public void testPostgresIncrementalPhaseUsesCommittedOffset() {
        JdbcSourceOffsetProvider provider = provider(DataSourceType.POSTGRES, DataSourceConfigKeys.OFFSET_INITIAL);
        Map<String, String> committedOffset = offset("lsn", "700");
        provider.currentOffset =
                new JdbcOffset(Collections.singletonList(new BinlogSplit(committedOffset)));

        Assert.assertEquals(committedOffset, provider.getLagReferenceOffset());
    }

    @Test
    public void testInitialSnapshotUsesFirstCommittedMysqlHighWatermark() {
        JdbcSourceOffsetProvider provider = provider(DataSourceType.MYSQL, DataSourceConfigKeys.OFFSET_INITIAL);
        provider.finishedSplits.add(snapshotSplit("split-1", mysqlOffset("mysql-bin.000010", 100)));
        provider.finishedSplits.add(snapshotSplit("split-2", mysqlOffset("mysql-bin.000009", 900)));
        provider.finishedSplits.add(snapshotSplit("split-3", mysqlOffset("mysql-bin.000010", 50)));

        Assert.assertEquals(mysqlOffset("mysql-bin.000010", 100), provider.getLagReferenceOffset());
    }

    @Test
    public void testIncrementalPhaseUsesCommittedBinlogOffset() {
        JdbcSourceOffsetProvider provider = provider(DataSourceType.MYSQL, DataSourceConfigKeys.OFFSET_INITIAL);
        provider.finishedSplits.add(snapshotSplit("split-1", mysqlOffset("mysql-bin.000001", 100)));
        Map<String, String> committedOffset = mysqlOffset("mysql-bin.000002", 250);
        provider.currentOffset =
                new JdbcOffset(Collections.singletonList(new BinlogSplit(committedOffset)));

        Assert.assertEquals(committedOffset, provider.getLagReferenceOffset());
    }

    @Test
    public void testRestoredSnapshotToBinlogTransitionUsesSnapshotHighWatermark() {
        JdbcSourceOffsetProvider provider = provider(DataSourceType.MYSQL, DataSourceConfigKeys.OFFSET_INITIAL);
        provider.finishedSplits.add(snapshotSplit("split-1", mysqlOffset("mysql-bin.000003", 300)));
        provider.finishedSplits.add(snapshotSplit("split-2", mysqlOffset("mysql-bin.000001", 100)));
        provider.currentOffset =
                new JdbcOffset(Collections.singletonList(new BinlogSplit()));

        Assert.assertEquals(mysqlOffset("mysql-bin.000003", 300), provider.getLagReferenceOffset());
    }

    @Test
    public void testSnapshotOnlyDoesNotExposeSourceLogLag() {
        JdbcSourceOffsetProvider provider = provider(DataSourceType.POSTGRES, DataSourceConfigKeys.OFFSET_SNAPSHOT);
        provider.finishedSplits.add(snapshotSplit("split-1", offset("lsn", "100")));

        Assert.assertNull(provider.getLagReferenceOffset());
        Assert.assertEquals("-1", provider.getLag());
    }

    @Test
    public void testLagIsAlwaysNumeric() {
        JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider();

        Assert.assertEquals("-1", provider.getLag());
        provider.setLagBytes(4096);
        Assert.assertEquals("4096", provider.getLag());
        Assert.assertEquals(4096, provider.getLagBytes());
    }

    @Test
    public void testMysqlLastSourceEventTimestampUsesCommittedOffsetSeconds() {
        JdbcSourceOffsetProvider provider = provider(DataSourceType.MYSQL, DataSourceConfigKeys.OFFSET_INITIAL);
        Map<String, String> committedOffset = mysqlOffset("mysql-bin.000002", 250);
        committedOffset.put("ts_sec", "1787039821");
        provider.currentOffset =
                new JdbcOffset(Collections.singletonList(new BinlogSplit(committedOffset)));

        Assert.assertEquals(1787039821L, provider.getLastSourceEventTimestampSeconds());
    }

    @Test
    public void testPostgresLastSourceEventTimestampConvertsCommittedOffsetMicrosToSeconds() {
        JdbcSourceOffsetProvider provider = provider(DataSourceType.POSTGRES, DataSourceConfigKeys.OFFSET_INITIAL);
        Map<String, String> committedOffset = new HashMap<>();
        committedOffset.put("lsn", "700");
        committedOffset.put("ts_usec", "1787039821987654");
        provider.currentOffset =
                new JdbcOffset(Collections.singletonList(new BinlogSplit(committedOffset)));

        Assert.assertEquals(1787039821L, provider.getLastSourceEventTimestampSeconds());
    }

    @Test
    public void testLastSourceEventTimestampUnavailableBeforeCommittedBinlogTimestamp() {
        JdbcSourceOffsetProvider provider = provider(DataSourceType.MYSQL, DataSourceConfigKeys.OFFSET_INITIAL);
        provider.currentOffset = new JdbcOffset(Collections.singletonList(
                snapshotSplit("split-1", mysqlOffset("mysql-bin.000001", 100))));

        Assert.assertEquals(0L, provider.getLastSourceEventTimestampSeconds());

        provider.currentOffset = new JdbcOffset(Collections.singletonList(
                new BinlogSplit(mysqlOffset("mysql-bin.000002", 250))));
        Assert.assertEquals(0L, provider.getLastSourceEventTimestampSeconds());
    }

    @Test
    public void testUnavailableLagDoesNotOverwriteLastSuccessfulValue() {
        JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider();
        provider.setLagBytes(4096);

        provider.updateLagBytes(-1);

        Assert.assertEquals(4096, provider.getLagBytes());
    }

    @Test
    public void testSuccessfulLagReplacesLastSuccessfulValue() {
        JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider();
        provider.setLagBytes(4096);

        provider.updateLagBytes(2048);

        Assert.assertEquals(2048, provider.getLagBytes());
    }

    @Test
    public void testParseFetchEndOffsetResponse() throws JobException {
        JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider();
        String response = "{\"code\":0,\"msg\":\"Success\",\"data\":{"
                + "\"endOffset\":{\"lsn\":\"200\"},\"lagBytes\":4096}}";

        FetchEndOffsetResult result = provider.parseCdcResponseData(
                response, new TypeReference<FetchEndOffsetResult>() {});

        Assert.assertEquals(offset("lsn", "200"), result.getEndOffset());
        Assert.assertEquals(4096, result.getLagBytes());
    }

    private static JdbcSourceOffsetProvider provider(DataSourceType type, String startupMode) {
        JdbcSourceOffsetProvider provider = new JdbcSourceOffsetProvider();
        provider.setSourceType(type);
        provider.setJobId(123L);
        provider.setSourceProperties(
                Collections.singletonMap(DataSourceConfigKeys.OFFSET, startupMode));
        return provider;
    }

    private static SnapshotSplit snapshotSplit(String splitId, Map<String, String> highWatermark) {
        return new SnapshotSplit(splitId, "db.table", Arrays.asList("id"), null, null, highWatermark);
    }

    private static Map<String, String> mysqlOffset(String file, long position) {
        Map<String, String> offset = new HashMap<>();
        offset.put("file", file);
        offset.put("pos", String.valueOf(position));
        return offset;
    }

    private static Map<String, String> offset(String key, String value) {
        return Collections.singletonMap(key, value);
    }
}
