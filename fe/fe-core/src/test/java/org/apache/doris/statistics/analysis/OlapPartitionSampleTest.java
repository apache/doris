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

package org.apache.doris.statistics.analysis;

import org.apache.doris.analysis.FixedBlockSampleSpec;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.io.Hll;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.analysis.TableStatisticsSqlBuilder.BatchedPartitionColumnSpec;
import org.apache.doris.statistics.model.ColumnStatistic;
import org.apache.doris.statistics.model.PartitionColumnStatistic;
import org.apache.doris.statistics.model.PartitionColumnStatisticBuilder;
import org.apache.doris.statistics.repository.ResultRow;
import org.apache.doris.statistics.sampling.SampledScanSqlRenderer;
import org.apache.doris.statistics.sampling.TabletSampleSpec.FixedBlockScanClause;
import org.apache.doris.statistics.util.Hll128;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

class OlapPartitionSampleTest {

    @Test
    void testPartitionMergePreservesCachedHllAndCombinesUnseenNdv() {
        Hll128 hllP1 = new Hll128();
        hllP1.update(11L);
        hllP1.update(22L);
        hllP1.update(33L);

        Hll128 hllP2 = new Hll128();
        hllP2.update(22L);
        hllP2.update(33L);
        hllP2.update(44L);

        // Shared low-cardinality partitions (unseenNdv = 0) deduplicate via HLL_UNION without mutating p1.ndv
        PartitionColumnStatistic p1Low = new PartitionColumnStatisticBuilder(1000)
                .setNdv(hllP1)
                .setUnseenNdv(0)
                .setNumNulls(0)
                .setDataSize(4000)
                .build();
        PartitionColumnStatistic p2Low = new PartitionColumnStatisticBuilder(1000)
                .setNdv(hllP2)
                .setUnseenNdv(0)
                .setNumNulls(0)
                .setDataSize(4000)
                .build();

        ColumnStatistic mergedLow = new PartitionColumnStatisticBuilder(p1Low)
                .merge(p2Low)
                .toColumnStatistics();
        Assertions.assertEquals(3L, p1Low.ndv.estimateCardinality(),
                "Merging partitions must not mutate cached p1.ndv in-place");
        Assertions.assertEquals(4.0, mergedLow.ndv, 0.001);
        Assertions.assertEquals(2000.0, mergedLow.count, 0.001);
        Assertions.assertEquals(8000.0, mergedLow.dataSize, 0.001);

        // High-cardinality sampled partitions combine HLL_UNION(observed) + SUM(unseen_ndv) and clamp to non-nulls
        Hll128 sampleHll1 = new Hll128();
        sampleHll1.update(1001L);
        sampleHll1.update(1002L);
        Hll128 sampleHll2 = new Hll128();
        sampleHll2.update(2001L);
        sampleHll2.update(2002L);

        PartitionColumnStatistic p1High = new PartitionColumnStatisticBuilder(50_000)
                .setNdv(sampleHll1)
                .setUnseenNdv(49_998)
                .setNumNulls(0)
                .setDataSize(400_000)
                .build();
        PartitionColumnStatistic p2High = new PartitionColumnStatisticBuilder(50_000)
                .setNdv(sampleHll2)
                .setUnseenNdv(49_998)
                .setNumNulls(0)
                .setDataSize(400_000)
                .build();

        Assertions.assertEquals(50_000L, p1High.effectiveNdv());
        ColumnStatistic mergedHigh = new PartitionColumnStatisticBuilder(p1High)
                .merge(p2High)
                .toColumnStatistics();
        Assertions.assertEquals(100_000.0, mergedHigh.ndv, 0.001);

        // Clamp check when unseenNdv + hll exceeds non-null rows
        PartitionColumnStatistic pClamp = new PartitionColumnStatisticBuilder(100)
                .setNdv(sampleHll1)
                .setUnseenNdv(200)
                .setNumNulls(10)
                .build();
        Assertions.assertEquals(90L, pClamp.effectiveNdv());
        Assertions.assertEquals(90.0, new PartitionColumnStatisticBuilder(pClamp).toColumnStatistics().ndv, 0.001);
    }

    @Test
    void testPartitionColumnStatisticFromResultRowLegacyAndUnseenNdv() throws Exception {
        Hll hll = new Hll();
        hll.updateWithHash(42);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        hll.serialize(new DataOutputStream(baos));
        String b64 = Base64.getEncoder().encodeToString(baos.toByteArray());

        try (MockedStatic<StatisticsUtil> mocked = Mockito.mockStatic(
                StatisticsUtil.class, Mockito.CALLS_REAL_METHODS)) {
            mocked.when(() -> StatisticsUtil.findColumn(0L, 1L, 2L, -1L, "k1"))
                    .thenReturn(new Column("k1", PrimitiveType.BIGINT));

            ResultRow legacyRow = new ResultRow(Arrays.asList(
                    "0", "1", "2", "-1", "p1", "k1",
                    "1000", b64, "5", "10", "999", "8000", "2026-11-25 00:00:00"));
            PartitionColumnStatistic legacyStat = PartitionColumnStatistic.fromResultRow(legacyRow);
            Assertions.assertFalse(legacyStat.isUnKnown);
            Assertions.assertEquals(0.0, legacyStat.unseenNdv, 0.001);
            Assertions.assertEquals(1L, legacyStat.effectiveNdv());

            ResultRow sampledRow = new ResultRow(Arrays.asList(
                    "0", "1", "2", "-1", "p1", "k1",
                    "1000", b64, "5", "10", "999", "8000", "2026-11-25 00:00:00", "750"));
            PartitionColumnStatistic sampledStat = PartitionColumnStatistic.fromResultRow(sampledRow);
            Assertions.assertFalse(sampledStat.isUnKnown);
            Assertions.assertEquals(750.0, sampledStat.unseenNdv, 0.001);
            Assertions.assertEquals(751L, sampledStat.effectiveNdv());
        }
    }

    @Test
    void testBatchedPartitionSampleInsertSqlParsesCleanly() {
        FixedBlockSampleSpec blockSpec = new FixedBlockSampleSpec(
                FixedBlockSampleSpec.Mode.KEY_RANGE_BLOCK,
                4096L,
                12345L,
                List.of(new FixedBlockSampleSpec.Group(0.5, 0.25, 64L, List.of(10001L, 10002L))));
        String sampleClause = SampledScanSqlRenderer.render(new FixedBlockScanClause(blockSpec), 65536);
        String fromClause = "`internal`.`db1`.`tbl1` INDEX `mv1` partition `p1` " + sampleClause;

        List<BatchedPartitionColumnSpec> specs = List.of(
                new BatchedPartitionColumnSpec(0, "k_id", "`k_id`", false, 8, true, 1.0),
                new BatchedPartitionColumnSpec(1, "v_str", "`v_str`", true, 16, false, 0.5));

        String sampledSql = TableStatisticsSqlBuilder.batchedPartitionSampleInsertSql(
                fromClause, 0L, 10L, 20L, 30L, "p1", 40L,
                false, true, 0.125, 100_000L, specs);
        Assertions.assertTrue(sampledSql.contains("`unseen_ndv`"), sampledSql);
        Assertions.assertTrue(sampledSql.contains("WHERE `__agg`.`n` > 0"), sampledSql);

        NereidsParser parser = new NereidsParser();
        ConnectContext ctx = new ConnectContext();
        ctx.getState().setInternal(true);
        ctx.setThreadLocalInfo();
        try {
            Assertions.assertInstanceOf(InsertIntoTableCommand.class, parser.parseSingle(sampledSql));

            String censusSql = TableStatisticsSqlBuilder.batchedPartitionSampleInsertSql(
                    "`internal`.`db1`.`tbl1` partition `p1`", 0L, 10L, 20L, -1L, "p1", 40L,
                    true, false, 1.0, 50L, specs);
            Assertions.assertFalse(censusSql.contains("WHERE `__agg`.`n` > 0"), censusSql);
            Assertions.assertInstanceOf(InsertIntoTableCommand.class, parser.parseSingle(censusSql));
        } finally {
            ConnectContext.remove();
        }
    }
}
