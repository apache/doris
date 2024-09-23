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

package org.apache.doris.planner;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.planner.normalize.QueryCacheNormalizer;
import org.apache.doris.thrift.TNormalizedPlanNode;
import org.apache.doris.thrift.TQueryCacheParam;
import org.apache.doris.thrift.TRuntimeFilterMode;
import org.apache.doris.thrift.TRuntimeFilterType;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class QueryCacheNormalizerTest extends TestWithFeService {
    private static final Logger LOG = LogManager.getLogger(QueryCacheNormalizerTest.class);

    @Override
    protected void runBeforeAll() throws Exception {
        connectContext.getSessionVariable().setEnableNereidsPlanner(true);

        // Create database `db1`.
        createDatabase("db1");

        useDatabase("db1");

        // Create tables.
        String nonPart = "create table db1.non_part("
                + "  k1 varchar(32),\n"
                + "  k2 varchar(32),\n"
                + "  k3 varchar(32),\n"
                + "  v1 int,\n"
                + "  v2 int)\n"
                + "DUPLICATE KEY(k1, k2, k3)\n"
                + "distributed by hash(k1) buckets 3\n"
                + "properties('replication_num' = '1')";

        String part1 = "create table db1.part1("
                + "  dt date,\n"
                + "  k1 varchar(32),\n"
                + "  k2 varchar(32),\n"
                + "  k3 varchar(32),\n"
                + "  v1 int,\n"
                + "  v2 int)\n"
                + "DUPLICATE KEY(dt, k1, k2, k3)\n"
                + "PARTITION BY RANGE(dt)\n"
                + "(\n"
                + "  PARTITION p202403 VALUES [('2024-03-01'), ('2024-04-01')),\n"
                + "  PARTITION p202404 VALUES [('2024-04-01'), ('2024-05-01')),\n"
                + "  PARTITION p202405 VALUES [('2024-05-01'), ('2024-06-01'))\n"
                + ")\n"
                + "distributed by hash(k1) buckets 3\n"
                + "properties('replication_num' = '1')";

        String part2 = "create table db1.part2("
                + "  dt date,\n"
                + "  k1 varchar(32),\n"
                + "  k2 varchar(32),\n"
                + "  k3 varchar(32),\n"
                + "  v1 int,\n"
                + "  v2 int)\n"
                + "DUPLICATE KEY(dt, k1, k2, k3)\n"
                + "PARTITION BY RANGE(dt)\n"
                + "(\n"
                + "  PARTITION p202403 VALUES [('2024-03-01'), ('2024-04-01')),\n"
                + "  PARTITION p202404 VALUES [('2024-04-01'), ('2024-05-01')),\n"
                + "  PARTITION p202405 VALUES [('2024-05-01'), ('2024-06-01'))\n"
                + ")\n"
                + "distributed by hash(k1) buckets 3\n"
                + "properties('replication_num' = '1')";

        String multiLeveParts = "create table db1.multi_level_parts("
                + "  k1 int,\n"
                + "  dt date,\n"
                + "  hour int,\n"
                + "  v1 int,\n"
                + "  v2 int)\n"
                + "DUPLICATE KEY(k1)\n"
                + "PARTITION BY RANGE(dt, hour)\n"
                + "(\n"
                + "  PARTITION p202403 VALUES [('2024-03-01', '0'), ('2024-03-01', '1')),\n"
                + "  PARTITION p202404 VALUES [('2024-03-01', '1'), ('2024-03-01', '2')),\n"
                + "  PARTITION p202405 VALUES [('2024-03-01', '2'), ('2024-03-01', '3'))\n"
                + ")\n"
                + "distributed by hash(k1) buckets 3\n"
                + "properties('replication_num' = '1')";

        createTables(nonPart, part1, part2, multiLeveParts);

        connectContext.getSessionVariable().setEnableNereidsPlanner(true);
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    @Test
    public void testNormalize() throws Exception {
        String digest1 = getDigest("select k1 as k, sum(v1) as v from db1.non_part group by 1");
        String digest2 = getDigest("select sum(v1) as v1, k1 as k from db1.non_part group by 2");
        Assertions.assertEquals(64, digest1.length());
        Assertions.assertEquals(digest1, digest2);

        String digest3 = getDigest("select k1 as k, sum(v1) as v from db1.non_part where v1 between 1 and 10 group by 1");
        Assertions.assertNotEquals(digest1, digest3);

        String digest4 = getDigest("select k1 as k, sum(v1) as v from db1.non_part where v1 >= 1 and v1 <= 10 group by 1");
        Assertions.assertEquals(digest3, digest4);
        Assertions.assertEquals(64, digest3.length());

        String digest5 = getDigest("select k1 as k, sum(v1) as v from db1.non_part where v1 >= 1 and v1 < 11 group by 1");
        Assertions.assertNotEquals(digest3, digest5);
        Assertions.assertEquals(64, digest5.length());
    }

    @Test
    public void testProjectOnOlapScan() throws Exception {
        String digest1 = getDigest("select k1 + 1, k2, sum(v1), sum(v2) as v from db1.non_part group by 1, 2");
        String digest2 = getDigest("select sum(v2), k2, sum(v1), k1 + 1 from db1.non_part group by 2, 4");
        Assertions.assertEquals(digest1, digest2);
        Assertions.assertEquals(64, digest1.length());

        String digest3 = getDigest("select k1 + 1, k2, sum(v1 + 1), sum(v2) as v from db1.non_part group by 1, 2");
        Assertions.assertNotEquals(digest1, digest3);
        Assertions.assertEquals(64, digest3.length());
    }

    @Test
    public void testProjectOnAggregate() throws Exception {
        connectContext.getSessionVariable()
                .setDisableNereidsRules("PRUNE_EMPTY_PARTITION,TWO_PHASE_AGGREGATE_WITHOUT_DISTINCT");
        try {
            String digest1 = getDigest(
                    "select k1 + 1, k2 + 2, sum(v1) + 3, sum(v2) + 4 as v from db1.non_part group by k1, k2"
            );
            String digest2 = getDigest(
                    "select sum(v2) + 4, k2 + 2, sum(v1) + 3, k1 + 1 as v from db1.non_part group by k2, k1"
            );
            Assertions.assertEquals(digest1, digest2);
            Assertions.assertEquals(64, digest1.length());
        } finally {
            connectContext.getSessionVariable()
                    .setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        }
    }

    @Test
    public void testPartitionTable() throws Throwable {
        TQueryCacheParam queryCacheParam1 = getQueryCacheParam(
                "select k1 as k, sum(v1) as v from db1.part1 group by 1");
        TQueryCacheParam queryCacheParam2 = getQueryCacheParam(
                "select k1 as k, sum(v1) as v from db1.part1 where dt < '2025-01-01' group by 1");
        Assertions.assertEquals(queryCacheParam1.digest, queryCacheParam2.digest);
        Assertions.assertEquals(32, queryCacheParam1.digest.remaining());
        Assertions.assertEquals(queryCacheParam1.tablet_to_range, queryCacheParam2.tablet_to_range);

        TQueryCacheParam queryCacheParam3 = getQueryCacheParam(
                "select k1 as k, sum(v1) as v from db1.part1 where dt < '2024-05-20' group by 1");
        Assertions.assertEquals(queryCacheParam1.digest, queryCacheParam3.digest);
        Assertions.assertNotEquals(
                Lists.newArrayList(queryCacheParam1.tablet_to_range.values()),
                Lists.newArrayList(queryCacheParam3.tablet_to_range.values())
        );
        Assertions.assertEquals(32, queryCacheParam3.digest.remaining());

        TQueryCacheParam queryCacheParam4 = getQueryCacheParam(
                "select k1 as k, sum(v1) as v from db1.part1 where dt < '2024-04-20' group by 1");
        Assertions.assertEquals(queryCacheParam1.digest, queryCacheParam4.digest);
        Assertions.assertNotEquals(
                Lists.newArrayList(queryCacheParam1.tablet_to_range.values()),
                Lists.newArrayList(queryCacheParam4.tablet_to_range.values())
        );
        Assertions.assertEquals(32, queryCacheParam4.digest.remaining());
    }

    @Test
    public void testMultiLevelPartitionTable() throws Throwable {
        List<TQueryCacheParam> queryCacheParams = normalize(
                "select k1, sum(v1) as v from db1.multi_level_parts group by 1");
        Assertions.assertEquals(1, queryCacheParams.size());
    }

    @Test
    public void testHaving() throws Throwable {
        List<TNormalizedPlanNode> normalizedPlanNodes = onePhaseAggWithoutDistinct(() -> normalizePlans(
                "select k1, sum(v1) as v from db1.part1 where dt='2024-05-01' group by 1 having v > 10"));
        Assertions.assertEquals(2, normalizedPlanNodes.size());
        Assertions.assertTrue(
                normalizedPlanNodes.get(0).getOlapScanNode() != null
                        && CollectionUtils.isEmpty(normalizedPlanNodes.get(0).getConjuncts())
        );
        Assertions.assertTrue(
                normalizedPlanNodes.get(1).getAggregationNode() != null
                && !CollectionUtils.isEmpty(normalizedPlanNodes.get(1).getConjuncts())
        );
    }

    @Test
    public void testRuntimeFilter() throws Throwable {
        connectContext.getSessionVariable().setRuntimeFilterMode(TRuntimeFilterMode.GLOBAL.toString());
        connectContext.getSessionVariable().setRuntimeFilterType(TRuntimeFilterType.IN_OR_BLOOM.getValue());
        List<TQueryCacheParam> queryCacheParams = normalize(
                "select * from (select k1, count(*) from db1.part1 where k1 < 15 group by k1)a\n"
                        + "join (select k1, count(*) from db1.part1 where k1 < 10 group by k1)b\n"
                        + "on a.k1 = b.k1");

        // only non target side can use query cache
        Assertions.assertEquals(1, queryCacheParams.size());
    }

    @Test
    public void testSelectFromWhereNoGroupBy() throws Throwable {
        List<TNormalizedPlanNode> plans1 = normalizePlans("select sum(v1) from db1.part1");
        List<TNormalizedPlanNode> plans2 = normalizePlans("select sum(v1) as v from db1.part1");
        Assertions.assertEquals(plans1, plans2);

        List<TNormalizedPlanNode> plans3 = normalizePlans("select sum(v1) from db1.part1 where dt < '2025-01-01'");
        Assertions.assertEquals(plans1, plans3);

        List<TNormalizedPlanNode> plans4 = normalizePlans("select sum(v1) from db1.part1 where dt < '2024-05-01'");
        Assertions.assertEquals(plans1, plans4);

        TQueryCacheParam queryCacheParam1 = getQueryCacheParam("select sum(v1) from db1.part1");
        TQueryCacheParam queryCacheParam2 = getQueryCacheParam("select sum(v1) from db1.part1 where dt <= '2024-04-20'");
        Assertions.assertEquals(queryCacheParam1.digest, queryCacheParam2.digest);
        Assertions.assertEquals(32, queryCacheParam1.digest.remaining());
        Assertions.assertNotEquals(
                Lists.newArrayList(queryCacheParam1.tablet_to_range.values()),
                Lists.newArrayList(queryCacheParam2.tablet_to_range.values())
        );

        Assertions.assertEquals(
                queryCacheParam1.tablet_to_range.values().stream().sorted().collect(Collectors.toList()),
                ImmutableList.of(
                        "[[types: [DATEV2]; keys: [2024-03-01]; ..types: [DATEV2]; keys: [2024-04-01]; )]",
                        "[[types: [DATEV2]; keys: [2024-03-01]; ..types: [DATEV2]; keys: [2024-04-01]; )]",
                        "[[types: [DATEV2]; keys: [2024-03-01]; ..types: [DATEV2]; keys: [2024-04-01]; )]",
                        "[[types: [DATEV2]; keys: [2024-04-01]; ..types: [DATEV2]; keys: [2024-05-01]; )]",
                        "[[types: [DATEV2]; keys: [2024-04-01]; ..types: [DATEV2]; keys: [2024-05-01]; )]",
                        "[[types: [DATEV2]; keys: [2024-04-01]; ..types: [DATEV2]; keys: [2024-05-01]; )]",
                        "[[types: [DATEV2]; keys: [2024-05-01]; ..types: [DATEV2]; keys: [2024-06-01]; )]",
                        "[[types: [DATEV2]; keys: [2024-05-01]; ..types: [DATEV2]; keys: [2024-06-01]; )]",
                        "[[types: [DATEV2]; keys: [2024-05-01]; ..types: [DATEV2]; keys: [2024-06-01]; )]"
                )
        );

        Assertions.assertEquals(
                queryCacheParam2.tablet_to_range.values().stream().sorted().collect(Collectors.toList()),
                ImmutableList.of(
                        "[[types: [DATEV2]; keys: [2024-03-01]; ..types: [DATEV2]; keys: [2024-04-01]; )]",
                        "[[types: [DATEV2]; keys: [2024-03-01]; ..types: [DATEV2]; keys: [2024-04-01]; )]",
                        "[[types: [DATEV2]; keys: [2024-03-01]; ..types: [DATEV2]; keys: [2024-04-01]; )]",
                        "[[types: [DATEV2]; keys: [2024-04-01]; ..types: [DATEV2]; keys: [2024-04-21]; )]",
                        "[[types: [DATEV2]; keys: [2024-04-01]; ..types: [DATEV2]; keys: [2024-04-21]; )]",
                        "[[types: [DATEV2]; keys: [2024-04-01]; ..types: [DATEV2]; keys: [2024-04-21]; )]"
                )
        );

        TQueryCacheParam queryCacheParam3 = getQueryCacheParam(
                "select sum(v1), sum(v1 + 1), sum(v2), sum(v2 + 2) + 2 from db1.part1"
        );
        TQueryCacheParam queryCacheParam4 = getQueryCacheParam(
                "select sum(v2 + 2) + 2, sum(v2), sum(v1 + 1), sum(v1) from db1.part1"
        );
        Assertions.assertEquals(queryCacheParam3.digest, queryCacheParam4.digest);
        Assertions.assertEquals(32, queryCacheParam3.digest.remaining());
        Assertions.assertEquals(queryCacheParam3.tablet_to_range, queryCacheParam4.tablet_to_range);

        TQueryCacheParam queryCacheParam5 = getQueryCacheParam(
                "select sum(v1) from db1.part1 where dt between '2024-04-15' and '2024-04-20' and k1 = 1"
        );
        TQueryCacheParam queryCacheParam6 = getQueryCacheParam(
                "select sum(v1) from db1.part1 where dt between '2024-04-15' and '2024-04-20' and k1 = 2"
        );
        Assertions.assertNotEquals(queryCacheParam5.digest, queryCacheParam6.digest);
        Assertions.assertEquals(32, queryCacheParam5.digest.remaining());
        Assertions.assertEquals(32, queryCacheParam6.digest.remaining());
        Assertions.assertEquals(queryCacheParam5.tablet_to_range, queryCacheParam6.tablet_to_range);
    }

    @Test
    public void testSelectFromGroupBy() {
        twoPhaseAggWithoutDistinct(() -> {
            TQueryCacheParam queryCacheParam1 = getQueryCacheParam("select k1, sum(v1) from db1.part1 group by k1");
            TQueryCacheParam queryCacheParam2 = getQueryCacheParam("select k1, sum(v1) from db1.part1 group by k1 limit 10");
            Assertions.assertNotEquals(queryCacheParam1.digest, queryCacheParam2.digest);
            Assertions.assertEquals(32, queryCacheParam1.digest.remaining());
            Assertions.assertEquals(32, queryCacheParam2.digest.remaining());

            TQueryCacheParam queryCacheParam3 = getQueryCacheParam("select sum(v1), k1 from db1.part1 group by k1");
            Assertions.assertEquals(queryCacheParam1.digest, queryCacheParam3.digest);
            Assertions.assertEquals(
                    Lists.newArrayList(queryCacheParam1.tablet_to_range.values()),
                    Lists.newArrayList(queryCacheParam3.tablet_to_range.values())
            );

            List<TNormalizedPlanNode> plans1 = normalizePlans("select k1, sum(v1) from db1.part1 group by k1");
            List<TNormalizedPlanNode> plans2 = normalizePlans(
                    "select sum(v1), k1 from db1.part1 where dt between '2024-04-10' and '2024-05-06' group by k1");
            Assertions.assertEquals(plans1, plans2);
        });
    }

    @Test
    public void phasesAgg() {
        List<TNormalizedPlanNode> onePhaseAggPlans = onePhaseAggWithoutDistinct(() -> normalizePlans(
                "select sum(v1), k1 from db1.part1 where dt = '2024-04-10' group by k1"));
        List<TNormalizedPlanNode> onePhaseAggPlans2 = onePhaseAggWithoutDistinct(() -> normalizePlans(
                "select k1, sum(v1) from db1.part1 where dt = '2024-04-10' group by k1"));
        Assertions.assertEquals(onePhaseAggPlans, onePhaseAggPlans2);

        List<TNormalizedPlanNode> twoPhaseAggPlans = twoPhaseAggWithoutDistinct(() -> normalizePlans(
                "select sum(v1), k1 from db1.part1 where dt = '2024-04-10' group by k1"));
        Assertions.assertNotEquals(onePhaseAggPlans, twoPhaseAggPlans);
    }

    @Test
    public void phasesDistinctAgg() {
        List<TNormalizedPlanNode> noDistinctPlans = onePhaseAggWithoutDistinct(() -> normalizePlans(
                "select k1 from db1.part1 where dt = '2024-04-10' group by k1"));
        List<TNormalizedPlanNode> onePhaseAggPlans = onePhaseAggWithDistinct(() -> normalizePlans(
                "select distinct k1 from db1.part1 where dt = '2024-04-10'"));
        Assertions.assertEquals(noDistinctPlans, onePhaseAggPlans);
        List<TNormalizedPlanNode> twoPhaseAggPlans = twoPhaseAggWithDistinct(() -> normalizePlans(
                "select distinct k1 from db1.part1 where dt = '2024-04-10'"));
        Assertions.assertEquals(onePhaseAggPlans, twoPhaseAggPlans);

        List<TNormalizedPlanNode> threePhaseAggPlans = threePhaseAggWithDistinct(() -> normalizePlans(
                "select sum(distinct v1), k1 from db1.part1 where dt = '2024-04-10' group by k1"));
        List<TNormalizedPlanNode> fourPhaseAggPlans = fourPhaseAggWithDistinct(() -> normalizePlans(
                "select sum(distinct v1), k1 from db1.part1 where dt = '2024-04-10' group by k1"));
        Assertions.assertNotEquals(fourPhaseAggPlans, threePhaseAggPlans);
    }

    private String getDigest(String sql) throws Exception {
        return Hex.encodeHexString(getQueryCacheParam(sql).digest);
    }

    private TQueryCacheParam getQueryCacheParam(String sql) throws Exception {
        List<TQueryCacheParam> queryCaches = normalize(sql);
        Assertions.assertEquals(1, queryCaches.size());
        return queryCaches.get(0);
    }

    private List<TQueryCacheParam> normalize(String sql) throws Exception {
        Planner planner = getSqlStmtExecutor(sql).planner();
        DescriptorTable descTable = planner.getDescTable();
        List<PlanFragment> fragments = planner.getFragments();
        List<TQueryCacheParam> queryCacheParams = new ArrayList<>();
        for (PlanFragment fragment : fragments) {
            QueryCacheNormalizer normalizer = new QueryCacheNormalizer(fragment, descTable);
            Optional<TQueryCacheParam> queryCacheParam = normalizer.normalize(connectContext);
            if (queryCacheParam.isPresent()) {
                queryCacheParams.add(queryCacheParam.get());
            }
        }
        return queryCacheParams;
    }

    private List<TNormalizedPlanNode> normalizePlans(String sql) throws Exception {
        Planner planner = getSqlStmtExecutor(sql).planner();
        DescriptorTable descTable = planner.getDescTable();
        List<PlanFragment> fragments = planner.getFragments();
        List<TNormalizedPlanNode> normalizedPlans = new ArrayList<>();
        for (PlanFragment fragment : fragments) {
            QueryCacheNormalizer normalizer = new QueryCacheNormalizer(fragment, descTable);
            normalizedPlans.addAll(normalizer.normalizePlans(connectContext));
        }
        return normalizedPlans;
    }

    private void onePhaseAggWithoutDistinct(Callback callback) {
        onePhaseAggWithoutDistinct(() -> {
            callback.run();
            return null;
        });
    }

    private <T> T onePhaseAggWithDistinct(ResultCallback<T> callback) {
        try {
            connectContext.getSessionVariable()
                    .setDisableNereidsRules("PRUNE_EMPTY_PARTITION,"
                            + "TWO_PHASE_AGGREGATE_WITH_DISTINCT,"
                            + "THREE_PHASE_AGGREGATE_WITH_DISTINCT,"
                            + "FOUR_PHASE_AGGREGATE_WITH_DISTINCT,"
                            + "FOUR_PHASE_AGGREGATE_WITH_DISTINCT_WITH_FULL_DISTRIBUTE,"
                            + "TWO_PHASE_AGGREGATE_SINGLE_DISTINCT_TO_MULTI");
            return callback.run();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        } finally {
            connectContext.getSessionVariable()
                    .setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        }
    }

    private <T> T twoPhaseAggWithDistinct(ResultCallback<T> callback) {
        try {
            connectContext.getSessionVariable()
                    .setDisableNereidsRules("PRUNE_EMPTY_PARTITION,"
                            + "ONE_PHASE_AGGREGATE_SINGLE_DISTINCT_TO_MULTI,"
                            + "THREE_PHASE_AGGREGATE_WITH_DISTINCT,"
                            + "FOUR_PHASE_AGGREGATE_WITH_DISTINCT,"
                            + "FOUR_PHASE_AGGREGATE_WITH_DISTINCT_WITH_FULL_DISTRIBUTE,"
                            + "ONE_PHASE_AGGREGATE_SINGLE_DISTINCT_TO_MULTI");
            return callback.run();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        } finally {
            connectContext.getSessionVariable()
                    .setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        }
    }

    private <T> T threePhaseAggWithDistinct(ResultCallback<T> callback) {
        try {
            connectContext.getSessionVariable()
                    .setDisableNereidsRules("PRUNE_EMPTY_PARTITION,"
                            + "ONE_PHASE_AGGREGATE_SINGLE_DISTINCT_TO_MULTI,"
                            + "FOUR_PHASE_AGGREGATE_WITH_DISTINCT,"
                            + "FOUR_PHASE_AGGREGATE_WITH_DISTINCT_WITH_FULL_DISTRIBUTE,"
                            + "ONE_PHASE_AGGREGATE_SINGLE_DISTINCT_TO_MULTI,"
                            + "TWO_PHASE_AGGREGATE_SINGLE_DISTINCT_TO_MULTI");
            return callback.run();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        } finally {
            connectContext.getSessionVariable()
                    .setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        }
    }

    private <T> T fourPhaseAggWithDistinct(ResultCallback<T> callback) {
        try {
            connectContext.getSessionVariable()
                    .setDisableNereidsRules("PRUNE_EMPTY_PARTITION,"
                            + "ONE_PHASE_AGGREGATE_SINGLE_DISTINCT_TO_MULTI,"
                            + "THREE_PHASE_AGGREGATE_WITH_DISTINCT,"
                            + "ONE_PHASE_AGGREGATE_SINGLE_DISTINCT_TO_MULTI,"
                            + "TWO_PHASE_AGGREGATE_SINGLE_DISTINCT_TO_MULTI");
            return callback.run();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        } finally {
            connectContext.getSessionVariable()
                    .setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        }
    }


    private <T> T onePhaseAggWithoutDistinct(ResultCallback<T> callback) {
        try {
            connectContext.getSessionVariable()
                    .setDisableNereidsRules("PRUNE_EMPTY_PARTITION,TWO_PHASE_AGGREGATE_WITHOUT_DISTINCT");
            return callback.run();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        } finally {
            connectContext.getSessionVariable()
                    .setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        }
    }

    private void twoPhaseAggWithoutDistinct(Callback callback) {
        twoPhaseAggWithoutDistinct(() -> {
            callback.run();
            return null;
        });
    }

    private <T> T twoPhaseAggWithoutDistinct(ResultCallback<T> callback) {
        try {
            connectContext.getSessionVariable()
                    .setDisableNereidsRules("PRUNE_EMPTY_PARTITION,ONE_PHASE_AGGREGATE_WITHOUT_DISTINCT");
            return callback.run();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        } finally {
            connectContext.getSessionVariable()
                    .setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        }
    }

    private interface Callback {
        void run() throws Throwable;
    }

    private interface ResultCallback<T> {
        T run() throws Throwable;
    }
}
