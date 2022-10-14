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

package org.apache.doris.nereids.jobs.cascades;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.ColumnStat;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.StatisticsManager;
import org.apache.doris.statistics.StatsDeriveResult;
import org.apache.doris.statistics.TableStats;

import com.google.common.collect.ImmutableList;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class DeriveStatsJobTest {

    @Mocked
    ConnectContext context;
    @Mocked
    Env env;
    @Mocked
    StatisticsManager statisticsManager;

    SlotReference slot1;

    @Test
    public void testExecute() throws Exception {
        LogicalOlapScan olapScan = constructOlapSCan();
        LogicalAggregate agg = constructAgg(olapScan);
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(agg);
        new DeriveStatsJob(cascadesContext.getMemo().getRoot().getLogicalExpression(),
                new JobContext(cascadesContext, null, Double.MAX_VALUE)).execute();
        while (!cascadesContext.getJobPool().isEmpty()) {
            cascadesContext.getJobPool().pop().execute();
        }
        StatsDeriveResult statistics = cascadesContext.getMemo().getRoot().getStatistics();
        Assertions.assertNotNull(statistics);
        Assertions.assertEquals(1, statistics.getRowCount());
    }

    private LogicalOlapScan constructOlapSCan() throws AnalysisException {
        ColumnStat columnStats1 = new ColumnStat(10, 0, 0, 5,
                Double.NaN, Double.NaN);
        new MockUp<TableStats>(TableStats.class) {
            @Mock
            public ColumnStat getColumnStats(String columnName) {
                return columnStats1;
            }
        };

        long tableId1 = 0;

        Statistics statistics = new Statistics();

        List<String> qualifier = ImmutableList.of("test", "t");
        slot1 = new SlotReference("c1", IntegerType.INSTANCE, true, qualifier);
        new Expectations() {{
                ConnectContext.get();
                result = context;
                context.getEnv();
                result = env;
                env.getStatisticsManager();
                result = statisticsManager;
                statisticsManager.getStatistics();
                result = statistics;
            }};

        OlapTable table1 = PlanConstructor.newOlapTable(tableId1, "t1", 0);
        return new LogicalOlapScan(RelationId.createGenerator().getNextId(), table1, Collections.emptyList()).withLogicalProperties(
                Optional.of(new LogicalProperties(() -> ImmutableList.of(slot1))));
    }

    private LogicalAggregate constructAgg(Plan child) {
        List<Expression> groupByExprList = new ArrayList<>();
        groupByExprList.add(slot1);
        AggregateFunction sum = new Sum(slot1);
        Alias alias = new Alias(sum, "a");
        return new LogicalAggregate<>(groupByExprList, Collections.singletonList(alias), child);
    }
}
