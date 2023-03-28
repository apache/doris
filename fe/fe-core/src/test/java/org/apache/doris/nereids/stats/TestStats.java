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

package org.apache.doris.nereids.stats;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.datasets.tpch.TPCHUtils;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.statistics.ColumnLevelStatisticCache;
import org.apache.doris.statistics.StatisticsCache;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.utframe.TestWithFeService;

import mockit.Mock;
import mockit.MockUp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

// Assume that all column name is unique in the tested database
// CHECKSTYLE OFF
public abstract class TestStats extends TestWithFeService {

    protected Map<String/*colname*/, ColumnLevelStatisticCache> stats = new HashMap<>();

    protected List<String> cols = new ArrayList<String>() {{
        add("id");
        add("catalog_id");
        add("db_id");
        add("tbl_id");
        add("idx_id");
        add("col_id");
        add("part_id");
        add("count");
        add("ndv");
        add("null_count");
        add("min");
        add("max");
        add("data_size_in_bytes");
        add("update_time");
    }};

    protected List<PrimitiveType> types = new ArrayList<PrimitiveType>() {{
        add(PrimitiveType.VARCHAR);
        add(PrimitiveType.VARCHAR);
        add(PrimitiveType.VARCHAR);
        add(PrimitiveType.VARCHAR);
        add(PrimitiveType.VARCHAR);
        add(PrimitiveType.VARCHAR);
        add(PrimitiveType.VARCHAR);
        add(PrimitiveType.BIGINT);
        add(PrimitiveType.BIGINT);
        add(PrimitiveType.BIGINT);
        add(PrimitiveType.VARCHAR);
        add(PrimitiveType.VARCHAR);
        add(PrimitiveType.BIGINT);
        add(PrimitiveType.DATETIME);
    }};

    protected List<String> values = new ArrayList<>();

    protected ResultRow resultRow = null;

    protected final static Map<String, Type> colType = new HashMap<>();

    protected abstract void initMockedColumnsStats();

    protected abstract void initQError();


    protected abstract void initMockedReturnedRows();

    protected abstract void initEnv() throws Exception;

    protected abstract void initColNameToType();

    protected Map<Integer/*query id*/, Map<PlanNodeId, Double>> mockedExactReturnedRows = new HashMap<>();
    protected Map<Integer, Double> queryIdToQError = new HashMap<>();

    protected double avgQError;


    public void run() throws Exception {
        new MockUp<StatisticsUtil>() {

            @Mock
            public Column findColumn(long catalogId, long dbId, long tblId, long idxId, String columnName) {
                return new Column(columnName, colType.get(columnName));
            }
        };
        initMockedReturnedRows();
        initColNameToType();
        initMockedColumnsStats();
        new MockUp<StatisticsCache>() {
            @Mock
            public ColumnLevelStatisticCache getColumnStatistics(long tblId, long idxId, String colName) {
                return stats.get(colName);
            }
        };

        connectContext.getSessionVariable().setEnableNereidsPlanner(true);
        connectContext.getSessionVariable().enableFallbackToOriginalPlanner = false;
        StatsErrorEstimator statsErrorEstimator = new StatsErrorEstimator();
        connectContext.setStatsErrorEstimator(statsErrorEstimator);
        List<Double> qErrorList = new ArrayList<>();
        initEnv();
        for (int i = 0; i < TPCHUtils.SQLS.size(); i++) {
            String sql = TPCHUtils.SQLS.get(i);
            int sqlNumber = i + 1;
            NereidsPlanner nereidsPlanner = new NereidsPlanner(
                    new StatementContext(connectContext, new OriginStatement(sql, 0)));
            NereidsParser nereidsParser = new NereidsParser();
            nereidsPlanner.plan(nereidsParser.parseSQL(sql).get(0));
            Map<PlanNodeId, Double> extractReturnedRows = mockedExactReturnedRows.get(sqlNumber);
            for (Entry<PlanNodeId, Double> entry : extractReturnedRows.entrySet()) {
            //  statsErrorEstimator.setExactReturnedRow(entry.getKey(), entry.getValue());
            }
            qErrorList.add(statsErrorEstimator.calculateQError());
            statsErrorEstimator = new StatsErrorEstimator();
            connectContext.setStatsErrorEstimator(statsErrorEstimator);
        }
        // Assert.assertTrue(
        //         qErrorList.stream()
        //                 .mapToDouble(Double::doubleValue).average().orElseGet(() -> Double.POSITIVE_INFINITY)
        //                 <= avgQError + 1);
    }
}
