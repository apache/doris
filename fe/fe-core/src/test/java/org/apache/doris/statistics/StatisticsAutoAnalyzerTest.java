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

package org.apache.doris.statistics;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.View;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisInfo.AnalysisType;
import org.apache.doris.statistics.AnalysisInfo.JobType;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.system.SystemInfoService;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class StatisticsAutoAnalyzerTest {

    @Test
    public void testAnalyzeAll(@Injectable AnalysisInfo analysisInfo) {
        new MockUp<CatalogIf>() {
            @Mock
            public Collection<DatabaseIf> getAllDbs() {
                Database db1 = new Database(1, SystemInfoService.DEFAULT_CLUSTER
                        + ClusterNamespace.CLUSTER_DELIMITER + FeConstants.INTERNAL_DB_NAME);
                Database db2 = new Database(2, "anyDB");
                List<DatabaseIf> databaseIfs = new ArrayList<>();
                databaseIfs.add(db1);
                databaseIfs.add(db2);
                return databaseIfs;
            }
        };
        new MockUp<StatisticsAutoAnalyzer>() {
            @Mock
            public List<AnalysisInfo> constructAnalysisInfo(DatabaseIf<TableIf> db) {
                return Arrays.asList(analysisInfo, analysisInfo);
            }

            int count = 0;

            @Mock
            public AnalysisInfo getReAnalyzeRequiredPart(AnalysisInfo jobInfo) {
                return count++ == 0 ? null : jobInfo;
            }

            @Mock
            public void createSystemAnalysisJob(AnalysisInfo jobInfo)
                    throws DdlException {

            }
        };

        StatisticsAutoAnalyzer saa = new StatisticsAutoAnalyzer();
        saa.runAfterCatalogReady();
        new Expectations() {
            {
                try {
                    saa.createSystemAnalysisJob((AnalysisInfo) any);
                    times = 1;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @Test
    public void testConstructAnalysisInfo(
            @Injectable OlapTable o2, @Injectable View v) {
        new MockUp<Database>() {
            @Mock
            public List<Table> getTables() {
                List<Table> tableIfs = new ArrayList<>();
                tableIfs.add(o2);
                tableIfs.add(v);
                return tableIfs;
            }

            @Mock
            public String getFullName() {
                return "anyDb";
            }
        };

        new MockUp<OlapTable>() {
            @Mock
            public String getName() {
                return "anytable";
            }

            @Mock
            public List<Column> getBaseSchema() {
                List<Column> columns = new ArrayList<>();
                columns.add(new Column("c1", PrimitiveType.INT));
                columns.add(new Column("c2", PrimitiveType.HLL));
                return columns;
            }
        };
        StatisticsAutoAnalyzer saa = new StatisticsAutoAnalyzer();
        List<AnalysisInfo> analysisInfos =
                saa.constructAnalysisInfo(new Database(1, "anydb"));
        Assertions.assertEquals(1, analysisInfos.size());
        Assertions.assertEquals("c1", analysisInfos.get(0).colName.split(",")[0]);
    }

    @Test
    public void testGetReAnalyzeRequiredPart0(@Mocked TableIf tableIf) {

        new Expectations() {
            {
                tableIf.getRowCount();
                result = 100;
            }
        };
        new MockUp<StatisticsUtil>() {
            @Mock
            public TableIf findTable(String catalogName, String dbName, String tblName) {
                return tableIf;
            }
        };
        AnalysisInfo analysisInfo = new AnalysisInfoBuilder().setAnalysisMethod(AnalysisMethod.FULL).setAnalysisType(
                AnalysisType.FUNDAMENTALS).setColName("col1").setJobType(JobType.SYSTEM).build();
        new MockUp<AnalysisManager>() {

            int count = 0;

            TableStats[] tableStatsArr =
                    new TableStats[] {new TableStats(0, 0, analysisInfo),
                            new TableStats(0, 0, analysisInfo), null};

            {
                tableStatsArr[0].updatedRows.addAndGet(100);
                tableStatsArr[1].updatedRows.addAndGet(0);
            }

            @Mock
            public TableStats findTableStatsStatus(long tblId) {
                return tableStatsArr[count++];
            }
        };

        new MockUp<StatisticsAutoAnalyzer>() {
            @Mock
            public Set<String> findReAnalyzeNeededPartitions(TableIf table, long lastExecTimeInMs) {
                Set<String> partitionNames = new HashSet<>();
                partitionNames.add("p1");
                partitionNames.add("p2");
                return partitionNames;
            }

            @Mock
            public AnalysisInfo getAnalysisJobInfo(AnalysisInfo jobInfo, TableIf table,
                    Set<String> needRunPartitions) {
                return new AnalysisInfoBuilder().build();
            }
        };
        StatisticsAutoAnalyzer statisticsAutoAnalyzer = new StatisticsAutoAnalyzer();
        AnalysisInfo analysisInfo2 = new AnalysisInfoBuilder()
                .setCatalogName("cname")
                .setDbName("db")
                .setTblName("tbl").build();
        Assertions.assertNotNull(statisticsAutoAnalyzer.getReAnalyzeRequiredPart(analysisInfo2));
        Assertions.assertNull(statisticsAutoAnalyzer.getReAnalyzeRequiredPart(analysisInfo2));
        Assertions.assertNotNull(statisticsAutoAnalyzer.getReAnalyzeRequiredPart(analysisInfo2));
    }
}
