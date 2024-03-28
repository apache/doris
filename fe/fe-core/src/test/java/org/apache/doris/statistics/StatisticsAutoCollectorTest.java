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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.View;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class StatisticsAutoCollectorTest {

    @Test
    public void testAnalyzeAll(@Injectable AnalysisInfo analysisInfo) {
        new MockUp<CatalogIf>() {
            @Mock
            public Collection<DatabaseIf> getAllDbs() {
                Database db1 = new Database(1, FeConstants.INTERNAL_DB_NAME);
                Database db2 = new Database(2, "anyDB");
                List<DatabaseIf> databaseIfs = new ArrayList<>();
                databaseIfs.add(db1);
                databaseIfs.add(db2);
                return databaseIfs;
            }
        };
        new MockUp<StatisticsAutoCollector>() {
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

        StatisticsAutoCollector saa = new StatisticsAutoCollector();
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
            public List<Column> getSchemaAllIndexes(boolean full) {
                List<Column> columns = new ArrayList<>();
                columns.add(new Column("c1", PrimitiveType.INT));
                columns.add(new Column("c2", PrimitiveType.HLL));
                return columns;
            }
        };
        StatisticsAutoCollector saa = new StatisticsAutoCollector();
        List<AnalysisInfo> analysisInfoList = saa.constructAnalysisInfo(new Database(1, "anydb"));
        Assertions.assertEquals(1, analysisInfoList.size());
        Assertions.assertNull(analysisInfoList.get(0).colName);
    }

    @Test
    public void testSkipWideTable() {

        TableIf tableIf = new OlapTable();

        new MockUp<OlapTable>() {
            @Mock
            public List<Column> getBaseSchema() {
                return Lists.newArrayList(new Column("col1", Type.INT), new Column("col2", Type.INT));
            }

            @Mock
            public List<Pair<String, String>> getColumnIndexPairs(Set<String> columns) {
                ArrayList<Pair<String, String>> list = Lists.newArrayList();
                list.add(Pair.of("1", "1"));
                return list;
            }
        };

        new MockUp<StatisticsUtil>() {
            int count = 0;
            int[] thresholds = {1, 10};

            @Mock
            public TableIf findTable(long catalogName, long dbName, long tblName) {
                return tableIf;
            }

            @Mock
            public int getAutoAnalyzeTableWidthThreshold() {
                return thresholds[count++];
            }
        };

        AnalysisInfo analysisInfo = new AnalysisInfoBuilder().build();
        StatisticsAutoCollector statisticsAutoCollector = new StatisticsAutoCollector();
        Assertions.assertNull(statisticsAutoCollector.getNeedAnalyzeColumns(analysisInfo));
        Assertions.assertNotNull(statisticsAutoCollector.getNeedAnalyzeColumns(analysisInfo));
    }

    @Test
    public void testLoop() {
        AtomicBoolean timeChecked = new AtomicBoolean();
        AtomicBoolean switchChecked = new AtomicBoolean();
        new MockUp<StatisticsUtil>() {

            @Mock
            public boolean inAnalyzeTime(LocalTime now) {
                timeChecked.set(true);
                return true;
            }

            @Mock
            public boolean enableAutoAnalyze() {
                switchChecked.set(true);
                return true;
            }
        };
        StatisticsAutoCollector autoCollector = new StatisticsAutoCollector();
        autoCollector.collect();
        Assertions.assertTrue(timeChecked.get() && switchChecked.get());

    }

    @Test
    public void checkAvailableThread() {
        StatisticsAutoCollector autoCollector = new StatisticsAutoCollector();
        Assertions.assertEquals(Config.auto_analyze_simultaneously_running_task_num,
                autoCollector.analysisTaskExecutor.executors.getMaximumPoolSize());
    }

    @Test
    public void testSkip(@Mocked OlapTable olapTable, @Mocked TableStatsMeta stats, @Mocked TableIf anyOtherTable) {
        new MockUp<OlapTable>() {

            @Mock
            public long getDataSize(boolean singleReplica) {
                return StatisticsUtil.getHugeTableLowerBoundSizeInBytes() * 5 + 1000000000;
            }
        };

        new MockUp<AnalysisManager>() {

            @Mock
            public TableStatsMeta findTableStatsStatus(long tblId) {
                return stats;
            }
        };
        // A very huge table has been updated recently, so we should skip it this time
        stats.updatedTime = System.currentTimeMillis() - 1000;
        stats.newPartitionLoaded = new AtomicBoolean();
        stats.newPartitionLoaded.set(true);
        StatisticsAutoCollector autoCollector = new StatisticsAutoCollector();
        // Test new partition loaded data for the first time. Not skip.
        Assertions.assertFalse(autoCollector.skip(olapTable));
        stats.newPartitionLoaded.set(false);
        // Assertions.assertTrue(autoCollector.skip(olapTable));
        // The update of this huge table is long time ago, so we shouldn't skip it this time
        stats.updatedTime = System.currentTimeMillis()
                - StatisticsUtil.getHugeTableAutoAnalyzeIntervalInMillis() - 10000;
        Assertions.assertFalse(autoCollector.skip(olapTable));
        new MockUp<AnalysisManager>() {

            @Mock
            public TableStatsMeta findTableStatsStatus(long tblId) {
                return null;
            }
        };
        // can't find table stats meta, which means this table never get analyzed,  so we shouldn't skip it this time
        Assertions.assertFalse(autoCollector.skip(olapTable));
        new MockUp<AnalysisManager>() {

            @Mock
            public TableStatsMeta findTableStatsStatus(long tblId) {
                return stats;
            }
        };
        stats.userInjected = true;
        Assertions.assertTrue(autoCollector.skip(olapTable));
        // this is not olap table nor external table, so we should skip it this time
        Assertions.assertTrue(autoCollector.skip(anyOtherTable));
    }

    // For small table, use full
    @Test
    public void testCreateAnalyzeJobForTbl1(
            @Injectable OlapTable t1,
            @Injectable Database db
    ) throws Exception {
        new MockUp<Database>() {

            @Mock
            public CatalogIf getCatalog() {
                return Env.getCurrentInternalCatalog();
            }

            @Mock
            public long getId() {
                return 0;
            }
        };
        new MockUp<OlapTable>() {

            int count = 0;

            @Mock
            public List<Column> getBaseSchema() {
                return Lists.newArrayList(new Column("test", PrimitiveType.INT));
            }

            @Mock
            public long getDataSize(boolean singleReplica) {
                return StatisticsUtil.getHugeTableLowerBoundSizeInBytes() - 1;
            }

            @Mock
            public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
                return new OlapAnalysisTask(info);
            }

            @Mock
            public List<Long> getMvColumnIndexIds(String columnName) {
                ArrayList<Long> objects = new ArrayList<>();
                objects.add(-1L);
                return objects;
            }
        };

        new MockUp<StatisticsUtil>() {
            @Mock
            public TableIf findTable(long catalogId, long dbId, long tblId) {
                return t1;
            }
        };

        StatisticsAutoCollector sac = new StatisticsAutoCollector();
        List<AnalysisInfo> jobInfos = new ArrayList<>();
        sac.createAnalyzeJobForTbl(db, jobInfos, t1);
        AnalysisInfo jobInfo = jobInfos.get(0);
        List<Pair<String, String>> columnNames = Lists.newArrayList();
        columnNames.add(Pair.of("test", "t1"));
        jobInfo = new AnalysisInfoBuilder(jobInfo).setJobColumns(columnNames).build();
        Map<Long, BaseAnalysisTask> analysisTasks = new HashMap<>();
        AnalysisManager analysisManager = Env.getCurrentEnv().getAnalysisManager();
        analysisManager.createTaskForEachColumns(jobInfo, analysisTasks, false);
        Assertions.assertEquals(1, analysisTasks.size());
        for (BaseAnalysisTask task : analysisTasks.values()) {
            Assertions.assertNull(task.getTableSample());
        }
    }

    // for big table, use sample
    @Test
    public void testCreateAnalyzeJobForTbl2(
            @Injectable OlapTable t1,
            @Injectable Database db
    ) throws Exception {
        new MockUp<Database>() {

            @Mock
            public CatalogIf getCatalog() {
                return Env.getCurrentInternalCatalog();
            }

            @Mock
            public long getId() {
                return 0;
            }
        };
        new MockUp<OlapTable>() {

            int count = 0;

            @Mock
            public List<Column> getBaseSchema() {
                return Lists.newArrayList(new Column("test", PrimitiveType.INT));
            }

            @Mock
            public long getDataSize(boolean singleReplica) {
                return StatisticsUtil.getHugeTableLowerBoundSizeInBytes() * 2;
            }

            @Mock
            public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
                return new OlapAnalysisTask(info);
            }

            @Mock
            public List<Long> getMvColumnIndexIds(String columnName) {
                ArrayList<Long> objects = new ArrayList<>();
                objects.add(-1L);
                return objects;
            }
        };

        new MockUp<StatisticsUtil>() {
            @Mock
            public TableIf findTable(long catalogId, long dbId, long tblId) {
                return t1;
            }
        };

        StatisticsAutoCollector sac = new StatisticsAutoCollector();
        List<AnalysisInfo> jobInfos = new ArrayList<>();
        sac.createAnalyzeJobForTbl(db, jobInfos, t1);
        AnalysisInfo jobInfo = jobInfos.get(0);
        List<Pair<String, String>> colNames = Lists.newArrayList();
        colNames.add(Pair.of("test", "1"));
        jobInfo = new AnalysisInfoBuilder(jobInfo).setJobColumns(colNames).build();
        Map<Long, BaseAnalysisTask> analysisTasks = new HashMap<>();
        AnalysisManager analysisManager = Env.getCurrentEnv().getAnalysisManager();
        analysisManager.createTaskForEachColumns(jobInfo, analysisTasks, false);
        Assertions.assertEquals(1, analysisTasks.size());
        for (BaseAnalysisTask task : analysisTasks.values()) {
            Assertions.assertNotNull(task.getTableSample());
        }
    }

    @Test
    public void testDisableAuto1() throws Exception {
        InternalCatalog catalog1 = EnvFactory.createInternalCatalog();
        List<CatalogIf> catalogs = Lists.newArrayList();
        catalogs.add(catalog1);

        new MockUp<StatisticsAutoCollector>() {
            @Mock
            public List<CatalogIf> getCatalogsInOrder() {
                return catalogs;
            }

            @Mock
            protected boolean canCollect() {
                return false;
            }

        };

        StatisticsAutoCollector sac = new StatisticsAutoCollector();
        new Expectations(catalog1) {{
                catalog1.enableAutoAnalyze();
                times = 0;
            }};

        sac.analyzeAll();
    }

    @Test
    public void testDisableAuto2() throws Exception {
        InternalCatalog catalog1 = EnvFactory.createInternalCatalog();
        List<CatalogIf> catalogs = Lists.newArrayList();
        catalogs.add(catalog1);

        Database db1 = new Database();
        List<DatabaseIf<? extends TableIf>> dbs = Lists.newArrayList();
        dbs.add(db1);

        new MockUp<StatisticsAutoCollector>() {
            int count = 0;
            boolean[] canCollectReturn = {true, false};
            @Mock
            public List<CatalogIf> getCatalogsInOrder() {
                return catalogs;
            }

            @Mock
            public List<DatabaseIf<? extends TableIf>> getDatabasesInOrder(CatalogIf<DatabaseIf> catalog) {
                return dbs;
            }

                @Mock
            protected boolean canCollect() {
                return canCollectReturn[count++];
            }

        };

        StatisticsAutoCollector sac = new StatisticsAutoCollector();
        new Expectations(catalog1, db1) {{
                catalog1.enableAutoAnalyze();
                result = true;
                times = 1;
                db1.getFullName();
                times = 0;
            }};

        sac.analyzeAll();
    }
}
