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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.jdbc.JdbcExternalTable;
import org.apache.doris.statistics.util.DBObjects;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.ImmutableList;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class JdbcAnalysisTaskTest {
    @Test
    public void testTableStats(@Mocked CatalogIf catalogIf, @Mocked DatabaseIf databaseIf, @Mocked JdbcExternalTable tableIf,
            @Mocked AnalysisJob job)
            throws Exception {
        new Expectations() {
            {
                tableIf.getId();
                result = 30001;
                tableIf.getName();
                result = "test";
                catalogIf.getId();
                result = 10001;
                catalogIf.getName();
                result = "jdbc";
                databaseIf.getId();
                result = 20001;
                databaseIf.getFullName();
                result = "mysql";
            }
        };

        new MockUp<StatisticsUtil>() {
            @Mock
            public List<ResultRow> execStatisticQuery(String sql) {
                Assertions.assertEquals("SELECT COUNT(1) as rowCount FROM `jdbc`.`mysql`.`test`", sql);
                return ImmutableList.of(new ResultRow(ImmutableList.of("100")));
            }

            @Mock
            public DBObjects convertIdToObjects(long catalogId, long dbId, long tblId) {
                return new DBObjects(catalogIf, databaseIf, tableIf);
            }
        };

        new MockUp<AnalysisManager>() {
            @Mock
            public void updateTableStatsStatus(TableStatsMeta tableStats) {
                Assertions.assertEquals(100, tableStats.rowCount);
            }
        };

        new MockUp<TableStatsMeta>() {
            @Mock
            public void update(AnalysisInfo analyzedJob, TableIf tableIf) {
                //do nothing
            }
        };

        AnalysisInfoBuilder analysisInfoBuilder = new AnalysisInfoBuilder();
        analysisInfoBuilder.setColName("col1");
        analysisInfoBuilder.setJobType(AnalysisInfo.JobType.MANUAL);
        analysisInfoBuilder.setExternalTableLevelTask(true);

        JdbcAnalysisTask task = new JdbcAnalysisTask(analysisInfoBuilder.build());
        task.col = new Column("col1", PrimitiveType.INT);
        task.tbl = tableIf;
        task.catalog = catalogIf;
        task.db = databaseIf;
        task.setJob(job);

        task.doExecute();
    }

    @Test
    public void testGetColumnStats(@Mocked CatalogIf catalogIf, @Mocked DatabaseIf databaseIf, @Mocked JdbcExternalTable tableIf)
            throws Exception {
        new Expectations() {
            {
                tableIf.getId();
                result = 30001;
                tableIf.getName();
                result = "test";
                catalogIf.getId();
                result = 10001;
                catalogIf.getName();
                result = "jdbc";
                databaseIf.getId();
                result = 20001;
                databaseIf.getFullName();
                result = "mysql";
            }
        };

        new MockUp<StatisticsUtil>() {
            @Mock
            public DBObjects convertIdToObjects(long catalogId, long dbId, long tblId) {
                return new DBObjects(catalogIf, databaseIf, tableIf);
            }
        };

        new MockUp<JdbcAnalysisTask>() {
            @Mock
            public void runQuery(String sql) {
                Assertions.assertEquals(" SELECT CONCAT(30001, '-', -1, '-', 'col1') AS id, "
                        + "10001 AS catalog_id, 20001 AS db_id, 30001 AS tbl_id, -1 AS idx_id, 'col1' AS col_id, "
                        + "NULL AS part_id, COUNT(1) AS row_count, NDV(`col1`) AS ndv, "
                        + "SUM(CASE WHEN `col1` IS NULL THEN 1 ELSE 0 END) AS null_count, "
                        + "SUBSTRING(CAST(MIN(`col1`) AS STRING), 1, 1024) AS min, "
                        + "SUBSTRING(CAST(MAX(`col1`) AS STRING), 1, 1024) AS max, "
                        + "COUNT(1) * 4 AS data_size, NOW() "
                        + "FROM `jdbc`.`mysql`.`test`", sql);
            }
        };

        AnalysisInfoBuilder analysisInfoBuilder = new AnalysisInfoBuilder();
        analysisInfoBuilder.setColName("col1");
        analysisInfoBuilder.setJobType(AnalysisInfo.JobType.MANUAL);
        analysisInfoBuilder.setExternalTableLevelTask(false);

        JdbcAnalysisTask task = new JdbcAnalysisTask(analysisInfoBuilder.build());
        task.col = new Column("col1", PrimitiveType.INT);
        task.tbl = tableIf;
        task.catalog = catalogIf;
        task.db = databaseIf;

        task.doExecute();
    }
}
