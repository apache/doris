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

import mockit.Expectations;
import mockit.Mocked;

import org.apache.doris.analysis.AnalyzeStmt;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class StatisticsJobTest {
    @Test
    public void testInitializeJob(
            @Mocked AnalyzeStmt mockAnalyzeStmt,
            @Mocked PaloAuth auth,
            @Mocked Analyzer analyzer) throws Exception {

        Column col1 = new Column("col1", PrimitiveType.STRING);
        Column col2 = new Column("col2", PrimitiveType.INT);
        OlapTable table = new OlapTable(1L, "tbl", Arrays.asList(col1, col2), KeysType.AGG_KEYS,
                new PartitionInfo(), new HashDistributionInfo());
        Database database = new Database(1L, "db");
        database.createTable(table);

        Catalog catalog = Catalog.getCurrentCatalog();
        Deencapsulation.setField(catalog, "statisticsJobScheduler", new StatisticsJobScheduler());

        ConcurrentHashMap<String, Database> fullNameToDb = new ConcurrentHashMap<>();
        fullNameToDb.put("cluster:db", database);
        Deencapsulation.setField(catalog, "fullNameToDb", fullNameToDb);

        ConcurrentHashMap<Long, Database> idToDb = new ConcurrentHashMap<>();
        idToDb.put(1L, database);
        Deencapsulation.setField(catalog, "idToDb", idToDb);

        UserIdentity userIdentity = new UserIdentity("root", "host", false);

        new Expectations() {
            {
                mockAnalyzeStmt.getTableName();
                this.minTimes = 0;
                this.result = new TableName("db", "tbl");

                mockAnalyzeStmt.getClusterName();
                this.minTimes = 0;
                this.result = "cluster";

                mockAnalyzeStmt.getAnalyzer();
                this.minTimes = 0;
                this.result = analyzer;

                mockAnalyzeStmt.getColumnNames();
                this.minTimes = 0;
                this.result = Arrays.asList("col1", "col2");

                mockAnalyzeStmt.getUserInfo();
                this.minTimes = 0;
                this.result = userIdentity;

                auth.checkDbPriv(userIdentity, this.anyString, PrivPredicate.SELECT);
                this.minTimes = 0;
                this.result = true;

                auth.checkTblPriv(userIdentity, this.anyString, this.anyString, PrivPredicate.SELECT);
                this.minTimes = 0;
                this.result = true;
            }
        };

        // Run the test
        StatisticsJob statisticsJobUnderTest = new StatisticsJob(mockAnalyzeStmt);
        List<Long> tableIdList = statisticsJobUnderTest.getTableIdList();
        List<StatisticsTask> taskList = statisticsJobUnderTest.getTaskList();

        // Verify the results
        Assert.assertEquals(1, tableIdList.size());
        /*
         * mateTask(2):
         *  - col2[avg_len、max_len]
         *  - data_size
         * sqlTask(5):
         *  - row_count
         *  - col1[min_value、max_value、ndv],
         *  - col2[min_value、max_value、ndv]
         *  - col1[num_nulls]
         *  - col2[num_nulls]
         * sampleTask(1):
         *  - col1[max_size、avg_size]
         */
        Assert.assertEquals(8, taskList.size());
    }
}
