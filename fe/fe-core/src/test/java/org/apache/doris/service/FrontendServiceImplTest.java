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

package org.apache.doris.service;


import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TCreatePartitionRequest;
import org.apache.doris.thrift.TCreatePartitionResult;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStringLiteral;
import org.apache.doris.utframe.UtFrameUtils;

import mockit.Mocked;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class FrontendServiceImplTest {
    private static String runningDir = "fe/mocked/FrontendServiceImplTest/" + UUID.randomUUID().toString() + "/";
    private static ConnectContext connectContext;
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @Mocked
    ExecuteEnv exeEnv;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        FeConstants.default_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        UtFrameUtils.createDorisCluster(runningDir);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Env.getCurrentEnv().createDb(createDbStmt);
    }

    @AfterClass
    public static void tearDown() {
        UtFrameUtils.cleanDorisFeDir(runningDir);
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().createTable(createTableStmt);
    }


    @Test
    public void testCreatePartitionRange() throws Exception {
        String createOlapTblStmt = new String("CREATE TABLE test.partition_range(\n"
                + "    event_day DATETIME,\n"
                + "    site_id INT DEFAULT '10',\n"
                + "    city_code VARCHAR(100)\n"
                + ")\n"
                + "DUPLICATE KEY(event_day, site_id, city_code)\n"
                + "AUTO PARTITION BY range date_trunc( event_day,'day') (\n"
                + "\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 2\n"
                + "PROPERTIES(\"replication_num\" = \"1\");");

        createTable(createOlapTblStmt);
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
        OlapTable table = (OlapTable) db.getTableOrAnalysisException("partition_range");

        List<List<TStringLiteral>> partitionValues = new ArrayList<>();
        List<TStringLiteral> values = new ArrayList<>();

        TStringLiteral start = new TStringLiteral();
        start.setValue("2023-08-07 00:00:00");
        values.add(start);

        partitionValues.add(values);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDbId(db.getId());
        request.setTableId(table.getId());
        request.setPartitionValues(partitionValues);
        TCreatePartitionResult partition = impl.createPartition(request);

        Assert.assertEquals(partition.getStatus().getStatusCode(), TStatusCode.OK);
        Partition p20230807 = table.getPartition("p20230807000000");
        Assert.assertNotNull(p20230807);
    }

    @Test
    public void testCreatePartitionList() throws Exception {
        String createOlapTblStmt = new String("CREATE TABLE test.partition_list(\n"
                + "    event_day DATETIME,\n"
                + "    site_id INT DEFAULT '10',\n"
                + "    city_code VARCHAR(100) not null\n"
                + ")\n"
                + "DUPLICATE KEY(event_day, site_id, city_code)\n"
                + "AUTO PARTITION BY list (city_code) (\n"
                + "\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 2\n"
                + "PROPERTIES(\"replication_num\" = \"1\");");

        createTable(createOlapTblStmt);
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
        OlapTable table = (OlapTable) db.getTableOrAnalysisException("partition_list");

        List<List<TStringLiteral>> partitionValues = new ArrayList<>();
        List<TStringLiteral> values = new ArrayList<>();

        TStringLiteral start = new TStringLiteral();
        start.setValue("BEIJING");
        values.add(start);

        partitionValues.add(values);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDbId(db.getId());
        request.setTableId(table.getId());
        request.setPartitionValues(partitionValues);
        TCreatePartitionResult partition = impl.createPartition(request);

        Assert.assertEquals(partition.getStatus().getStatusCode(), TStatusCode.OK);
        List<Partition> pbs = (List<Partition>) table.getAllPartitions();
        Assert.assertEquals(pbs.size(), 1);
    }
}
