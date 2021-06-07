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

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

public class RangePartitionPrunerTest {


  private static final String runningDir = "fe/mocked/RangePartitionPrunerTest/" + UUID.randomUUID() + "/";

  private static ConnectContext connectContext;

  @BeforeClass
  public static void beforeClass() throws Exception {
    UtFrameUtils.createMinDorisCluster(runningDir);

    // create connect context
    connectContext = UtFrameUtils.createDefaultCtx();
    // create database
    String createDbStmtStr = "create database test;";
    CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
    Catalog.getCurrentCatalog().createDb(createDbStmt);

    String sql = "CREATE TABLE test.`prune1` (\n" +
      "  `a` int(11) NULL COMMENT \"\",\n" +
      "  `b` int(11) NULL COMMENT \"\",\n" +
      "  `c` int(11) NULL COMMENT \"\",\n" +
      "  `d` int(11) NULL COMMENT \"\"\n" +
      ")\n" +
      "UNIQUE KEY(`a`,`b`,`c`)\n" +
      "COMMENT \"OLAP\"\n" +
      "PARTITION BY RANGE(`a`,`b`,`c`)(\n" +
      "  PARTITION p VALUES LESS THAN ('0')\n" +
      ")\n" +
      "DISTRIBUTED BY HASH(`a`) BUCKETS 2 \n" +
      "PROPERTIES(\"replication_num\" = \"1\");" ;
    createTable(sql);

  }

  @AfterClass
  public static void tearDown() {
    File file = new File(runningDir);
    file.delete();
  }

  private static void createTable(String sql) throws Exception {
    CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
    Catalog.getCurrentCatalog().createTable(createTableStmt);
  }

  @Before
  public void before() {
    FeConstants.runningUnitTest = true;
  }

  @After
  public void after() {
    FeConstants.runningUnitTest = false;
  }

  @Test
  public void testGe() throws Exception {
    String queryStr = "explain select * from test.`prune1` where a >= 0;";
    String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
    Assert.assertTrue(explainString.contains("partitions=0/1"));
    Assert.assertFalse(explainString.contains("partitions=1/1"));
  }

  @Test
  public void testGt() throws Exception {
    String queryStr = "explain select * from test.`prune1` where a > 0;";
    String explainString = UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, queryStr);
    Assert.assertTrue(explainString.contains("partitions=0/1"));
    Assert.assertFalse(explainString.contains("partitions=1/1"));
  }


}
