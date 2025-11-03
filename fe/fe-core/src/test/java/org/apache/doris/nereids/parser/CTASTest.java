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

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CTASTest extends ParserTestBase {

    @Test
    public void testSimpleCtas() {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan;
        String cteSql1 = "CREATE TABLE T14 AS SELECT * FROM T10 WHERE DT >  DATE_FORMAT(NOW(), 'YYYYMMDD');";
        logicalPlan = nereidsParser.parseSingle(cteSql1);
        Assertions.assertTrue(logicalPlan instanceof CreateTableCommand);
    }

    @Test
    public void testCtasWithProperties() {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan;
        String cteSql1 = "CREATE TABLE T14 PROPERTIES ('REPLICATION_NUM'='1') AS SELECT * FROM T10 WHERE DT >  DATE_FORMAT(NOW(), 'YYYYMMDD');";
        logicalPlan = nereidsParser.parseSingle(cteSql1);
        Assertions.assertTrue(logicalPlan instanceof CreateTableCommand);
    }

    @Test
    public void testCtasWithColumn() {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan;
        String cteSql1 = "CREATE TABLE T14(ID, AGE, DT) AS SELECT * FROM T10 WHERE DT >  DATE_FORMAT(NOW(), 'YYYYMMDD');";
        logicalPlan = nereidsParser.parseSingle(cteSql1);
        Assertions.assertTrue(logicalPlan instanceof CreateTableCommand);
    }

    @Test
    public void testCtasWithDistributed() {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan;
        String cteSql1 = "CREATE TABLE T14 DISTRIBUTED BY RANDOM AS SELECT * FROM T10 WHERE DT >  DATE_FORMAT(NOW(), 'YYYYMMDD');";
        logicalPlan = nereidsParser.parseSingle(cteSql1);
        Assertions.assertTrue(logicalPlan instanceof CreateTableCommand);
    }

    @Test
    public void testCtasWithbuckets() {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan;
        String cteSql1 = "CREATE TABLE T14 DISTRIBUTED BY HASH(ID) BUCKETS 10 AS SELECT * FROM T10 WHERE DT >  DATE_FORMAT(NOW(), 'YYYYMMDD');";
        logicalPlan = nereidsParser.parseSingle(cteSql1);
        Assertions.assertTrue(logicalPlan instanceof CreateTableCommand);
    }
}
