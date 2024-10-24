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

package org.apache.doris.analysis;

import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ShowCreateTableStmtTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase("test");
        useDatabase("test");
        createTable("create table table1\n"
                + "(k1 int comment 'test column k1', k2 int comment 'test column k2')  comment 'test table1' "
                + "PARTITION BY RANGE(`k1`)\n"
                + "(\n"
                + "    PARTITION `p01` VALUES LESS THAN (\"10\"),\n"
                + "    PARTITION `p02` VALUES LESS THAN (\"100\")\n"
                + ") "
                + "distributed by hash(k1) buckets 1\n"
                + "properties(\"replication_num\" = \"1\");");
    }


    @Test
    public void testNormal() throws Exception {
        String sql = "show create table table1";
        ShowResultSet showResultSet = showCreateTable(sql);
        String showSql = showResultSet.getResultRows().get(0).get(1);
        Assertions.assertTrue(showSql.contains("`k1` int NULL COMMENT 'test column k1'"));
        Assertions.assertTrue(showSql.contains("COMMENT 'test table1'"));
    }

    @Test
    public void testBrief() throws Exception {
        String sql = "show brief create table table1";
        ShowResultSet showResultSet = showCreateTable(sql);
        String showSql = showResultSet.getResultRows().get(0).get(1);
        Assertions.assertTrue(!showSql.contains("PARTITION BY"));
        Assertions.assertTrue(!showSql.contains("PARTITION `p01`"));
    }
}
