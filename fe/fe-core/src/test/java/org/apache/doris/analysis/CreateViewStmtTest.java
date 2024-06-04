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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

/**
 * test for CreateViewStmtTest.
 **/
public class CreateViewStmtTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        String table1 = "create table test.table1 (k1 int, k2 int) distributed by hash(k1) "
                + "buckets 1 properties('replication_num' = '1')";
        createTable(table1);
    }

    @Test
    public void testCreateView() throws Exception {
        connectContext.setDatabase("test");
        String createViewStr1 = "create view 1view1 as select k1,k2 from test.table1;";
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                String.format("Incorrect table name '1view1'. Table name regex is '%s'", FeNameFormat.getTableNameRegex()),
                () -> parseAndAnalyzeStmt(createViewStr1, connectContext));

        String createViewStr2 = "create view view2 as select k1,k2 from test.table1;";
        ExceptionChecker.expectThrowsNoException(
                () -> parseAndAnalyzeStmt(createViewStr2, connectContext));
    }
}
