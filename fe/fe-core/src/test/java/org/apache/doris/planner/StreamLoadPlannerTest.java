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

import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.ImportColumnsStmt;
import org.apache.doris.analysis.ImportWhereStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.common.util.SqlParserUtils;

import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;

public class StreamLoadPlannerTest {
    @Test
    public void testParseStmt() throws Exception {
        String sql = new String("COLUMNS (k1, k2, k3=abc(), k4=default_value())");
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(sql)));
        ImportColumnsStmt columnsStmt = (ImportColumnsStmt) SqlParserUtils.getFirstStmt(parser);
        Assert.assertEquals(4, columnsStmt.getColumns().size());

        sql = new String("WHERE k1 > 2 and k3 < 4");
        parser = new SqlParser(new SqlScanner(new StringReader(sql)));
        ImportWhereStmt whereStmt = (ImportWhereStmt) SqlParserUtils.getFirstStmt(parser);
        Assert.assertTrue(whereStmt.getExpr() instanceof CompoundPredicate);
    }
}
