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

import org.apache.doris.common.util.SqlParserUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.StringReader;

public class QueryOrganizationTest {
    @Test
    public void testOneSetOperand() throws Exception {
        String sql = "SELECT * FROM tbl WHERE a = 1 ORDER BY b LIMIT 10";
        org.apache.doris.analysis.SqlScanner input = new org.apache.doris.analysis.SqlScanner(
                new StringReader(sql), 0L);
        org.apache.doris.analysis.SqlParser parser = new org.apache.doris.analysis.SqlParser(input);
        StatementBase statementBase = SqlParserUtils.getFirstStmt(parser);

        Assertions.assertTrue(statementBase instanceof SelectStmt);
        SelectStmt selectStmt = (SelectStmt) statementBase;
        Assertions.assertEquals(10, selectStmt.getLimit());
        Assertions.assertEquals(1, selectStmt.getOrderByElements().size());
    }

    @Test
    public void testTwoSetOperandWithSecondInnerOrganization() throws Exception {
        String sql = "SELECT * FROM tbl WHERE a = 1 UNION (SELECT * FROM tbl WHERE a = 2 ORDER BY b LIMIT 10)";
        org.apache.doris.analysis.SqlScanner input = new org.apache.doris.analysis.SqlScanner(
                new StringReader(sql), 0L);
        org.apache.doris.analysis.SqlParser parser = new org.apache.doris.analysis.SqlParser(input);
        StatementBase statementBase = SqlParserUtils.getFirstStmt(parser);

        Assertions.assertTrue(statementBase instanceof SetOperationStmt);
        SetOperationStmt setOperationStmt = (SetOperationStmt) statementBase;
        Assertions.assertEquals(-1, setOperationStmt.getLimit());
        Assertions.assertNull(setOperationStmt.getOrderByElements());
        Assertions.assertEquals(2, setOperationStmt.getOperands().size());

        QueryStmt secondQueryStmt = setOperationStmt.getOperands().get(1).getQueryStmt();
        Assertions.assertEquals(10, secondQueryStmt.getLimit());
        Assertions.assertEquals(1, secondQueryStmt.getOrderByElements().size());
    }

    @Test
    public void testTwoSetOperandWithFirstInnerOrganization() throws Exception {
        String sql = "(SELECT * FROM tbl WHERE a = 1  ORDER BY b LIMIT 10) UNION SELECT * FROM tbl WHERE a = 2";
        org.apache.doris.analysis.SqlScanner input = new org.apache.doris.analysis.SqlScanner(
                new StringReader(sql), 0L);
        org.apache.doris.analysis.SqlParser parser = new org.apache.doris.analysis.SqlParser(input);
        StatementBase statementBase = SqlParserUtils.getFirstStmt(parser);

        Assertions.assertTrue(statementBase instanceof SetOperationStmt);
        SetOperationStmt setOperationStmt = (SetOperationStmt) statementBase;
        Assertions.assertEquals(-1, setOperationStmt.getLimit());
        Assertions.assertNull(setOperationStmt.getOrderByElements());
        Assertions.assertEquals(2, setOperationStmt.getOperands().size());

        QueryStmt secondQueryStmt = setOperationStmt.getOperands().get(0).getQueryStmt();
        Assertions.assertEquals(10, secondQueryStmt.getLimit());
        Assertions.assertEquals(1, secondQueryStmt.getOrderByElements().size());
    }

    @Test
    public void testTwoSetOperandWithOuterOrganization() throws Exception {
        String sql = "SELECT * FROM tbl WHERE a = 1 UNION SELECT * FROM tbl WHERE a = 2 ORDER BY b LIMIT 10";
        org.apache.doris.analysis.SqlScanner input = new org.apache.doris.analysis.SqlScanner(
                new StringReader(sql), 0L);
        org.apache.doris.analysis.SqlParser parser = new org.apache.doris.analysis.SqlParser(input);
        StatementBase statementBase = SqlParserUtils.getFirstStmt(parser);

        Assertions.assertTrue(statementBase instanceof SetOperationStmt);
        SetOperationStmt setOperationStmt = (SetOperationStmt) statementBase;
        Assertions.assertEquals(10, setOperationStmt.getLimit());
        Assertions.assertEquals(1, setOperationStmt.getOrderByElements().size());
        Assertions.assertEquals(2, setOperationStmt.getOperands().size());

        QueryStmt secondQueryStmt = setOperationStmt.getOperands().get(1).getQueryStmt();
        Assertions.assertEquals(-1, secondQueryStmt.getLimit());
        Assertions.assertNull(secondQueryStmt.getOrderByElements());
    }
}
