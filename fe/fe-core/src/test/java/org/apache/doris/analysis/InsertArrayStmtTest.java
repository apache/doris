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

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.StringReader;
import java.util.ArrayList;

public class InsertArrayStmtTest {
    private static final String RUNNING_DIR = UtFrameUtils.generateRandomFeRunningDir(InsertArrayStmtTest.class);
    private static ConnectContext connectContext;

    @BeforeClass
    public static void setUp() throws Exception {
        Config.enable_complex_type_support = true;
        UtFrameUtils.createDorisCluster(RUNNING_DIR);
        connectContext = UtFrameUtils.createDefaultCtx();
        createDatabase("create database test;");
    }

    @AfterClass
    public static void tearDown() {
        UtFrameUtils.cleanDorisFeDir(RUNNING_DIR);
    }

    private static void createDatabase(String sql) throws Exception {
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
    }

    private static InsertStmt parseAndAnalyze(String sql) throws Exception {
        SqlParser parser = new SqlParser(new SqlScanner(
                new StringReader(sql), connectContext.getSessionVariable().getSqlMode()
        ));
        InsertStmt insertStmt = (InsertStmt) SqlParserUtils.getFirstStmt(parser);
        Analyzer analyzer = new Analyzer(connectContext.getCatalog(), connectContext);
        insertStmt.analyze(analyzer);
        return insertStmt;
    };

    @Test
    public void testInsertArrayStmt() throws Exception {
        ExceptionChecker.expectThrowsNoException(() -> {
            createTable("create table test.table1 (k1 INT, v1 Array<int>) duplicate key (k1) " +
                    " distributed by hash (k1) buckets 1 properties ('replication_num' = '1');");
        });

        connectContext.setQueryId(new TUniqueId(1, 0));
        InsertStmt insertStmt = parseAndAnalyze("insert into test.table1 values (1, [1, 2, 3]);");
        ArrayList<Expr> row = ((SelectStmt) insertStmt.getQueryStmt()).getValueList().getFirstRow();
        Assert.assertEquals(2, row.size());
        Assert.assertTrue(row.get(1) instanceof ArrayLiteral);
        ArrayLiteral arrayLiteral = (ArrayLiteral) row.get(1);
        Assert.assertEquals(3, arrayLiteral.getChildren().size());
        Assert.assertTrue(arrayLiteral.isAnalyzed);
        for (int i = 1; i <= 3; ++ i) {
            Expr child = arrayLiteral.getChild(i - 1);
            Assert.assertTrue(child.isAnalyzed);
            Assert.assertSame(PrimitiveType.INT, child.getType().getPrimitiveType());
            Assert.assertTrue(child instanceof IntLiteral);
            Assert.assertEquals(i, ((IntLiteral) child).getValue());
        }

        connectContext.setQueryId(new TUniqueId(2, 0));
        insertStmt = parseAndAnalyze("insert into test.table1 values (1, []);");
        row = ((SelectStmt) insertStmt.getQueryStmt()).getValueList().getFirstRow();
        Assert.assertEquals(2, row.size());
        Assert.assertTrue(row.get(1) instanceof ArrayLiteral);
        arrayLiteral = (ArrayLiteral) row.get(1);
        Assert.assertTrue(arrayLiteral.isAnalyzed);
        Assert.assertTrue(arrayLiteral.getChildren().isEmpty());
        Assert.assertSame(PrimitiveType.INT, ((ArrayType) arrayLiteral.getType()).getItemType().getPrimitiveType());

        connectContext.setQueryId(new TUniqueId(3, 0));
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "type not match", ()-> {
            parseAndAnalyze("insert into test.table1 values (1, [[1, 2], [3, 4]]);");
        });
    }
}