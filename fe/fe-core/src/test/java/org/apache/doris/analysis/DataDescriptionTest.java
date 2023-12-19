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

import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.MockedAuth;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataDescriptionTest {

    @Mocked
    private AccessControllerManager accessManager;
    @Mocked
    private ConnectContext ctx;
    @Mocked
    private Database db;
    @Mocked
    private OlapTable tbl;
    @Mocked
    private Analyzer analyzer;
    @Mocked
    private Env env;
    @Mocked
    private InternalCatalog catalog;

    @Before
    public void setUp() throws AnalysisException {
        MockedAuth.mockedAccess(accessManager);
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
        new Expectations() {
            {

                analyzer.getDefaultDb();
                minTimes = 0;
                result = "testDb";

                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;

                catalog.getDbNullable(anyString);
                minTimes = 0;
                result = db;

                db.getTableNullable(anyString);
                minTimes = 0;
                result = tbl;

            }
        };
    }

    @Test
    public void testNormal() throws AnalysisException {
        DataDescription desc = new DataDescription("testTable", null, Lists.newArrayList("abc.txt"),
                                                   null, null, null, false, null);
        desc.analyze("testDb");
        Assert.assertEquals("APPEND DATA INFILE ('abc.txt') INTO TABLE testTable", desc.toString());

        desc = new DataDescription("testTable", null, Lists.newArrayList("abc.txt"), null, null, null,
                                                  true, null);
        desc.analyze("testDb");
        Assert.assertEquals("APPEND DATA INFILE ('abc.txt') NEGATIVE INTO TABLE testTable", desc.toString());

        desc = new DataDescription("testTable", null, Lists.newArrayList("abc.txt", "bcd.txt"), null,
                                                  null, null, true, null);
        desc.analyze("testDb");
        Assert.assertEquals("APPEND DATA INFILE ('abc.txt', 'bcd.txt') NEGATIVE INTO TABLE testTable", desc.toString());

        desc = new DataDescription("testTable", null, Lists.newArrayList("abc.txt"),
                                                  Lists.newArrayList("col1", "col2"), null, null, true, null);
        desc.analyze("testDb");
        Assert.assertEquals("APPEND DATA INFILE ('abc.txt') NEGATIVE INTO TABLE testTable (col1, col2)", desc.toString());
        Assert.assertEquals("testTable", desc.getTableName());
        Assert.assertEquals("[col1, col2]", desc.getFileFieldNames().toString());
        Assert.assertEquals("[abc.txt]", desc.getFilePaths().toString());
        Assert.assertTrue(desc.isNegative());
        Assert.assertNull(desc.getColumnSeparator());
        Expr whereExpr = new BinaryPredicate(BinaryPredicate.Operator.EQ, new IntLiteral(1),  new IntLiteral(1));

        desc = new DataDescription("testTable", null, Lists.newArrayList("abc.txt"),
                Lists.newArrayList("col1", "col2"), new Separator(","), "csv", null, false, null, null, whereExpr, LoadTask.MergeType.MERGE, whereExpr, null, null);
        desc.analyze("testDb");
        Assert.assertEquals("MERGE DATA INFILE ('abc.txt') INTO TABLE testTable COLUMNS TERMINATED BY ',' FORMAT AS 'csv' (col1, col2) WHERE 1 = 1 DELETE ON 1 = 1", desc.toString());
        Assert.assertEquals("1 = 1", desc.getWhereExpr().toSql());
        Assert.assertEquals("1 = 1", desc.getDeleteCondition().toSql());
        Assert.assertEquals(",", desc.getColumnSeparator());

        desc = new DataDescription("testTable", null, Lists.newArrayList("abc.txt", "bcd.txt"),
                                                  Lists.newArrayList("col1", "col2"), new Separator("\t"),
                                                  null, true, null);
        desc.analyze("testDb");
        Assert.assertEquals("APPEND DATA INFILE ('abc.txt', 'bcd.txt') NEGATIVE INTO TABLE testTable"
                        +  " COLUMNS TERMINATED BY '\t' (col1, col2)",
                desc.toString());

        // hive \x01 column separator
        desc = new DataDescription("testTable", null, Lists.newArrayList("abc.txt", "bcd.txt"),
                                                  Lists.newArrayList("col1", "col2"), new Separator("\\x01"),
                                                  null, true, null);
        desc.analyze("testDb");
        Assert.assertEquals("APPEND DATA INFILE ('abc.txt', 'bcd.txt') NEGATIVE INTO TABLE testTable"
                        +  " COLUMNS TERMINATED BY '\\x01' (col1, col2)",
                desc.toString());

        // with partition
        desc = new DataDescription("testTable", new PartitionNames(false, Lists.newArrayList("p1", "p2")),
                                                  Lists.newArrayList("abc.txt"),
                                                  null, null, null, false, null);
        desc.analyze("testDb");
        Assert.assertEquals("APPEND DATA INFILE ('abc.txt') INTO TABLE testTable PARTITIONS (p1, p2)", desc.toString());

        // alignment_timestamp func
        List<Expr> params = Lists.newArrayList();
        params.add(new StringLiteral("day"));
        params.add(new SlotRef(null, "k2"));
        BinaryPredicate predicate = new BinaryPredicate(Operator.EQ, new SlotRef(null, "k1"),
                new FunctionCallExpr("alignment_timestamp", params));
        desc = new DataDescription("testTable", new PartitionNames(false, Lists.newArrayList("p1", "p2")),
                                                  Lists.newArrayList("abc.txt"),
                                                  Lists.newArrayList("k2", "k3"), null, null, false, Lists
                                                          .newArrayList((Expr) predicate));
        desc.analyze("testDb");
        String sql = "APPEND DATA INFILE ('abc.txt') INTO TABLE testTable PARTITIONS (p1, p2) (k2, k3)"
                + " SET (`k1` = alignment_timestamp('day', `k2`))";
        Assert.assertEquals(sql, desc.toString());

        // replace_value func
        params.clear();
        params.add(new StringLiteral("-"));
        params.add(new StringLiteral("10"));
        predicate = new BinaryPredicate(Operator.EQ, new SlotRef(null, "k1"),
                new FunctionCallExpr("replace_value", params));
        desc = new DataDescription("testTable", new PartitionNames(false, Lists.newArrayList("p1", "p2")),
                                                  Lists.newArrayList("abc.txt"),
                                                  Lists.newArrayList("k2", "k3"), null, null,
                                                  false, Lists.newArrayList((Expr) predicate));
        desc.analyze("testDb");
        sql = "APPEND DATA INFILE ('abc.txt') INTO TABLE testTable PARTITIONS (p1, p2) (k2, k3)"
                + " SET (`k1` = replace_value('-', '10'))";
        Assert.assertEquals(sql, desc.toString());

        // replace_value null
        params.clear();
        params.add(new StringLiteral(""));
        params.add(new NullLiteral());
        predicate = new BinaryPredicate(Operator.EQ, new SlotRef(null, "k1"),
                new FunctionCallExpr("replace_value", params));
        desc = new DataDescription("testTable", new PartitionNames(false, Lists.newArrayList("p1", "p2")),
                                                  Lists.newArrayList("abc.txt"),
                                                  Lists.newArrayList("k2", "k3"), null, null, false, Lists
                                                          .newArrayList((Expr) predicate));
        desc.analyze("testDb");
        sql = "APPEND DATA INFILE ('abc.txt') INTO TABLE testTable PARTITIONS (p1, p2) (k2, k3)"
                + " SET (`k1` = replace_value('', NULL))";
        Assert.assertEquals(sql, desc.toString());

        // data from table and set bitmap_dict
        params.clear();
        params.add(new SlotRef(null, "k2"));
        predicate = new BinaryPredicate(Operator.EQ, new SlotRef(null, "k1"),
                                        new FunctionCallExpr("bitmap_dict", params));
        desc = new DataDescription("testTable", new PartitionNames(false, Lists.newArrayList("p1", "p2")),
                                   "testHiveTable", false, Lists.newArrayList(predicate),
                null, LoadTask.MergeType.APPEND, null, null);
        desc.analyze("testDb");
        sql = "APPEND DATA FROM TABLE testHiveTable INTO TABLE testTable PARTITIONS (p1, p2) SET (`k1` = bitmap_dict(`k2`))";
        Assert.assertEquals(sql, desc.toSql());

        Map<String, String> properties = Maps.newHashMap();
        properties.put("line_delimiter", "abc");
        properties.put("fuzzy_parse", "true");
        properties.put("strip_outer_array", "true");
        properties.put("jsonpaths",  "[\"$.h1.h2.k1\",\"$.h1.h2.v1\",\"$.h1.h2.v2\"]");
        properties.put("json_root", "$.RECORDS");
        properties.put("read_json_by_line", "true");
        properties.put("num_as_string", "true");
        desc = new DataDescription("testTable", null, Lists.newArrayList("abc.txt"),
                Lists.newArrayList("col1", "col2"), new Separator(","), "json", null, false, null,
                null, null, LoadTask.MergeType.APPEND, null, null, properties);

        desc.analyze("testDb");
        Assert.assertEquals("abc", desc.getLineDelimiter());
        Assert.assertTrue(desc.isFuzzyParse());
        Assert.assertTrue(desc.isStripOuterArray());
        Assert.assertEquals("[\"$.h1.h2.k1\",\"$.h1.h2.v1\",\"$.h1.h2.v2\"]", desc.getJsonPaths());
        Assert.assertEquals("$.RECORDS", desc.getJsonRoot());
        Assert.assertTrue(desc.isNumAsString());
    }

    @Test(expected = AnalysisException.class)
    public void testNoTable() throws AnalysisException {
        DataDescription desc = new DataDescription("", null, Lists.newArrayList("abc.txt"),
                                                                  null, null, null, false, null);
        desc.analyze("testDb");
    }

    @Test(expected = AnalysisException.class)
    public void testNegMerge() throws AnalysisException {
        Expr whereExpr = new BinaryPredicate(BinaryPredicate.Operator.EQ, new IntLiteral(1),  new IntLiteral(1));

        DataDescription desc = new DataDescription("testTable", null, Lists.newArrayList("abc.txt"),
                Lists.newArrayList("col1", "col2"), new Separator(","), "csv", null, true, null, null, whereExpr, LoadTask.MergeType.MERGE, whereExpr, null, null);
        desc.analyze("testDb");
    }

    @Test(expected = AnalysisException.class)
    public void testNoFile() throws AnalysisException {
        DataDescription desc = new DataDescription("testTable", null, null, null, null, null, false, null);
        desc.analyze("testDb");
    }

    @Test(expected = AnalysisException.class)
    public void testDupCol() throws AnalysisException {
        DataDescription desc = new DataDescription("testTable", null, Lists.newArrayList("abc.txt"),
                                                                  Lists.newArrayList("col1", "col1"), null, null, false, null);
        desc.analyze("testDb");
    }

    @Test
    public void testAnalyzeColumnsWithDuplicatedColumn(@Injectable SlotRef column1,
                                                       @Injectable SlotRef column2) {
        List<String> columns = Lists.newArrayList();
        String duplicatedColumnName = "id";
        columns.add(duplicatedColumnName);
        columns.add(duplicatedColumnName);

        DataDescription dataDescription = new DataDescription(null, null, null, columns, null, null, false, null);
        try {
            Deencapsulation.invoke(dataDescription, "analyzeColumns");
            Assert.fail();
        } catch (Exception e) {
            if (!(e instanceof AnalysisException)) {
                Assert.fail();
            }
        }
    }

    @Test
    public void testAnalyzeColumnsWithDuplicatedColumnMapping(@Injectable BinaryPredicate columnMapping1,
                                                              @Injectable BinaryPredicate columnMapping2,
                                                              @Injectable SlotRef column1,
                                                              @Injectable SlotRef column2,
                                                              @Injectable FunctionCallExpr expr1,
                                                              @Injectable FunctionCallExpr expr2,
                                                              @Injectable FunctionName functionName) {
        List<String> columns = Lists.newArrayList();
        columns.add("tmp_col1");
        columns.add("tmp_col2");
        List<Expr> columnMappingList = Lists.newArrayList();
        columnMappingList.add(columnMapping1);
        columnMappingList.add(columnMapping2);
        String duplicatedColumnName = "id";
        new Expectations() {
            {
                columnMapping1.getChild(0);
                minTimes = 0;
                result = column1;
                columnMapping2.getChild(0);
                minTimes = 0;
                result = column2;
                columnMapping1.getChild(1);
                minTimes = 0;
                result = expr1;
                expr1.getFnName();
                minTimes = 0;
                result = functionName;
                functionName.getFunction();
                minTimes = 0;
                result = "test";
                column1.getColumnName();
                minTimes = 0;
                result = duplicatedColumnName;
                column2.getColumnName();
                minTimes = 0;
                result = duplicatedColumnName;
            }
        };
        DataDescription dataDescription = new DataDescription(null, null, null, columns, null, null, false,
                                                              columnMappingList);
        try {
            Deencapsulation.invoke(dataDescription, "analyzeColumns");
            Assert.fail();
        } catch (Exception e) {
            if (!(e instanceof AnalysisException)) {
                Assert.fail();
            }
        }
    }

    @Test
    public void testAnalyzeSequenceColumnNormal() throws AnalysisException {
        DataDescription desc = new DataDescription("testTable", null, Lists.newArrayList("abc.txt"),
                Lists.newArrayList("k1", "k2", "source_sequence", "v1"), new Separator("\t"),
                null, null, false, null, null, null, LoadTask.MergeType.APPEND, null, "source_sequence", null);
        new Expectations() {
            {
                tbl.getName();
                minTimes = 0;
                result = "testTable";

                tbl.hasSequenceCol();
                minTimes = 0;
                result = true;
            }
        };
        desc.analyze("testDb");
    }

    @Test(expected = AnalysisException.class)
    public void testAnalyzeSequenceColumnWithoutSourceSequence() throws AnalysisException {
        DataDescription desc = new DataDescription("testTable", null, Lists.newArrayList("abc.txt"),
                Lists.newArrayList("k1", "k2", "v1"), new Separator("\t"),
                null, null, false, null, null, null, LoadTask.MergeType.APPEND, null, "source_sequence", null);
        new Expectations() {
            {
                tbl.getName();
                minTimes = 0;
                result = "testTable";

                tbl.hasSequenceCol();
                minTimes = 0;
                result = true;
            }
        };
        desc.analyze("testDb");
    }

    @Test
    public void testMysqlLoadData() throws AnalysisException {
        TableName tbl = new TableName(null, "testDb", "testTable");
        List<Expr> params = Lists.newArrayList();
        params.add(new StringLiteral("day"));
        params.add(new SlotRef(null, "k2"));
        BinaryPredicate predicate =
                new BinaryPredicate(Operator.EQ, new SlotRef(null, "k1"), new FunctionCallExpr("bitmap_dict", params));
        Map<String, String> properties = Maps.newHashMap();
        properties.put("line_delimiter", "abc");
        DataDescription desc =
                new DataDescription(tbl, new PartitionNames(false, Lists.newArrayList("p1", "p2")), "abc.txt", true,
                        Lists.newArrayList("k1", "k2", "v1"), new Separator("010203"), new Separator("040506"), 0,
                        Lists.newArrayList(predicate), properties);
        String db = desc.analyzeFullDbName(null, analyzer);
        Assert.assertEquals("testDb", db);
        Assert.assertEquals("testDb", desc.getDbName());
        db = desc.analyzeFullDbName("testDb1", analyzer);
        Assert.assertEquals("testDb1", db);
        Assert.assertEquals("testDb1", desc.getDbName());

        desc.analyze("testDb1");
        Assert.assertEquals(1, desc.getFilePaths().size());
        Assert.assertEquals("abc.txt", desc.getFilePaths().get(0));

        Assert.assertEquals(2, desc.getPartitionNames().getPartitionNames().size());
        Assert.assertEquals("p1", desc.getPartitionNames().getPartitionNames().get(0));
        Assert.assertEquals("p2", desc.getPartitionNames().getPartitionNames().get(1));

        Assert.assertEquals("040506", desc.getLineDelimiter());
        Assert.assertEquals("010203", desc.getColumnSeparator());
        String sql = "DATA LOCAL INFILE 'abc.txt' "
                + "INTO TABLE testDb1.testTable "
                + "PARTITIONS (p1, p2) "
                + "COLUMNS TERMINATED BY '010203' "
                + "LINES TERMINATED BY '040506' "
                + "(k1, k2, v1) "
                + "SET (`k1` = bitmap_dict('day', `k2`))";
        Assert.assertEquals(sql, desc.toSql());
    }

    @Test(expected = AnalysisException.class)
    public void testHllFunctionArgsNull() throws AnalysisException {
        String functionName = FunctionSet.HLL_HASH;
        List<String> args = new ArrayList<>();
        args.add(null);

        DataDescription.validateMappingFunction(functionName, args, new HashMap<String, String>(), null, false);
    }
}
