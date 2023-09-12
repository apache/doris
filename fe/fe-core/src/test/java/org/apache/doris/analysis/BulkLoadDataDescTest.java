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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundOlapTableSink;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.stats.ExpressionEstimation;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.BinaryArithmetic;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.commands.LoadCommand;
import org.apache.doris.nereids.trees.plans.commands.info.BulkLoadDataDesc;
import org.apache.doris.nereids.trees.plans.logical.LogicalCheckPolicy;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class BulkLoadDataDescTest extends TestWithFeService {

    private List<String> sinkCols1 = new ArrayList<>();
    private List<String> sinkCols2 = new ArrayList<>();

    @Override
    protected void runBeforeAll() throws Exception {
        connectContext.getSessionVariable().enableFallbackToOriginalPlanner = false;
        connectContext.getSessionVariable().enableNereidsTimeout = false;
        connectContext.getSessionVariable().enableNereidsDML = true;
        FeConstants.runningUnitTest = true;

        createDatabase("nereids_load");
        useDatabase("nereids_load");
        String createTableSql = "CREATE TABLE `customer` (\n"
                + "  `custkey` int(11) NOT NULL,\n"
                + "  `c_name` varchar(25) NOT NULL,\n"
                + "  `c_address` varchar(40) NOT NULL,\n"
                + "  `c_nationkey` int(11) NOT NULL,\n"
                + "  `c_phone` varchar(15) NOT NULL,\n"
                + "  `c_acctbal` DECIMAL(15, 2) NOT NULL,\n"
                + "  `c_mktsegment` varchar(10) NOT NULL,\n"
                + "  `c_comment` varchar(117) NOT NULL\n"
                + ") ENGINE=OLAP\n"
                + "UNIQUE KEY(`custkey`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`custkey`) BUCKETS 24\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"function_column.sequence_col\" = \"c_nationkey\","
                + "\"storage_format\" = \"V2\"\n"
                + ");";
        createTable(createTableSql);
        sinkCols1.add("custkey");
        sinkCols1.add("c_name");
        sinkCols1.add("c_address");
        sinkCols1.add("c_nationkey");
        sinkCols1.add("c_phone");
        sinkCols1.add("c_acctbal");
        sinkCols1.add("c_mktsegment");
        sinkCols1.add("c_comment");

        String createTableSql2 = "CREATE TABLE `customer_dup` (\n"
                + "  `custkey` int(11) NOT NULL,\n"
                + "  `c_name` varchar(25) NOT NULL,\n"
                + "  `address` varchar(40) NOT NULL,\n"
                + "  `c_nationkey` int(11) NOT NULL,\n"
                + "  `c_phone` varchar(15) NOT NULL,\n"
                + "  `c_acctbal` DECIMAL(15, 2) NOT NULL,\n"
                + "  `c_mktsegment` varchar(10) NOT NULL,\n"
                + "  `c_comment` varchar(117) NOT NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`custkey`,`c_name`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`custkey`) BUCKETS 24\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"storage_format\" = \"V2\"\n"
                + ");";
        createTable(createTableSql2);
        sinkCols2.add("custkey");
        sinkCols2.add("c_name");
        sinkCols2.add("address");
        sinkCols2.add("c_nationkey");
        sinkCols2.add("c_phone");
        sinkCols2.add("c_acctbal");
        sinkCols2.add("c_mktsegment");
        sinkCols2.add("c_comment");

    }

    @Test
    public void testParseLoadStmt() throws Exception {
        String loadSql1 = "LOAD LABEL customer_j23( "
                + "     DATA INFILE(\"s3://bucket/customer\") "
                + "     INTO TABLE customer"
                + "     COLUMNS TERMINATED BY \"|\""
                + "     LINES TERMINATED BY \"\n\""
                + "     (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) "
                + "     SET ( custkey=c_custkey+1 )   "
                + "     PRECEDING FILTER c_nationkey=\"CHINA\"     "
                + "     WHERE c_custkey > 100"
                + "     ORDER BY c_custkey "
                + "  ) "
                + "  WITH S3(  "
                + "     \"s3.access_key\" = \"AK\", "
                + "     \"s3.secret_key\" = \"SK\", "
                + "     \"s3.endpoint\" = \"cos.ap-beijing.myqcloud.com\",   "
                + "     \"s3.region\" = \"ap-beijing\") "
                + "PROPERTIES( \"exec_mem_limit\" = \"8589934592\") COMMENT \"test\";";

        List<Pair<LogicalPlan, StatementContext>> statements = new NereidsParser().parseMultiple(loadSql1);
        Assertions.assertFalse(statements.isEmpty());

        List<String> expectedSinkColumns = new ArrayList<>(sinkCols1);
        expectedSinkColumns.add(Column.SEQUENCE_COL);
        List<NamedExpression> expectedProjects = new ArrayList<NamedExpression>() {
            {
                add(new UnboundAlias(new Add(
                        new UnboundSlot("c_custkey"), new TinyIntLiteral((byte) 1)), "custkey"));
                add(new UnboundSlot("c_name"));
                add(new UnboundSlot("c_address"));
                add(new UnboundSlot("c_nationkey"));
                add(new UnboundSlot("c_phone"));
                add(new UnboundSlot("c_acctbal"));
                add(new UnboundSlot("c_mktsegment"));
                add(new UnboundSlot("c_comment"));
            }
        };
        List<Expression> expectedConjuncts = new ArrayList<Expression>() {
            {
                add(new GreaterThan(new UnboundSlot("c_custkey"), new IntegerLiteral(100)));
                add(new EqualTo(new UnboundSlot("c_nationkey"), new StringLiteral("CHINA")));
            }
        };
        assertInsertIntoPlan(statements, expectedSinkColumns, expectedProjects, expectedConjuncts);
    }

    private void assertInsertIntoPlan(List<Pair<LogicalPlan, StatementContext>> statements,
                                      List<String> expectedSinkColumns,
                                      List<NamedExpression> expectedProjects,
                                      List<Expression> expectedConjuncts) throws AnalysisException {
        Assertions.assertTrue(statements.get(0).first instanceof LoadCommand);
        List<LogicalPlan> plans = ((LoadCommand) statements.get(0).first).parseToInsertIntoPlan(connectContext);
        Assertions.assertTrue(plans.get(0) instanceof UnboundOlapTableSink);
        List<String> colNames = ((UnboundOlapTableSink<?>) plans.get(0)).getColNames();
        Assertions.assertEquals(colNames.size(), expectedSinkColumns.size());
        for (String sinkCol : expectedSinkColumns) {
            Assertions.assertTrue(colNames.contains(sinkCol));
        }
        Assertions.assertTrue(plans.get(0).child(0) instanceof LogicalProject);
        LogicalProject<?> project = ((LogicalProject<?>) plans.get(0).child(0));
        Set<String> projects = project.getProjects().stream()
                .map(Object::toString)
                .collect(Collectors.toSet());
        for (NamedExpression namedExpression : expectedProjects) {
            Assertions.assertTrue(projects.contains(namedExpression.toString()));
        }
        Assertions.assertTrue(project.child(0) instanceof LogicalFilter);
        LogicalFilter<?> filter = ((LogicalFilter<?>) project.child(0));
        Set<String> filterConjuncts = filter.getConjuncts().stream()
                .map(Object::toString)
                .collect(Collectors.toSet());
        for (Expression expectedConjunct : expectedConjuncts) {
            Assertions.assertTrue(filterConjuncts.contains(expectedConjunct.toString()));
        }
        Assertions.assertTrue(filter.child(0) instanceof LogicalCheckPolicy);
        Assertions.assertTrue(filter.child(0).child(0) instanceof UnboundTVFRelation);
    }

    @Test
    public void testParseLoadStmtPartitions() throws Exception {
        String loadSql1 = "LOAD LABEL customer_j23( "
                + "     DATA INFILE(\"s3://bucket/customer\") "
                + "     INTO TABLE customer"
                + "     PARTITION (c_name, dt) "
                + "     COLUMNS TERMINATED BY \"|\""
                + "     LINES TERMINATED BY \"\n\""
                + "     (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, dt) "
                + "     ORDER BY c_custkey "
                + "  ) "
                + "  WITH S3(  "
                + "     \"s3.access_key\" = \"AK\", "
                + "     \"s3.secret_key\" = \"SK\", "
                + "     \"s3.endpoint\" = \"cos.ap-beijing.myqcloud.com\",   "
                + "     \"s3.region\" = \"ap-beijing\") "
                + "PROPERTIES( \"exec_mem_limit\" = \"8589934592\") COMMENT \"test\";";
        List<Pair<LogicalPlan, StatementContext>> statements = new NereidsParser().parseMultiple(loadSql1);
        Assertions.assertFalse(statements.isEmpty());

        List<String> expectedSinkColumns = new ArrayList<>(sinkCols1);
        expectedSinkColumns.add(Column.SEQUENCE_COL);
        expectedSinkColumns.add(Column.DELETE_SIGN);
        List<NamedExpression> expectedProjects = new ArrayList<NamedExpression>() {
            {
                add(new UnboundSlot("c_custkey"));
                add(new UnboundSlot("c_name"));
                add(new UnboundSlot("c_address"));
                add(new UnboundSlot("c_nationkey"));
                add(new UnboundSlot("c_phone"));
                add(new UnboundSlot("c_acctbal"));
                add(new UnboundSlot("c_mktsegment"));
                add(new UnboundSlot("c_comment"));
                add(new UnboundSlot("dt"));
            }
        };
        List<Expression> expectedConjuncts = new ArrayList<>();
        assertInsertIntoPlan(statements, expectedSinkColumns, expectedProjects, expectedConjuncts);
    }

    @Test
    public void testParseLoadStmtColumFromPath() throws Exception {
        String loadSql1 = "LOAD LABEL customer_j23( "
                + "     DATA INFILE(\"s3://bucket/customer\") "
                + "     INTO TABLE customer"
                + "     PARTITION (c_name, dt) "
                + "     COLUMNS TERMINATED BY \"|\""
                + "     LINES TERMINATED BY \"\n\""
                + "     (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, dt) "
                + "     COLUMNS FROM PATH AS (pt)   "
                + "     SET ( custkey=c_custkey+1 )   "
                + "     PRECEDING FILTER c_nationkey=\"CHINA\"     "
                + "     WHERE c_custkey > 100"
                + "     ORDER BY c_custkey "
                + "  ) "
                + "  WITH S3(  "
                + "     \"s3.access_key\" = \"AK\", "
                + "     \"s3.secret_key\" = \"SK\", "
                + "     \"s3.endpoint\" = \"cos.ap-beijing.myqcloud.com\",   "
                + "     \"s3.region\" = \"ap-beijing\") "
                + "PROPERTIES( \"exec_mem_limit\" = \"8589934592\") COMMENT \"test\";";
        List<Pair<LogicalPlan, StatementContext>> statements = new NereidsParser().parseMultiple(loadSql1);
        Assertions.assertFalse(statements.isEmpty());

        List<String> expectedSinkColumns = new ArrayList<>(sinkCols1);
        expectedSinkColumns.add(Column.SEQUENCE_COL);
        expectedSinkColumns.add(Column.DELETE_SIGN);
        expectedSinkColumns.add("pt");
        List<NamedExpression> expectedProjects = new ArrayList<NamedExpression>() {
            {
                add(new UnboundAlias(new Add(
                        new UnboundSlot("c_custkey"), new TinyIntLiteral((byte) 1)), "custkey"));
                add(new UnboundSlot("c_name"));
                add(new UnboundSlot("c_address"));
                add(new UnboundSlot("c_nationkey"));
                add(new UnboundSlot("c_phone"));
                add(new UnboundSlot("c_acctbal"));
                add(new UnboundSlot("c_mktsegment"));
                add(new UnboundSlot("c_comment"));
                add(new UnboundSlot("pt"));
            }
        };
        List<Expression> expectedConjuncts = new ArrayList<Expression>() {
            {
                add(new GreaterThan(new UnboundSlot("c_custkey"), new IntegerLiteral(100)));
                add(new EqualTo(new UnboundSlot("c_nationkey"), new StringLiteral("CHINA")));
            }
        };
        assertInsertIntoPlan(statements, expectedSinkColumns, expectedProjects, expectedConjuncts);
    }

    @Test
    public void testParseLoadStmtNoColumn() throws Exception {
        String loadSql1 = "LOAD LABEL customer_no_col( "
                + "     DATA INFILE(\"s3://bucket/customer\") "
                + "     INTO TABLE customer"
                + "     FORMAT AS CSV"
                + "     ORDER BY custkey "
                + "  ) "
                + "  WITH S3(  "
                + "     \"s3.access_key\" = \"AK\", "
                + "     \"s3.secret_key\" = \"SK\", "
                + "     \"s3.endpoint\" = \"cos.ap-beijing.myqcloud.com\",   "
                + "     \"s3.region\" = \"ap-beijing\") "
                + "PROPERTIES( \"exec_mem_limit\" = \"8589934592\") COMMENT \"test\";";

        List<Pair<LogicalPlan, StatementContext>> statements = new NereidsParser().parseMultiple(loadSql1);
        Assertions.assertFalse(statements.isEmpty());
        List<String> expectedSinkColumns = new ArrayList<>();
        List<NamedExpression> expectedProjects = new ArrayList<NamedExpression>() {
            {
                add(new UnboundStar(ImmutableList.of()));
            }
        };
        List<Expression> expectedConjuncts = new ArrayList<>();
        assertInsertIntoPlan(statements, expectedSinkColumns, expectedProjects, expectedConjuncts);

        // k1:int;k2:bigint;k3:varchar(20);k4:datetime(6)
        String loadSql2 = "LOAD LABEL customer_no_col2( "
                + "     DATA INFILE(\"s3://bucket/customer\") "
                + "     INTO TABLE customer"
                + "     FORMAT AS CSV"
                + "     ORDER BY custkey "
                + "     PROPERTIES( "
                + "         \"csv_schema\" = \""
                + "             custkey:INT;"
                + "             c_name:STRING;"
                + "             c_address:STRING;"
                + "             c_nationkey:INT;"
                + "             c_phone:STRING;"
                + "             c_acctbal:DECIMAL(15, 2);"
                + "             c_mktsegment:STRING;"
                + "             c_comment:STRING;\""
                + "     ) "
                + "  ) "
                + "  WITH S3(  "
                + "     \"s3.access_key\" = \"AK\", "
                + "     \"s3.secret_key\" = \"SK\", "
                + "     \"s3.endpoint\" = \"cos.ap-beijing.myqcloud.com\",   "
                + "     \"s3.region\" = \"ap-beijing\") "
                + "PROPERTIES( \"exec_mem_limit\" = \"8589934592\") COMMENT \"test\";";

        List<Pair<LogicalPlan, StatementContext>> statements2 = new NereidsParser().parseMultiple(loadSql2);
        Assertions.assertFalse(statements2.isEmpty());
        List<String> expectedSinkColumns2 = new ArrayList<>(sinkCols1);
        expectedSinkColumns2.add(Column.SEQUENCE_COL);
        List<NamedExpression> expectedProjects2 = new ArrayList<NamedExpression>() {
            {
                add(new UnboundSlot("custkey"));
                add(new UnboundSlot("c_name"));
                add(new UnboundSlot("c_address"));
                add(new UnboundSlot("c_nationkey"));
                add(new UnboundSlot("c_phone"));
                add(new UnboundSlot("c_acctbal"));
                add(new UnboundSlot("c_mktsegment"));
                add(new UnboundSlot("c_comment"));
            }
        };
        List<Expression> expectedConjuncts2 = new ArrayList<>();
        assertInsertIntoPlan(statements2, expectedSinkColumns2, expectedProjects2, expectedConjuncts2);
    }

    @Test
    public void testParseLoadStmtWithParquetMappingFilter() throws Exception {
        String loadSql1 = "LOAD LABEL customer_dup_mapping( "
                + "     DATA INFILE(\"s3://bucket/customer\") "
                + "     INTO TABLE customer_dup"
                + "     FORMAT AS PARQUET"
                + "     (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) "
                + "     SET ( custkey=c_custkey+1, address=c_address+'_base')   "
                + "     PRECEDING FILTER c_nationkey=\"CHINA\"     "
                + "     WHERE c_custkey = 100"
                + "  ) "
                + "  WITH S3(  "
                + "     \"s3.access_key\" = \"AK\", "
                + "     \"s3.secret_key\" = \"SK\", "
                + "     \"s3.endpoint\" = \"cos.ap-beijing.myqcloud.com\",   "
                + "     \"s3.region\" = \"ap-beijing\") "
                + "PROPERTIES( \"exec_mem_limit\" = \"8589934592\") COMMENT \"test\";";
        List<Pair<LogicalPlan, StatementContext>> statements = new NereidsParser().parseMultiple(loadSql1);
        Assertions.assertFalse(statements.isEmpty());
        List<String> expectedSinkColumns = new ArrayList<>(sinkCols2);
        List<NamedExpression> expectedProjects = new ArrayList<NamedExpression>() {
            {
                add(new UnboundAlias(new Add(
                        new UnboundSlot("c_custkey"), new TinyIntLiteral((byte) 1)), "custkey"));
                add(new UnboundSlot("c_name"));
                add(new UnboundAlias(new Add(
                        new UnboundSlot("c_address"), new StringLiteral("_base")), "address"));
                add(new UnboundSlot("c_nationkey"));
                add(new UnboundSlot("c_phone"));
                add(new UnboundSlot("c_acctbal"));
                add(new UnboundSlot("c_mktsegment"));
                add(new UnboundSlot("c_comment"));
            }
        };
        List<Expression> expectedConjuncts = new ArrayList<Expression>() {
            {
                add(new EqualTo(new UnboundSlot("c_custkey"), new IntegerLiteral(100)));
                add(new EqualTo(new UnboundSlot("c_nationkey"), new StringLiteral("CHINA")));

            }
        };
        assertInsertIntoPlan(statements, expectedSinkColumns, expectedProjects, expectedConjuncts);
    }

    @Test
    public void testParseLoadStmtWithDeleteOn() throws Exception {
        String loadSqlWithDeleteOnErr1 = "LOAD LABEL customer_label1( "
                + "     APPEND DATA INFILE(\"s3://bucket/customer\") "
                + "     INTO TABLE customer"
                + "     COLUMNS TERMINATED BY \"|\""
                + "     (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) "
                + "     SET ( custkey=c_custkey+1 )   "
                + "     PRECEDING FILTER c_nationkey=\"CHINA\"     "
                + "     WHERE c_custkey > 100"
                + "     DELETE ON c_custkey < 120     "
                + "     ORDER BY c_custkey "
                + "  ) "
                + "  WITH S3(  "
                + "     \"s3.access_key\" = \"AK\", "
                + "     \"s3.secret_key\" = \"SK\", "
                + "     \"s3.endpoint\" = \"cos.ap-beijing.myqcloud.com\",   "
                + "     \"s3.region\" = \"ap-beijing\") "
                + "PROPERTIES( \"exec_mem_limit\" = \"8589934592\") COMMENT \"test\";";
        try {
            new StmtExecutor(connectContext, loadSqlWithDeleteOnErr1).execute();
        } catch (AnalysisException e) {
            Assertions.assertTrue(e.getCause() instanceof IllegalArgumentException);
            Assertions.assertEquals(BulkLoadDataDesc.EXPECT_MERGE_DELETE_ON, e.getCause().getMessage());
        }

        String loadSqlWithDeleteOnErr2 = "LOAD LABEL customer_label1( "
                + "     MERGE DATA INFILE(\"s3://bucket/customer\") "
                + "     INTO TABLE customer"
                + "     COLUMNS TERMINATED BY \"|\""
                + "     (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) "
                + "     SET ( custkey=c_custkey+1 )   "
                + "     PRECEDING FILTER c_nationkey=\"CHINA\"     "
                + "     WHERE c_custkey > 100"
                + "     ORDER BY c_custkey "
                + "  ) "
                + "  WITH S3(  "
                + "     \"s3.access_key\" = \"AK\", "
                + "     \"s3.secret_key\" = \"SK\", "
                + "     \"s3.endpoint\" = \"cos.ap-beijing.myqcloud.com\",   "
                + "     \"s3.region\" = \"ap-beijing\") "
                + "PROPERTIES( \"exec_mem_limit\" = \"8589934592\") COMMENT \"test\";";
        try {
            new StmtExecutor(connectContext, loadSqlWithDeleteOnErr2).execute();
        } catch (AnalysisException e) {
            Assertions.assertTrue(e.getCause() instanceof IllegalArgumentException);
            Assertions.assertEquals(BulkLoadDataDesc.EXPECT_DELETE_ON, e.getCause().getMessage());
        }

        String loadSqlWithDeleteOnOk = "LOAD LABEL customer_label2( "
                + "     MERGE DATA INFILE(\"s3://bucket/customer\") "
                + "     INTO TABLE customer"
                + "     COLUMNS TERMINATED BY \"|\""
                + "     (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) "
                + "     SET ( custkey=c_custkey+1 )   "
                + "     PRECEDING FILTER c_nationkey=\"CHINA\"     "
                + "     WHERE custkey > 100"
                + "     DELETE ON c_custkey < 120     "
                + "     ORDER BY c_custkey "
                + "  ) "
                + "  WITH S3(  "
                + "     \"s3.access_key\" = \"AK\", "
                + "     \"s3.secret_key\" = \"SK\", "
                + "     \"s3.endpoint\" = \"cos.ap-beijing.myqcloud.com\",   "
                + "     \"s3.region\" = \"ap-beijing\") "
                + "PROPERTIES( \"exec_mem_limit\" = \"8589934592\") COMMENT \"test\";";

        FeConstants.unitTestConstant = new ArrayList<Column>() {
            {
                add(new Column("c_custkey", PrimitiveType.INT, true));
                add(new Column("c_name", PrimitiveType.VARCHAR, true));
                add(new Column("c_address", PrimitiveType.VARCHAR, true));
                add(new Column("c_nationkey", PrimitiveType.INT, true));
                add(new Column("c_phone", PrimitiveType.VARCHAR, true));
                add(new Column("c_acctbal", PrimitiveType.DECIMALV2, true));
                add(new Column("c_mktsegment", PrimitiveType.VARCHAR, true));
                add(new Column("c_comment", PrimitiveType.VARCHAR, true));
            }
        };
        new MockUp<ExpressionEstimation>(ExpressionEstimation.class) {
            @Mock
            public ColumnStatistic visitCast(Cast cast, Statistics context) {
                return ColumnStatistic.UNKNOWN;
            }

            @Mock
            public ColumnStatistic visitBinaryArithmetic(BinaryArithmetic binaryArithmetic, Statistics context) {
                return ColumnStatistic.UNKNOWN;
            }
        };

        List<Pair<LogicalPlan, StatementContext>> statements = new NereidsParser().parseMultiple(loadSqlWithDeleteOnOk);
        Assertions.assertFalse(statements.isEmpty());

        List<String> expectedSinkColumns = new ArrayList<>(sinkCols1);
        expectedSinkColumns.add(Column.SEQUENCE_COL);
        List<NamedExpression> expectedProjects = new ArrayList<>();
        List<Expression> expectedConjuncts = new ArrayList<>();
        assertInsertIntoPlan(statements, expectedSinkColumns, expectedProjects, expectedConjuncts);
        // new StmtExecutor(connectContext, loadSqlWithDeleteOnOk).execute();
    }

    @Test
    public void testParseLoadStmtPatternPath() throws Exception {
        String path1 = "part*";
        String path2 = "*/part_000";
        String path3 = "*part_000*";
        String path4 = "*/*part_000*";
        String loadTemplate = "LOAD LABEL customer_j23( "
                + "     DATA INFILE(\"s3://bucket/customer/PATTERN\") "
                + "     INTO TABLE customer"
                + "     PARTITION (c_name, dt) "
                + "     COLUMNS TERMINATED BY \"|\""
                + "     LINES TERMINATED BY \"\n\""
                + "     (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, dt) "
                + "     ORDER BY c_custkey "
                + "  ) "
                + "  WITH S3(  "
                + "     \"s3.access_key\" = \"AK\", "
                + "     \"s3.secret_key\" = \"SK\", "
                + "     \"s3.endpoint\" = \"cos.ap-beijing.myqcloud.com\",   "
                + "     \"s3.region\" = \"ap-beijing\") "
                + "PROPERTIES( \"exec_mem_limit\" = \"8589934592\") COMMENT \"test\";";
        Assertions.assertFalse(new NereidsParser()
                .parseMultiple(loadTemplate.replace("PATTERN", path1)).isEmpty());
        Assertions.assertFalse(new NereidsParser()
                .parseMultiple(loadTemplate.replace("PATTERN", path2)).isEmpty());
        Assertions.assertFalse(new NereidsParser()
                .parseMultiple(loadTemplate.replace("PATTERN", path3)).isEmpty());
        Assertions.assertFalse(new NereidsParser()
                .parseMultiple(loadTemplate.replace("PATTERN", path4)).isEmpty());
    }

    @Test
    public void testParseLoadStmtMultiLocations() throws Exception {
        String loadMultiLocations = "LOAD LABEL customer_j23( "
                + "     DATA INFILE("
                + "         \"s3://bucket/customer/path1\", "
                + "         \"s3://bucket/customer/path2\", "
                + "         \"s3://bucket/customer/path3\") "
                + "     INTO TABLE customer"
                + "     PARTITION (c_name, dt) "
                + "     COLUMNS TERMINATED BY \"|\""
                + "     LINES TERMINATED BY \"\n\""
                + "     (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, dt) "
                + "     ORDER BY c_custkey "
                + "  ) "
                + "  WITH S3(  "
                + "     \"s3.access_key\" = \"AK\", "
                + "     \"s3.secret_key\" = \"SK\", "
                + "     \"s3.endpoint\" = \"cos.ap-beijing.myqcloud.com\",   "
                + "     \"s3.region\" = \"ap-beijing\") "
                + "PROPERTIES( \"exec_mem_limit\" = \"8589934592\") COMMENT \"test\";";
        Assertions.assertFalse(new NereidsParser()
                .parseMultiple(loadMultiLocations).isEmpty());
    }

    @Test
    public void testParseLoadStmtMultiBulkDesc() throws Exception {
        String loadMultiLocations = "LOAD LABEL customer_j23( "
                + "     DATA INFILE("
                + "         \"s3://bucket/customer/path1\", "
                + "         \"s3://bucket/customer/path2\", "
                + "         \"s3://bucket/customer/path3\") "
                + "     INTO TABLE customer"
                + "     PARTITION (c_name) "
                + "     COLUMNS TERMINATED BY \"|\""
                + "     (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) "
                + "     ORDER BY c_custkey "
                + "     ,"
                + "     DATA INFILE(\"s3://bucket/customer/par_a*\") "
                + "     INTO TABLE customer_dup"
                + "     FORMAT AS PARQUET"
                + "     (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) "
                + "     SET ( custkey=c_custkey+1, address=c_address+'_base')   "
                + "     WHERE c_custkey < 50"
                + "     ,"
                + "     DATA INFILE("
                + "         \"s3://bucket/customer/p\") "
                + "     INTO TABLE customer"
                + "     PARTITION (c_name, dt) "
                + "     COLUMNS TERMINATED BY \"|\""
                + "     LINES TERMINATED BY \"\n\""
                + "     (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, dt)"
                + "     SET ( custkey=c_custkey+1 )   "
                + "     PRECEDING FILTER c_nationkey=\"CHINA\"     "
                + "     WHERE c_custkey > 100"
                + "     ORDER BY c_custkey "
                + "  ) "
                + "  WITH S3(  "
                + "     \"s3.access_key\" = \"AK\", "
                + "     \"s3.secret_key\" = \"SK\", "
                + "     \"s3.endpoint\" = \"cos.ap-beijing.myqcloud.com\",   "
                + "     \"s3.region\" = \"ap-beijing\") "
                + "PROPERTIES( \"exec_mem_limit\" = \"8589934592\") COMMENT \"test\";";
        Assertions.assertFalse(new NereidsParser()
                .parseMultiple(loadMultiLocations).isEmpty());
    }
}
