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
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class BulkLoadDataDescTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        connectContext.getSessionVariable().enableFallbackToOriginalPlanner = false;
        connectContext.getSessionVariable().enableNereidsTimeout = false;
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
                + "\"storage_format\" = \"V2\"\n"
                + ");";
        createTable(createTableSql);

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

    }

    @Test
    public void testParseLoadStmt() throws Exception {
        String loadSql1 = "LOAD LABEL customer_j23( "
                + "     DATA INFILE(\"s3://bucket/customer\") "
                + "     INTO TABLE customer"
                + "     COLUMNS TERMINATED BY \"|\""
                + "     LINES TERMINATED BY \"\n\""
                + "     (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, temp) "
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
                + "  ) "
                + "  WITH S3(  "
                + "     \"s3.access_key\" = \"AK\", "
                + "     \"s3.secret_key\" = \"SK\", "
                + "     \"s3.endpoint\" = \"cos.ap-beijing.myqcloud.com\",   "
                + "     \"s3.region\" = \"ap-beijing\") "
                + "PROPERTIES( \"exec_mem_limit\" = \"8589934592\") COMMENT \"test\";";
        List<Pair<LogicalPlan, StatementContext>> statements = new NereidsParser().parseMultiple(loadSql1);
        Assertions.assertFalse(statements.isEmpty());
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
                + "  ) "
                + "  WITH S3(  "
                + "     \"s3.access_key\" = \"AK\", "
                + "     \"s3.secret_key\" = \"SK\", "
                + "     \"s3.endpoint\" = \"cos.ap-beijing.myqcloud.com\",   "
                + "     \"s3.region\" = \"ap-beijing\") "
                + "PROPERTIES( \"exec_mem_limit\" = \"8589934592\") COMMENT \"test\";";
        List<Pair<LogicalPlan, StatementContext>> statements = new NereidsParser().parseMultiple(loadSql1);
        Assertions.assertFalse(statements.isEmpty());
    }

    @Test
    public void testParseLoadStmtNoColumn() throws Exception {
        String loadSql1 = "LOAD LABEL customer_no_col( "
                + "     DATA INFILE(\"s3://bucket/customer\") "
                + "     INTO TABLE customer"
                + "     FORMAT AS ORC"
                + "  ) "
                + "  WITH S3(  "
                + "     \"s3.access_key\" = \"AK\", "
                + "     \"s3.secret_key\" = \"SK\", "
                + "     \"s3.endpoint\" = \"cos.ap-beijing.myqcloud.com\",   "
                + "     \"s3.region\" = \"ap-beijing\") "
                + "PROPERTIES( \"exec_mem_limit\" = \"8589934592\") COMMENT \"test\";";

        List<Pair<LogicalPlan, StatementContext>> statements = new NereidsParser().parseMultiple(loadSql1);
        Assertions.assertFalse(statements.isEmpty());
    }

    @Test
    public void testParseLoadStmtWithParquetMappingFilter() throws Exception {
        String loadSql1 = "LOAD LABEL customer_dup_mapping( "
                + "     DATA INFILE(\"s3://bucket/customer\") "
                + "     INTO TABLE customer_dup"
                + "     FORMAT AS PARQUET"
                + "     (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, temp) "
                + "     SET ( custkey=c_custkey+1, address=c_address+'_base')   "
                + "     PRECEDING FILTER c_nationkey=\"CHINA\"     "
                + "     WHERE c_custkey = 100"
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

        FeConstants.unitTestConstant = new ArrayList<Column>() {{
            add(new Column("c_custkey", PrimitiveType.INT));
            add(new Column("c_name", PrimitiveType.VARCHAR));
            add(new Column("c_address", PrimitiveType.VARCHAR));
            add(new Column("c_nationkey", PrimitiveType.INT));
            add(new Column("c_phone", PrimitiveType.VARCHAR));
            add(new Column("c_acctbal", PrimitiveType.DECIMALV2));
            add(new Column("c_mktsegment", PrimitiveType.VARCHAR));
            add(new Column("c_comment", PrimitiveType.VARCHAR));
        }};
        List<Pair<LogicalPlan, StatementContext>> statements = new NereidsParser().parseMultiple(loadSqlWithDeleteOnOk);
        Assertions.assertFalse(statements.isEmpty());
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