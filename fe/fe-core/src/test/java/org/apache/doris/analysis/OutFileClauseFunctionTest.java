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

import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OutFileClauseFunctionTest extends TestWithFeService {
    private static final String DB_NAME = "db1";

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 10;
        FeConstants.runningUnitTest = true;
        Config.enable_outfile_to_local = true;
        connectContext = createDefaultCtx();
        createDatabase(DB_NAME);
        useDatabase(DB_NAME);
        String createTableSQL = "create table " + DB_NAME
                + ".test  (k1 int, k2 varchar ) "
                + "distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        createTable(createTableSQL);
    }

    @Test
    public void testSelectStmtOutFileClause() throws Exception {
        String query1 = "select * from db1.test into outfile \"file:///" + runningDir + "/result_\";";
        QueryStmt analyzedQueryStmt = createStmt(query1);
        Assertions.assertTrue(analyzedQueryStmt.hasOutFileClause());
        OutFileClause outFileClause = analyzedQueryStmt.getOutFileClause();
        boolean isOutFileClauseAnalyzed = Deencapsulation.getField(outFileClause, "isAnalyzed");
        Assertions.assertTrue(isOutFileClauseAnalyzed);
        Assertions.assertEquals(outFileClause.getFileFormatType(), TFileFormatType.FORMAT_CSV_PLAIN);
    }

    @Test
    public void testSetOperationStmtOutFileClause() throws Exception {
        String query1 = "select * from db1.test union select * from db1.test into outfile \"file:///" + runningDir + "/result_\";";
        QueryStmt analyzedSetOperationStmt = createStmt(query1);
        Assertions.assertTrue(analyzedSetOperationStmt.hasOutFileClause());
        OutFileClause outFileClause = analyzedSetOperationStmt.getOutFileClause();
        boolean isOutFileClauseAnalyzed = Deencapsulation.getField(outFileClause, "isAnalyzed");
        Assertions.assertTrue(isOutFileClauseAnalyzed);
        Assertions.assertEquals(outFileClause.getFileFormatType(), TFileFormatType.FORMAT_CSV_PLAIN);
    }

    @Test
    public void testParquetFormat() throws Exception {
        String query1 = "select * from db1.test union select * from db1.test into outfile \"file:///" + runningDir + "/result_\" FORMAT AS PARQUET;";
        QueryStmt analyzedSetOperationStmt = createStmt(query1);
        Assertions.assertTrue(analyzedSetOperationStmt.hasOutFileClause());
        OutFileClause outFileClause = analyzedSetOperationStmt.getOutFileClause();
        boolean isOutFileClauseAnalyzed = Deencapsulation.getField(outFileClause, "isAnalyzed");
        Assertions.assertTrue(isOutFileClauseAnalyzed);
        Assertions.assertEquals(outFileClause.getFileFormatType(), TFileFormatType.FORMAT_PARQUET);
    }

    @Test
    public void testHdfsFile() throws Exception {
        String loc1 = "'hdfs://hacluster/data/test/'";
        String loc2 = "'hdfs:///data/test/'";

        String query1 = "select * from db1.test \n"
                + "into outfile "
                + loc1
                + "\n"
                + "format as csv\n"
                + "properties(\n"
                + "'column_separator' = ',',\n"
                + "'line_delimiter' = '\\n',\n"
                + "'broker.name' = 'broker',\n"
                + "'broker.fs.defaultFS'='hdfs://hacluster/',\n"
                + "'broker.dfs.nameservices'='hacluster',\n"
                + "'broker.dfs.ha.namenodes.hacluster'='n1,n2',\n"
                + "'broker.dfs.namenode.rpc-address.hacluster.n1'='master1:8020',\n"
                + "'broker.dfs.namenode.rpc-address.hacluster.n2'='master2:8020',\n"
                + "'broker.dfs.client.failover.proxy.provider.hacluster'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'"
                + ");";
        QueryStmt analyzedOutStmt = createStmt(query1);
        Assertions.assertTrue(analyzedOutStmt.hasOutFileClause());

        OutFileClause outFileClause = analyzedOutStmt.getOutFileClause();
        boolean isOutFileClauseAnalyzed = Deencapsulation.getField(outFileClause, "isAnalyzed");
        Assertions.assertTrue(isOutFileClauseAnalyzed);

        QueryStmt analyzedOutStmtLoc2 = createStmt(query1.replace(loc1, loc2));
        Assertions.assertTrue(analyzedOutStmtLoc2.hasOutFileClause());

        String query2 = "select * from db1.test \n"
                + "into outfile "
                + loc1
                + "\n"
                + "format as csv\n"
                + "properties(\n"
                + "'column_separator' = ',',\n"
                + "'line_delimiter' = '\\n',\n"
                + "'fs.defaultFS'='hdfs://hacluster/',\n"
                + "'dfs.nameservices'='hacluster',\n"
                + "'dfs.ha.namenodes.hacluster'='n1,n2',\n"
                + "'dfs.namenode.rpc-address.hacluster.n1'='master1:8020',\n"
                + "'dfs.namenode.rpc-address.hacluster.n2'='master2:8020',\n"
                + "'dfs.client.failover.proxy.provider.hacluster'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider'"
                + ");";
        QueryStmt analyzedOutStmt2 = createStmt(query2);
        Assertions.assertTrue(analyzedOutStmt2.hasOutFileClause());

        OutFileClause outFileClause2 = analyzedOutStmt2.getOutFileClause();
        boolean isOutFileClauseAnalyzed2 = Deencapsulation.getField(outFileClause2, "isAnalyzed");
        Assertions.assertTrue(isOutFileClauseAnalyzed2);

        QueryStmt analyzedOutStmt2Loc2 = createStmt(query2.replace(loc1, loc2));
        Assertions.assertTrue(analyzedOutStmt2Loc2.hasOutFileClause());
    }
}
