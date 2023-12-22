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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.qe.ShowExecutor;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.lang.reflect.Method;

public class AdminShowReplicaTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("create table test.tbl1\n"
                + "(k1 date, k2 int)\n"
                + "partition by range(k1)\n"
                + "(\n"
                + "    partition p1 values less than(\"2021-07-01\"),\n"
                + "    partition p2 values less than(\"2021-08-01\")\n"
                + ")\n" + "distributed by hash(k2) buckets 10\n"
                + "properties(\"replication_num\" = \"1\");");
    }

    @Test
    public void testShowReplicaDistribution() throws Exception {
        String stmtStr = "admin show replica distribution from test.tbl1 partition(p1)";
        AdminShowReplicaDistributionStmt stmt = (AdminShowReplicaDistributionStmt) parseAndAnalyzeStmt(
                stmtStr);
        ShowExecutor executor = new ShowExecutor(connectContext, stmt);
        ShowResultSet resultSet = executor.execute();
        Assert.assertEquals(1, resultSet.getResultRows().size());
        Assert.assertEquals(7, resultSet.getResultRows().get(0).size());

        stmtStr = "show data skew from test.tbl1 partition(p1)";
        ShowDataSkewStmt skewStmt = (ShowDataSkewStmt) parseAndAnalyzeStmt(stmtStr);
        executor = new ShowExecutor(connectContext, skewStmt);
        resultSet = executor.execute();
        Assert.assertEquals(10, resultSet.getResultRows().size());
        Assert.assertEquals(5, resultSet.getResultRows().get(0).size());

        // update tablets' data size and row count
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("test");
        OlapTable olapTable = db.getOlapTableOrAnalysisException("tbl1");
        for (Partition partition : olapTable.getPartitions()) {
            for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
                for (Tablet tablet : index.getTablets()) {
                    for (Replica replica : tablet.getReplicas()) {
                        replica.updateStat(1024, 2);
                    }
                }
            }
        }

        executor = new ShowExecutor(connectContext, stmt);
        resultSet = executor.execute();
        Assert.assertEquals(1, resultSet.getResultRows().size());
        Assert.assertEquals(7, resultSet.getResultRows().get(0).size());

        executor = new ShowExecutor(connectContext, skewStmt);
        resultSet = executor.execute();
        Assert.assertEquals(10, resultSet.getResultRows().size());
        Assert.assertEquals("4", resultSet.getResultRows().get(4).get(0));
        Assert.assertEquals(5, resultSet.getResultRows().get(0).size());
    }

    @Test
    public void testShowReplicaStatus() {
        String stmt = new String("ADMIN SHOW REPLICA STATUS FROM db.tbl1 WHERE status = 'ok'");
        testAnalyzeWhere(stmt, true);

        stmt = new String("ADMIN SHOW REPLICA STATUS FROM db.tbl1 WHERE status != 'ok'");
        testAnalyzeWhere(stmt, true);

        stmt = new String("ADMIN SHOW REPLICA STATUS FROM db.tbl1 WHERE status = 'dead'");
        testAnalyzeWhere(stmt, true);

        stmt = new String("ADMIN SHOW REPLICA STATUS FROM db.tbl1 WHERE status != 'VERSION_ERROR'");
        testAnalyzeWhere(stmt, true);

        stmt = new String("ADMIN SHOW REPLICA STATUS FROM db.tbl1 WHERE status = 'MISSING'");
        testAnalyzeWhere(stmt, true);

        stmt = new String("ADMIN SHOW REPLICA STATUS FROM db.tbl1 WHERE status = 'missing'");
        testAnalyzeWhere(stmt, true);

        stmt = new String("ADMIN SHOW REPLICA STATUS FROM db.tbl1 WHERE status != 'what'");
        testAnalyzeWhere(stmt, false);

        stmt = new String("ADMIN SHOW REPLICA STATUS FROM db.tbl1 WHERE status = 'how'");
        testAnalyzeWhere(stmt, false);
    }

    private void testAnalyzeWhere(String stmt, boolean correct) {
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(stmt)));
        AdminShowReplicaStatusStmt showStmt = null;
        try {
            showStmt = (AdminShowReplicaStatusStmt) SqlParserUtils.getFirstStmt(parser);
        } catch (Error e) {
            Assert.fail(e.getMessage());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        try {
            Method method = AdminShowReplicaStatusStmt.class.getDeclaredMethod("analyzeWhere");
            method.setAccessible(true);
            if (!(Boolean) method.invoke(showStmt)) {
                if (correct) {
                    Assert.fail();
                }
                return;
            }
        } catch (Exception e) {
            if (tryAssert(correct, e)) {
                return;
            }
        }
        if (!correct) {
            Assert.fail();
        }
    }

    private boolean tryAssert(boolean correct, Exception e) {
        if (correct) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
        return true;
    }

}
