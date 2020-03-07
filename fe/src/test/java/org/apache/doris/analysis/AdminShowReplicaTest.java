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

import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;
import java.lang.reflect.Method;

public class AdminShowReplicaTest {

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
