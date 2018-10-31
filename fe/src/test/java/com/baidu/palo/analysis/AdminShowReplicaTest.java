package com.baidu.palo.analysis;

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
            showStmt = (AdminShowReplicaStatusStmt) parser.parse().value;
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
