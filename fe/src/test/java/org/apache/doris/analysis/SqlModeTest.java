package org.apache.doris.analysis;

import org.apache.doris.qe.SqlModeHelper;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;

public class SqlModeTest {

    @Test
    public void testDefaultMode() {
        String stmt = new String("SELECT * FROM db1.tbl1 WHERE name = 'BILL GATES'");
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(stmt)));
        SelectStmt selectStmt = null;
        try {
            selectStmt = (SelectStmt) parser.parse().value;
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertEquals("SELECT  FROM `db1`.`tbl1` WHERE `name` = 'BILL GATES'", selectStmt.toSql());
        parser = new SqlParser(new SqlScanner(new StringReader(stmt), SqlModeHelper.MODE_DEFAULT));
        try {
            selectStmt = (SelectStmt) parser.parse().value;
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        Assert.assertEquals("SELECT  FROM `db1`.`tbl1` WHERE `name` = 'BILL GATES'", selectStmt.toSql());
    }

    @Test
    public void testPipesAsConcatMode() {
        String stmt = new String("SELECT 'a' || 'b' || 'c'");
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(stmt), SqlModeHelper.MODE_PIPES_AS_CONCAT));
        SelectStmt selectStmt = null;
        try {
            selectStmt = (SelectStmt) parser.parse().value;
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        Expr e = selectStmt.getSelectList().getItems().get(0).getExpr();
        if (!(e instanceof FunctionCallExpr)) {
            Assert.fail("Mode not working");
        }
        Assert.assertEquals("concat('a', 'b', 'c')", e.toSql());
    }
}
