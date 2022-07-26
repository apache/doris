package org.apache.doris.analysis;

import org.apache.doris.common.util.SqlParserUtils;

import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;

public class SqlParserTest {

    @Test
    public void testIndex() {
        String[] alterStmts = {
                "alter table example_db.table1 add index idx_user(username) comment 'username index';",
                "alter table example_db.table1 add index idx_user_bitmap(username) using bitmap comment 'username bitmap index';",
                "alter table example_db.table1 add index idx_user_ngrambf(username) using NGRAM_BF(3, 256) comment 'username ngrambf_v1 index';",
                "create index idx_ngrambf on example_db.table1(username) using NGRAM_BF(3, 256) comment 'username ngram_bf index';"
        };
        for (String alterStmt : alterStmts) {
            SqlParser parser = new SqlParser(new SqlScanner(new StringReader(alterStmt)));
            StatementBase alterQueryStmt = null;
            try {
                alterQueryStmt = SqlParserUtils.getFirstStmt(parser);
            } catch (Error e) {
                Assert.fail(e.getMessage());
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
            Assert.assertTrue(alterQueryStmt instanceof AlterTableStmt);
        }
    }
}
