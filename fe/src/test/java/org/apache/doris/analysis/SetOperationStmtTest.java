package org.apache.doris.analysis;

import java.io.StringReader;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.mysql.privilege.MockedAuth;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.qe.ConnectContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import mockit.Mocked;

public class SetOperationStmtTest {
    private Analyzer analyzer;

    @Mocked
    private PaloAuth auth;
    @Mocked
    private ConnectContext ctx;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        MockedAuth.mockedAuth(auth);
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
    }
    @Test
    public void testNormal() throws Exception {
        String sql = "select k1,k2 from t where k1='a' union select k1,k2 from t where k1='b';";
        SqlScanner input = new SqlScanner(new StringReader(sql));
        SqlParser parser = new SqlParser(input);
        SetOperationStmt stmt = (SetOperationStmt) parser.parse().value;
        Assert.assertEquals(SetOperationStmt.Operation.UNION, stmt.getOperands().get(1).getOperation());
        sql = "select k1,k2 from t where k1='a' intersect select k1,k2 from t where k1='b';";
        input = new SqlScanner(new StringReader(sql));
        parser = new SqlParser(input);
        stmt = (SetOperationStmt) parser.parse().value;
        Assert.assertEquals(SetOperationStmt.Operation.INTERSECT, stmt.getOperands().get(1).getOperation());
        sql = "select k1,k2 from t where k1='a' except select k1,k2 from t where k1='b';";
        input = new SqlScanner(new StringReader(sql));
        parser = new SqlParser(input);
        stmt = (SetOperationStmt) parser.parse().value;
        Assert.assertEquals(SetOperationStmt.Operation.EXCEPT, stmt.getOperands().get(1).getOperation());
        sql = "select k1,k2 from t where k1='a' minus select k1,k2 from t where k1='b';";
        input = new SqlScanner(new StringReader(sql));
        parser = new SqlParser(input);
        stmt = (SetOperationStmt) parser.parse().value;
        Assert.assertEquals(SetOperationStmt.Operation.EXCEPT, stmt.getOperands().get(1).getOperation());
        sql = "select k1,k2 from t where k1='a' union select k1,k2 from t where k1='b' intersect select k1,k2 from t "
                + "where k1='c' except select k1,k2 from t where k1='d';";
        input = new SqlScanner(new StringReader(sql));
        parser = new SqlParser(input);
        stmt = (SetOperationStmt) parser.parse().value;
        Assert.assertEquals(SetOperationStmt.Operation.UNION, stmt.getOperands().get(1).getOperation());
        Assert.assertEquals(SetOperationStmt.Operation.INTERSECT, stmt.getOperands().get(2).getOperation());
        Assert.assertEquals(SetOperationStmt.Operation.EXCEPT, stmt.getOperands().get(3).getOperation());
        Assert.assertEquals(4, stmt.getOperands().size());


    }

}
