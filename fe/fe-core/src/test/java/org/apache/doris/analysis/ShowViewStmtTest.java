package org.apache.doris.analysis;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.MockedAuth;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.qe.ConnectContext;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import mockit.Expectations;
import mockit.Mocked;

public class ShowViewStmtTest {
    private Analyzer analyzer;
    private Catalog catalog;

    @Mocked
    private PaloAuth auth;
    @Mocked
    private ConnectContext ctx;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        catalog = AccessTestUtil.fetchAdminCatalog();
        MockedAuth.mockedAuth(auth);
        MockedAuth.mockedConnectContext(ctx, "root", "192.168.1.1");
    }

    @Test
    public void testNormal() throws UserException {
        new Expectations(catalog) {
            {
                Catalog.getCurrentCatalog();
                result = catalog;
            }
        };

        ShowViewStmt stmt = new ShowViewStmt("", new TableName("testDb", "testTbl"));
        stmt.analyze(analyzer);
        Assert.assertEquals("SHOW VIEW FROM `testCluster:testDb`.`testTbl`", stmt.toString());
        Assert.assertEquals("testCluster:testDb", stmt.getDb());
        Assert.assertEquals("testTbl", stmt.getTbl());
        Assert.assertEquals(2, stmt.getMetaData().getColumnCount());
        Assert.assertEquals("View", stmt.getMetaData().getColumn(0).getName());
        Assert.assertEquals("Create View", stmt.getMetaData().getColumn(1).getName());
    }

    @Test(expected = UserException.class)
    public void testNoDb() throws UserException {
        ShowViewStmt stmt = new ShowViewStmt("", new TableName("", "testTbl"));
        stmt.analyze(analyzer);
        Assert.fail();
    }
}
