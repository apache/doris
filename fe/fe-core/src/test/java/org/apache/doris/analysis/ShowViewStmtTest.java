package org.apache.doris.analysis;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.mysql.privilege.MockedAuth;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.qe.ConnectContext;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.StringReader;
import java.util.List;
import java.util.Set;
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

    @Test
    public void testGetTableRefs() throws Exception {
        String sql = "with w as (select a from db1.test1) " +
                "select b, c from db1.test2 " +
                "left outer join " +
                "(select d from db1.test3 join w on db1.test3.e = w.a) test4 " +
                "on test1.f = test4.d";
        SqlScanner input = new SqlScanner(new StringReader(sql));
        SqlParser parser = new SqlParser(input);
        QueryStmt queryStmt = (QueryStmt) SqlParserUtils.getFirstStmt(parser);
        List<TableRef> tblRefs = Lists.newArrayList();
        Set<String> parentViewNameSet = Sets.newHashSet();
        queryStmt.getTableRefs(tblRefs, parentViewNameSet);

        Assert.assertEquals(3, tblRefs.size());
        Assert.assertEquals("test1", tblRefs.get(0).getName().getTbl());
        Assert.assertEquals("test2", tblRefs.get(1).getName().getTbl());
        Assert.assertEquals("test3", tblRefs.get(2).getName().getTbl());
    }
}
