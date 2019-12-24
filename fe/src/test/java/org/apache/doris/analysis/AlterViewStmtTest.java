package org.apache.doris.analysis;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.View;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.AlterViewInfo;
import org.apache.doris.persist.CreateTableInfo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;

public class AlterViewStmtTest {
    private Analyzer analyzer;

    private Catalog catalog;

    @Mocked
    EditLog editLog;

    @Mocked
    private ConnectContext connectContext;

    @Mocked
    private PaloAuth auth;

    @Before
    public void setUp() {
        catalog = Deencapsulation.newInstance(Catalog.class);
        analyzer = new Analyzer(catalog, connectContext);


        Database db = new Database(50000L, "testCluster:testDb");

        Column column1 = new Column("col1", PrimitiveType.BIGINT);
        Column column2 = new Column("col2", PrimitiveType.DOUBLE);

        List<Column> baseSchema = new LinkedList<Column>();
        baseSchema.add(column1);
        baseSchema.add(column2);

        OlapTable table = new OlapTable(30000, "testTbl",
                baseSchema, KeysType.AGG_KEYS, new SinglePartitionInfo(), null);
        db.createTable(table);


        new Expectations(auth) {
            {
                auth.checkGlobalPriv((ConnectContext) any, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                auth.checkDbPriv((ConnectContext) any, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;

                auth.checkTblPriv((ConnectContext) any, anyString, anyString, (PrivPredicate) any);
                minTimes = 0;
                result = true;
            }
        };

        new Expectations(editLog) {
            {
                editLog.logCreateTable((CreateTableInfo) any);
                minTimes = 0;

                editLog.logModifyViewDef((AlterViewInfo) any);
                minTimes = 0;
            }
        };

        Deencapsulation.setField(catalog, "editLog", editLog);

        new MockUp<Catalog>() {
            @Mock
            Catalog getInstance() {
                return catalog;
            }
            @Mock
            PaloAuth getAuth() {
                return auth;
            }
            @Mock
            Database getDb(long dbId) {
                return db;
            }
            @Mock
            Database getDb(String dbName) {
                return db;
            }
        };

        new MockUp<Analyzer>() {
            @Mock
            String getClusterName() {
                return "testCluster";
            }
        };
    }

    @Test
    public void testNormal() {
        String originStmt = "select col1 as c1, sum(col2) as c2 from testTbl group by col1";
        View view = new View(30000L, "testView", null);
        view.setInlineViewDef("select col1 as c1, sum(col2) as c2 from testTbl group by col1");
        try {
            view.init();
        } catch (UserException e) {
            Assert.fail();
        }

        Database db = analyzer.getCatalog().getDb("testDb");
        db.createTable(view);

        Assert.assertEquals(originStmt, view.getInlineViewDef());

        String alterStmt = "select col1 as k1, col2 as k2 from testDb.testTbl where col1 > 10";
        SqlParser parser = new SqlParser(new SqlScanner(new StringReader(alterStmt)));
        QueryStmt alterQueryStmt = null;
        try {
            alterQueryStmt = (QueryStmt) parser.parse().value;
        } catch (Error e) {
            Assert.fail(e.getMessage());
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        ColWithComment col1 = new ColWithComment("h1", null);
        ColWithComment col2 = new ColWithComment("h2", null);

        AlterViewStmt alterViewStmt = new AlterViewStmt(new TableName("testDb", "testView"), Lists.newArrayList(col1, col2), alterQueryStmt);
        try {
            alterViewStmt.analyze(analyzer);
            analyzer.getCatalog().alterView(alterViewStmt);
        } catch (UserException e) {
            Assert.fail();
        }

        View newView = (View) db.getTable("testView");

        Assert.assertEquals("SELECT `col1` AS `h1`, `col2` AS `h2` FROM `testCluster:testDb`.`testTbl` WHERE `col1` > 10",
                newView.getInlineViewDef());
    }
}
