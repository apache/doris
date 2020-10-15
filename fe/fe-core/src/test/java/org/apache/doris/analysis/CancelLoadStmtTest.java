package org.apache.doris.analysis;

import mockit.Expectations;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.FakeCatalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CancelLoadStmtTest {
    private Analyzer analyzer;
    private Catalog catalog;

    FakeCatalog fakeCatalog;

    @Before
    public void setUp() {
        fakeCatalog = new FakeCatalog();

        catalog = AccessTestUtil.fetchAdminCatalog();
        FakeCatalog.setCatalog(catalog);

        analyzer = AccessTestUtil.fetchAdminAnalyzer(true);
        new Expectations(analyzer) {
            {
                analyzer.getDefaultDb();
                minTimes = 0;
                result = "testCluster:testDb";

                analyzer.getQualifiedUser();
                minTimes = 0;
                result = "testCluster:testUser";

                analyzer.getClusterName();
                minTimes = 0;
                result = "testCluster";

                analyzer.getCatalog();
                minTimes = 0;
                result = catalog;
            }
        };
    }

    @Test
    public void testNormal() throws UserException, AnalysisException {
        SlotRef slotRef = new SlotRef(null, "label");
        StringLiteral stringLiteral = new StringLiteral("doris_test_label");

        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef, stringLiteral);
        CancelLoadStmt stmt = new CancelLoadStmt(null, binaryPredicate);
        stmt.analyze(analyzer);
        Assert.assertTrue(stmt.isAccurateMatch());
        Assert.assertEquals("CANCEL LOAD FROM testCluster:testDb WHERE `label` = 'doris_test_label'", stmt.toString());

        LikePredicate likePredicate = new LikePredicate(LikePredicate.Operator.LIKE, slotRef, stringLiteral);
        stmt = new CancelLoadStmt(null, likePredicate);
        stmt.analyze(analyzer);
        Assert.assertFalse(stmt.isAccurateMatch());
        Assert.assertEquals("CANCEL LOAD FROM testCluster:testDb WHERE `label` LIKE 'doris_test_label'", stmt.toString());
    }

    @Test(expected = AnalysisException.class)
    public void testNoDb() throws UserException, AnalysisException {
        SlotRef slotRef = new SlotRef(null, "label");
        StringLiteral stringLiteral = new StringLiteral("doris_test_label");
        new Expectations(analyzer) {
            {
                analyzer.getDefaultDb();
                minTimes = 0;
                result = "";

                analyzer.getClusterName();
                minTimes = 0;
                result = "testCluster";
            }
        };

        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef, stringLiteral);
        CancelLoadStmt stmt = new CancelLoadStmt(null, binaryPredicate);
        stmt.analyze(analyzer);
        Assert.fail("No exception throws.");
    }
}
