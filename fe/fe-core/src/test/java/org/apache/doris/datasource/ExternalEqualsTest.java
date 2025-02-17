package org.apache.doris.datasource;

import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.datasource.test.TestExternalDatabase;
import org.apache.doris.datasource.test.TestExternalTable;

import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class ExternalEqualsTest {
    @Mocked
    private TestExternalCatalog ctl1;
    @Mocked
    private TestExternalCatalog ctl2;

    @Test
    public void testEquals() {
        TestExternalDatabase db1 = new TestExternalDatabase(ctl1, 1L, "db1", null);
        TestExternalDatabase db2 = new TestExternalDatabase(ctl2, 1L, "db2", null);
        TestExternalDatabase db3 = new TestExternalDatabase(ctl1, 1L, "db2", null);
        TestExternalDatabase db11 = new TestExternalDatabase(ctl1, 1L, "db1", null);
        Assert.assertNotEquals(db1, db2);
        Assert.assertNotEquals(db1, db3);
        Assert.assertEquals(db1, db11);

        TestExternalTable t1 = new TestExternalTable(1L, "t1", null, ctl1, db1);
        TestExternalTable t2 = new TestExternalTable(2L, "t2", null, ctl2, db2);
        TestExternalTable t3 = new TestExternalTable(3L, "t3", null, ctl1, db1);
        TestExternalTable t11 = new TestExternalTable(4L, "t1", null, ctl1, db1);
        Assert.assertNotEquals(t1, t2);
        Assert.assertNotEquals(t1, t3);
        Assert.assertEquals(t1, t11);
    }
}
