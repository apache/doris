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

package org.apache.doris.datasource;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.metacache.MetaCache;
import org.apache.doris.datasource.test.TestExternalCatalog;
import org.apache.doris.datasource.test.TestExternalDatabase;
import org.apache.doris.datasource.test.TestExternalTable;
import org.apache.doris.mtmv.BaseTableInfo;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class ExternalEqualsTest {
    @Mocked
    private TestExternalCatalog ctl1;
    @Mocked
    private TestExternalCatalog ctl2;
    @Mocked
    private MetaCache<TestExternalTable> metaCache;

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

    @Test
    public void testRegisterTableRestoresExternalTableOwnerReferences() {
        new Expectations() {
            {
                ctl1.fromRemoteTableName("db1", "tbl_event");
                result = "tbl_event";
                ctl1.getName();
                result = "test_catalog";
            }
        };
        TestExternalDatabase db = new TestExternalDatabase(ctl1, 2L, "db1", "db1");
        Deencapsulation.setField(db, "initialized", true);
        Deencapsulation.setField(db, "metaCache", metaCache);
        TestExternalTable table = new TestExternalTable(3L, "tbl_event", "tbl_event", ctl1, db);
        table.setCatalog(null);
        table.setDb(null);

        db.registerTable(table);

        Assert.assertSame(ctl1, table.getCatalog());
        Assert.assertSame(db, table.getDatabase());
        new BaseTableInfo(table);
    }
}
