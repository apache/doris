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

package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.datasource.InternalCatalog;

import org.junit.Assert;
import org.junit.Test;

public class PrivEntryTest {
    @Test
    public void testNameWithUnderscores() throws Exception {
        TablePrivEntry tablePrivEntry = TablePrivEntry.create("user1", "127.%", InternalCatalog.INTERNAL_CATALOG_NAME,
                "db_db1", "tbl_tbl1", false, PrivBitSet.of(PaloPrivilege.SELECT_PRIV, PaloPrivilege.DROP_PRIV));
        // pattern match
        Assert.assertFalse(tablePrivEntry.getDbPattern().match("db-db1"));
        Assert.assertFalse(tablePrivEntry.getTblPattern().match("tbl-tbl1"));
        // create TablePrivTable
        TablePrivTable tablePrivTable = new TablePrivTable();
        tablePrivTable.addEntry(tablePrivEntry, false, false);
        UserIdentity userIdentity = new UserIdentity("user1", "127.%", false);
        userIdentity.setIsAnalyzed();

        PrivBitSet privs1 = PrivBitSet.of();
        tablePrivTable.getPrivs(userIdentity, "##internal", "db#db1", "tbl#tbl1", privs1);
        Assert.assertFalse(PaloPrivilege.satisfy(privs1, PrivPredicate.DROP));

        PrivBitSet privs2 = PrivBitSet.of();
        tablePrivTable.getPrivs(userIdentity, InternalCatalog.INTERNAL_CATALOG_NAME, "db_db1", "tbl_tbl1", privs2);
        Assert.assertTrue(PaloPrivilege.satisfy(privs2, PrivPredicate.DROP));
    }

    @Test
    public void testPrivBitSet() {
        PrivBitSet privBitSet = PrivBitSet.of(PaloPrivilege.ADMIN_PRIV, PaloPrivilege.NODE_PRIV);
        Assert.assertTrue(privBitSet.containsPrivs(PaloPrivilege.ADMIN_PRIV));
        Assert.assertTrue(privBitSet.containsPrivs(PaloPrivilege.NODE_PRIV));
        privBitSet.set(PaloPrivilege.DROP_PRIV.getIdx());
        Assert.assertTrue(privBitSet.containsPrivs(PaloPrivilege.DROP_PRIV));
        privBitSet.set(PaloPrivilege.DROP_PRIV.getIdx());
        Assert.assertTrue(privBitSet.containsPrivs(PaloPrivilege.DROP_PRIV));
        privBitSet.unset(PaloPrivilege.NODE_PRIV.getIdx());
        Assert.assertFalse(privBitSet.containsPrivs(PaloPrivilege.NODE_PRIV));
        privBitSet.unset(PaloPrivilege.NODE_PRIV.getIdx());
        Assert.assertFalse(privBitSet.containsPrivs(PaloPrivilege.NODE_PRIV));
    }
}
