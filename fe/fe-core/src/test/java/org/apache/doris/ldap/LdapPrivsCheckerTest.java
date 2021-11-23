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

package org.apache.doris.ldap;

import mockit.Expectations;
import mockit.Mocked;
import org.apache.doris.analysis.ResourcePattern;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.LdapConfig;
import org.apache.doris.mysql.privilege.PaloPrivilege;
import org.apache.doris.mysql.privilege.PaloRole;
import org.apache.doris.mysql.privilege.PrivBitSet;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class LdapPrivsCheckerTest {
    private static final String CLUSTER = "default_cluster";
    private static final String DB = "palodb";
    private static final String TABLE_DB = "tabledb";
    private static final String TABLE1 = "table1";
    private static final String TABLE2 = "table2";
    private static final String RESOURCE1 = "spark_resource";
    private static final String RESOURCE2 = "resource";
    private static final String USER = "default_cluster:zhangsan";
    private static final String IP = "192.168.0.1";
    private UserIdentity userIdent = UserIdentity.createAnalyzedUserIdentWithIp(USER, IP);

    @Mocked
    private ConnectContext context;

    @Before
    public void setUp() {
        LdapConfig.ldap_authentication_enabled = true;
        new Expectations() {
            {
                ConnectContext.get();
                minTimes = 0;
                result = context;

                PaloRole role = new PaloRole("");
                Map<TablePattern, PrivBitSet> tblPatternToPrivs = role.getTblPatternToPrivs();

                TablePattern global = new TablePattern("*", "*");
                tblPatternToPrivs.put(global, PrivBitSet.of(PaloPrivilege.SELECT_PRIV, PaloPrivilege.CREATE_PRIV));
                TablePattern db = new TablePattern(DB, "*");
                tblPatternToPrivs.put(db, PrivBitSet.of(PaloPrivilege.SELECT_PRIV, PaloPrivilege.LOAD_PRIV));
                TablePattern tbl1 = new TablePattern(TABLE_DB, TABLE1);
                tblPatternToPrivs.put(tbl1, PrivBitSet.of(PaloPrivilege.SELECT_PRIV, PaloPrivilege.ALTER_PRIV));
                TablePattern tbl2 = new TablePattern(TABLE_DB, TABLE2);
                tblPatternToPrivs.put(tbl2, PrivBitSet.of(PaloPrivilege.SELECT_PRIV, PaloPrivilege.DROP_PRIV));

                Map<ResourcePattern, PrivBitSet> resourcePatternToPrivs = role.getResourcePatternToPrivs();
                ResourcePattern globalResource = new ResourcePattern("*");
                resourcePatternToPrivs.put(globalResource, PrivBitSet.of(PaloPrivilege.USAGE_PRIV));
                ResourcePattern resource1 = new ResourcePattern(RESOURCE1);
                resourcePatternToPrivs.put(resource1, PrivBitSet.of(PaloPrivilege.USAGE_PRIV));
                ResourcePattern resource2 = new ResourcePattern(RESOURCE1);
                resourcePatternToPrivs.put(resource2, PrivBitSet.of(PaloPrivilege.USAGE_PRIV));
                try {
                    global.analyze(CLUSTER);
                    db.analyze(CLUSTER);
                    tbl1.analyze(CLUSTER);
                    tbl2.analyze(CLUSTER);
                    resource1.analyze();
                    resource2.analyze();
                } catch (AnalysisException e) {
                    e.printStackTrace();
                }
                context.getLdapGroupsPrivs();
                minTimes = 0;
                result = role;

                context.getCurrentUserIdentity();
                minTimes = 0;
                result = UserIdentity.createAnalyzedUserIdentWithIp(USER, IP);
            }
        };
    }

    @Test
    public void testHasGlobalPrivFromLdap() {
        Assert.assertTrue(LdapPrivsChecker.hasGlobalPrivFromLdap(userIdent, PrivPredicate.CREATE));
        Assert.assertTrue(LdapPrivsChecker.hasGlobalPrivFromLdap(userIdent, PrivPredicate.USAGE));
        Assert.assertFalse(LdapPrivsChecker.hasGlobalPrivFromLdap(userIdent, PrivPredicate.DROP));
    }

    @Test
    public void testHasDbPrivFromLdap() {
        Assert.assertTrue(LdapPrivsChecker.hasDbPrivFromLdap(userIdent, CLUSTER + ":" + DB, PrivPredicate.LOAD));
        Assert.assertFalse(LdapPrivsChecker.hasDbPrivFromLdap(userIdent, CLUSTER + ":" + DB, PrivPredicate.DROP));
        Assert.assertTrue(LdapPrivsChecker.hasDbPrivFromLdap(userIdent, PrivPredicate.LOAD));
        Assert.assertFalse(LdapPrivsChecker.hasDbPrivFromLdap(userIdent, PrivPredicate.DROP));
    }

    @Test
    public void testHasTblPrivFromLdap() {
        Assert.assertTrue(LdapPrivsChecker.hasTblPrivFromLdap(userIdent, CLUSTER + ":" + TABLE_DB, TABLE1,
                PrivPredicate.ALTER));
        Assert.assertFalse(LdapPrivsChecker.hasTblPrivFromLdap(userIdent, CLUSTER + ":" + TABLE_DB, TABLE1,
                PrivPredicate.DROP));
        Assert.assertTrue(LdapPrivsChecker.hasTblPrivFromLdap(userIdent, CLUSTER + ":" + TABLE_DB, TABLE2,
                PrivPredicate.DROP));
        Assert.assertFalse(LdapPrivsChecker.hasTblPrivFromLdap(userIdent, CLUSTER + ":" + TABLE_DB, TABLE2,
                PrivPredicate.CREATE));
        Assert.assertTrue(LdapPrivsChecker.hasTblPrivFromLdap(userIdent, PrivPredicate.ALTER));
        Assert.assertFalse(LdapPrivsChecker.hasTblPrivFromLdap(userIdent, PrivPredicate.LOAD));
    }

    @Test
    public void testHasResourcePrivFromLdap() {
        Assert.assertTrue(LdapPrivsChecker.hasResourcePrivFromLdap(userIdent, RESOURCE1, PrivPredicate.USAGE));
        Assert.assertFalse(LdapPrivsChecker.hasResourcePrivFromLdap(userIdent, "resource",
                PrivPredicate.USAGE));
    }

    @Test
    public void testGetGlobalPrivFromLdap() {
        Assert.assertEquals(PrivBitSet.of(PaloPrivilege.SELECT_PRIV, PaloPrivilege.CREATE_PRIV, PaloPrivilege.USAGE_PRIV).toString(),
                LdapPrivsChecker.getGlobalPrivFromLdap(userIdent).toString());
    }

    @Test
    public void testGetDbPrivFromLdap() {
        Assert.assertEquals(PrivBitSet.of(PaloPrivilege.SELECT_PRIV, PaloPrivilege.LOAD_PRIV).toString(),
                LdapPrivsChecker.getDbPrivFromLdap(userIdent, CLUSTER + ":" + DB).toString());
    }

    @Test
    public void testGetTblPrivFromLdap() {
        Assert.assertEquals(PrivBitSet.of(PaloPrivilege.SELECT_PRIV, PaloPrivilege.ALTER_PRIV).toString(),
                LdapPrivsChecker.getTblPrivFromLdap(userIdent, CLUSTER + ":" + TABLE_DB, TABLE1).toString());
    }

    @Test
    public void testGetResourcePrivFromLdap() {
        Assert.assertEquals(PrivBitSet.of(PaloPrivilege.USAGE_PRIV).toString(),
                LdapPrivsChecker.getResourcePrivFromLdap(userIdent, RESOURCE1).toString());
    }

    @Test
    public void testHasPrivsOfDb() {
        Assert.assertTrue(LdapPrivsChecker.hasPrivsOfDb(userIdent, CLUSTER + ":" + TABLE_DB));
    }

    @Test
    public void testIsCurrentUser() {
        Assert.assertTrue(LdapPrivsChecker.isCurrentUser(userIdent));
        Assert.assertFalse(LdapPrivsChecker.isCurrentUser(UserIdentity.
                createAnalyzedUserIdentWithIp("default_cluster:lisi", IP)));
        Assert.assertFalse(LdapPrivsChecker.isCurrentUser(UserIdentity.
                createAnalyzedUserIdentWithIp(USER, "127.0.0.1")));
    }

    @Test
    public void testGetLdapAllDbPrivs() throws AnalysisException {
        Map<TablePattern, PrivBitSet> allDb = LdapPrivsChecker.getLdapAllDbPrivs(userIdent);
        TablePattern db = new TablePattern(DB, "*");
        db.analyze(CLUSTER);
        Assert.assertEquals(PrivBitSet.of(PaloPrivilege.SELECT_PRIV, PaloPrivilege.LOAD_PRIV).toString(),
                allDb.get(db).toString());
    }

    @Test
    public void testGetLdapAllTblPrivs() throws AnalysisException {
        Map<TablePattern, PrivBitSet> allTbl = LdapPrivsChecker.getLdapAllTblPrivs(userIdent);
        TablePattern tbl1 = new TablePattern(TABLE_DB, TABLE1);
        TablePattern tbl2 = new TablePattern(TABLE_DB, TABLE2);
        tbl1.analyze(CLUSTER);
        tbl2.analyze(CLUSTER);
        Assert.assertEquals(PrivBitSet.of(PaloPrivilege.SELECT_PRIV, PaloPrivilege.ALTER_PRIV).toString(),
                allTbl.get(tbl1).toString());
        Assert.assertEquals(PrivBitSet.of(PaloPrivilege.SELECT_PRIV, PaloPrivilege.DROP_PRIV).toString(),
                allTbl.get(tbl2).toString());
    }

    @Test
    public void testGetLdapAllResourcePrivs() {
        Map<ResourcePattern, PrivBitSet> allResource = LdapPrivsChecker.getLdapAllResourcePrivs(userIdent);
        ResourcePattern resource1 = new ResourcePattern(RESOURCE1);
        ResourcePattern resource2 = new ResourcePattern(RESOURCE1);
        Assert.assertEquals(PrivBitSet.of(PaloPrivilege.USAGE_PRIV).toString(), allResource.get(resource1).toString());
        Assert.assertEquals(PrivBitSet.of(PaloPrivilege.USAGE_PRIV).toString(), allResource.get(resource2).toString());
    }
}
