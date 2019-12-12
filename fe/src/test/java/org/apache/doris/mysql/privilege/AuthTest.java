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

import mockit.Expectations;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.CreateRoleStmt;
import org.apache.doris.analysis.CreateUserStmt;
import org.apache.doris.analysis.DropRoleStmt;
import org.apache.doris.analysis.DropUserStmt;
import org.apache.doris.analysis.GrantStmt;
import org.apache.doris.analysis.RevokeStmt;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AccessPrivilege;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.DomainResolver;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.PrivInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Set;

import mockit.Delegate;
import mockit.Mocked;

public class AuthTest {

    private PaloAuth auth;
    @Mocked
    public Catalog catalog;
    @Mocked
    private Analyzer analyzer;
    @Mocked
    private EditLog editLog;
    @Mocked
    private ConnectContext ctx;

    private MockDomianResolver resolver;

    // Thread is not mockable in Jmockit, so use a subclass instead.
    private static final class MockDomianResolver extends DomainResolver {
        public MockDomianResolver(PaloAuth auth) {
            super(auth);
        }
        @Override
        public boolean resolveWithBNS(String domainName, Set<String> resolvedIPs) {
            switch (domainName) {
                case "palo.domain1":
                    resolvedIPs.add("10.1.1.1");
                    resolvedIPs.add("10.1.1.2");
                    resolvedIPs.add("10.1.1.3");
                    break;
                case "palo.domain2":
                    resolvedIPs.add("20.1.1.1");
                    resolvedIPs.add("20.1.1.2");
                    resolvedIPs.add("20.1.1.3");
                    break;
            }
            return true;
        }
    }

    @Before
    public void setUp() throws NoSuchMethodException, SecurityException {
        auth = new PaloAuth();

        new Expectations() {
            {
                analyzer.getClusterName();
                minTimes = 0;
                result = SystemInfoService.DEFAULT_CLUSTER;

                Catalog.getCurrentCatalog();
                minTimes = 0;
                result = catalog;

                catalog.getAuth();
                minTimes = 0;
                result = auth;

                catalog.getEditLog();
                minTimes = 0;
                result = editLog;

                editLog.logCreateUser((PrivInfo) any);
                minTimes = 0;

                ConnectContext.get();
                minTimes = 0;
                result = ctx;

                ctx.getQualifiedUser();
                minTimes = 0;
                result = "root";

                ctx.getRemoteIP();
                minTimes = 0;
                result = "192.168.1.1";

                ctx.getState();
                minTimes = 0;
                result = new QueryState();

                ctx.getCurrentUserIdentity();
                minTimes = 0;
                result = UserIdentity.createAnalyzedUserIdentWithIp("root", "%");
            }
        };

        resolver = new MockDomianResolver(auth);
    }

    @Test
    public void test() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        // 1. create cmy@%
        UserIdentity userIdentity = new UserIdentity("cmy", "%");
        UserDesc userDesc = new UserDesc(userIdentity, "12345", true);
        CreateUserStmt createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.createUser(createUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }

        // 2. check if cmy from specified ip can access to palo
        List<UserIdentity> currentUser = Lists.newArrayList();
        Assert.assertTrue(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":cmy", "192.168.0.1", "12345",
                currentUser));
        Assert.assertFalse(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":cmy", "192.168.0.1",
                "123456", null));
        Assert.assertFalse(auth.checkPlainPassword("other:cmy", "192.168.0.1", "12345", null));
        Assert.assertTrue(currentUser.get(0).equals(userIdentity));

        // 3. create another user: zhangsan@"192.%"
        userIdentity = new UserIdentity("zhangsan", "192.%");
        userDesc = new UserDesc(userIdentity, "12345", true);
        createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.createUser(createUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }

        // 4. check if zhangsan from specified ip can access to palo
        Assert.assertTrue(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "192.168.0.1",
                "12345", null));
        Assert.assertFalse(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "172.168.0.1",
                "12345", null));
        Assert.assertFalse(auth.checkPlainPassword("zhangsan", "192.168.0.1", "12345", null));

        // 4.1 check if we can create same user
        userIdentity = new UserIdentity("zhangsan", "192.%");
        userDesc = new UserDesc(userIdentity, "12345", true);
        createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        boolean hasException = false;
        try {
            auth.createUser(createUserStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 4.2 check if we can create same user name with different host
        userIdentity = new UserIdentity("zhangsan", "172.18.1.1");
        userDesc = new UserDesc(userIdentity, "12345", true);
        createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.createUser(createUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }
        Assert.assertTrue(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "172.18.1.1",
                "12345", null));

        // 5. create a user with domain [palo.domain]
        userIdentity = new UserIdentity("zhangsan", "palo.domain1", true);
        userDesc = new UserDesc(userIdentity, "12345", true);
        createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        try {
            auth.createUser(createUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }
        
        // 5.1 resolve domain [palo.domain1]
        resolver.runAfterCatalogReady();
        
        // 6. check if user from resolved ip can access to palo
        Assert.assertTrue(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.1",
                "12345", null));
        Assert.assertFalse(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.1",
                "123456", null));
        Assert.assertFalse(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "11.1.1.1",
                "12345", null));

        // 7. add duplicated user@['palo.domain1']
        userIdentity = new UserIdentity("zhangsan", "palo.domain1", true);
        userDesc = new UserDesc(userIdentity, "12345", true);
        createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        hasException = false;
        try {
            auth.createUser(createUserStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 8. add another user@['palo.domain2']
        userIdentity = new UserIdentity("lisi", "palo.domain2", true);
        userDesc = new UserDesc(userIdentity, "123456", true);
        createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.createUser(createUserStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // 8.1 resolve domain [palo.domain2]
        resolver.runAfterCatalogReady();

        Assert.assertTrue(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "20.1.1.1",
                "123456", null));
        Assert.assertFalse(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "10.1.1.1",
                "123456", null));
        Assert.assertFalse(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "20.1.1.2",
                "123455", null));

        /*
         * Now we have 4 users:
         * cmy@'%'
         * zhangsan@"192.%"
         * zhangsan@['palo.domain1']
         * lisi@['palo.domain2']
         */

        // 9. grant for cmy@'%'
        TablePattern tablePattern = new TablePattern("*", "*");
        List<AccessPrivilege> privileges = Lists.newArrayList(AccessPrivilege.CREATE_PRIV, AccessPrivilege.DROP_PRIV);
        GrantStmt grantStmt = new GrantStmt(new UserIdentity("cmy", "%"), null, tablePattern, privileges);

        try {
            grantStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        List<UserIdentity> currentUser2 = Lists.newArrayList();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":cmy", "172.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        // check auth before grant
        Assert.assertFalse(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db1",
                                            PrivPredicate.CREATE));

        try {
            auth.grant(grantStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // 9.1 check auth
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db1",
                                           PrivPredicate.CREATE));
        UserIdentity zhangsan1 = UserIdentity.createAnalyzedUserIdentWithIp(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan",
                "172.1.1.1");
        Assert.assertFalse(auth.checkDbPriv(zhangsan1, SystemInfoService.DEFAULT_CLUSTER + ":db1",
                                            PrivPredicate.CREATE));

        // 10. grant auth for non exist user
        tablePattern = new TablePattern("*", "*");
        privileges = Lists.newArrayList(AccessPrivilege.CREATE_PRIV, AccessPrivilege.DROP_PRIV);
        grantStmt = new GrantStmt(new UserIdentity("nouser", "%"), null, tablePattern, privileges);

        try {
            grantStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        hasException = false;
        try {
            auth.grant(grantStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 11. grant auth for user with non exist host
        tablePattern = new TablePattern("*", "*");
        privileges = Lists.newArrayList(AccessPrivilege.SELECT_PRIV, AccessPrivilege.DROP_PRIV);
        grantStmt = new GrantStmt(new UserIdentity("zhangsan", "%"), null, tablePattern, privileges);

        try {
            grantStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        hasException = false;
        try {
            auth.grant(grantStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);
        
        // 12. grant db auth to exist user
        tablePattern = new TablePattern("db1", "*");
        privileges = Lists.newArrayList(AccessPrivilege.SELECT_PRIV, AccessPrivilege.DROP_PRIV);
        grantStmt = new GrantStmt(new UserIdentity("zhangsan", "192.%"), null, tablePattern, privileges);

        try {
            grantStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.grant(grantStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "192.168.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());

        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db1",
                                           PrivPredicate.SELECT));
        Assert.assertFalse(auth.checkGlobalPriv(currentUser2.get(0), PrivPredicate.SELECT));
        Assert.assertTrue(auth.checkTblPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db1",
                "tbl1", PrivPredicate.SELECT));

        // 13. grant tbl auth to exist user
        tablePattern = new TablePattern("db2", "tbl2");
        privileges = Lists.newArrayList(AccessPrivilege.ALTER_PRIV, AccessPrivilege.DROP_PRIV);
        grantStmt = new GrantStmt(new UserIdentity("zhangsan", "192.%"), null, tablePattern, privileges);

        try {
            grantStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.grant(grantStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }
        
        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "192.168.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertFalse(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db2",
                                           PrivPredicate.SELECT));
        Assert.assertFalse(auth.checkGlobalPriv(currentUser2.get(0), PrivPredicate.SELECT));
        Assert.assertTrue(auth.checkTblPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db2", "tbl2",
                                            PrivPredicate.DROP));

        // 14. grant db auth to zhangsan@['palo.domain1']
        tablePattern = new TablePattern("db3", "*");
        privileges = Lists.newArrayList(AccessPrivilege.ALTER_PRIV, AccessPrivilege.DROP_PRIV);
        grantStmt = new GrantStmt(new UserIdentity("zhangsan", "palo.domain1", true), null, tablePattern, privileges);

        try {
            grantStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.grant(grantStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db3",
                                            PrivPredicate.ALTER));
        // 15. grant new auth to exist priv entry (exist ALTER/DROP, add SELECT)
        tablePattern = new TablePattern("db3", "*");
        privileges = Lists.newArrayList(AccessPrivilege.SELECT_PRIV);
        grantStmt = new GrantStmt(new UserIdentity("zhangsan", "palo.domain1", true), null, tablePattern, privileges);

        try {
            grantStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.grant(grantStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db3",
                                            PrivPredicate.SELECT));

        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.2", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db3",
                                           PrivPredicate.ALTER));
        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.3", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db3",
                                           PrivPredicate.DROP));

        /*
         * for now, we have following auth:
         * cmy@'%'
         *      *.* -> CREATE/DROP
         * zhangsan@"192.%"
         *      db1.* -> SELECT/DROP
         *      db2.tbl2 -> ALTER/DROP
         * zhangsan@['palo.domain1']
         *      db3.* -> ALTER/DROP/SELECT
         * lisi@['palo.domain2']
         *      N/A
         */

        // 16. revoke privs from non exist user
        tablePattern = new TablePattern("*", "*");
        privileges = Lists.newArrayList(AccessPrivilege.SELECT_PRIV);
        RevokeStmt revokeStmt = new RevokeStmt(new UserIdentity("nouser", "%"), null, tablePattern, privileges);

        try {
            revokeStmt.analyze(analyzer);
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }

        hasException = false;
        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 17. revoke privs from non exist host
        tablePattern = new TablePattern("*", "*");
        privileges = Lists.newArrayList(AccessPrivilege.SELECT_PRIV);
        revokeStmt = new RevokeStmt(new UserIdentity("cmy", "172.%"), null, tablePattern, privileges);

        try {
            revokeStmt.analyze(analyzer);
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }

        hasException = false;
        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 18. revoke privs from non exist db
        tablePattern = new TablePattern("nodb", "*");
        privileges = Lists.newArrayList(AccessPrivilege.SELECT_PRIV);
        revokeStmt = new RevokeStmt(new UserIdentity("cmy", "%"), null, tablePattern, privileges);

        try {
            revokeStmt.analyze(analyzer);
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }

        hasException = false;
        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 19. revoke privs from user @ ip
        tablePattern = new TablePattern("*", "*");
        privileges = Lists.newArrayList(AccessPrivilege.CREATE_PRIV);
        revokeStmt = new RevokeStmt(new UserIdentity("cmy", "%"), null, tablePattern, privileges);

        try {
            revokeStmt.analyze(analyzer);
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":cmy", "172.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db",
                                           PrivPredicate.CREATE));
        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertFalse(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db",
                                            PrivPredicate.CREATE));
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db",
                                            PrivPredicate.DROP));

        // 19. revoke tbl privs from user @ ip
        tablePattern = new TablePattern("db2", "tbl2");
        privileges = Lists.newArrayList(AccessPrivilege.ALTER_PRIV);
        revokeStmt = new RevokeStmt(new UserIdentity("zhangsan", "192.%"), null, tablePattern, privileges);

        try {
            revokeStmt.analyze(analyzer);
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "192.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkTblPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db2",
                "tbl2", PrivPredicate.ALTER));
        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "192.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertFalse(auth.checkTblPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db2",
                "tbl2", PrivPredicate.ALTER));
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db1",
                                           PrivPredicate.SELECT));

        // 20. revoke privs from non exist user @ domain
        tablePattern = new TablePattern("db2", "tbl2");
        privileges = Lists.newArrayList(AccessPrivilege.ALTER_PRIV);
        revokeStmt = new RevokeStmt(new UserIdentity("zhangsan", "nodomain", true), null, tablePattern, privileges);

        try {
            revokeStmt.analyze(analyzer);
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }

        hasException = false;
        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 21. revoke privs from non exist db from user @ domain
        tablePattern = new TablePattern("nodb", "*");
        privileges = Lists.newArrayList(AccessPrivilege.ALTER_PRIV);
        revokeStmt = new RevokeStmt(new UserIdentity("zhangsan", "palo.domain1", true), null, tablePattern, privileges);

        try {
            revokeStmt.analyze(analyzer);
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }

        hasException = false;
        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 22. revoke privs from exist user @ domain
        tablePattern = new TablePattern("db3", "*");
        privileges = Lists.newArrayList(AccessPrivilege.DROP_PRIV);
        revokeStmt = new RevokeStmt(new UserIdentity("zhangsan", "palo.domain1", true), null, tablePattern, privileges);

        try {
            revokeStmt.analyze(analyzer);
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db3",
                                           PrivPredicate.DROP));

        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertFalse(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db3",
                                            PrivPredicate.DROP));

        /*
         * for now, we have following auth:
         * cmy@'%'
         *      *.* -> DROP
         * zhangsan@"192.%"
         *      db1.* -> SELECT/DROP
         *      db2.tbl2 -> DROP
         * zhangsan@['palo.domain1']
         *      db3.* -> ALTER/SELECT
         * lisi@['palo.domain2']
         *      N/A
         */

        // 23. create admin role, which is not allowed
        CreateRoleStmt roleStmt = new CreateRoleStmt(PaloRole.ADMIN_ROLE);
        hasException = false;
        try {
            roleStmt.analyze(analyzer);
        } catch (UserException e1) {
            e1.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 23. create operator role, which is not allowed
        roleStmt = new CreateRoleStmt(PaloRole.OPERATOR_ROLE);
        hasException = false;
        try {
            roleStmt.analyze(analyzer);
        } catch (UserException e1) {
            e1.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 24. create role
        roleStmt = new CreateRoleStmt("role1");
        try {
            roleStmt.analyze(analyzer);
        } catch (UserException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        try {
            auth.createRole(roleStmt);
        } catch (DdlException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        // 25. grant auth to non exist role, will create this new role
        privileges = Lists.newArrayList(AccessPrivilege.DROP_PRIV, AccessPrivilege.SELECT_PRIV);
        grantStmt = new GrantStmt(null, "role2", new TablePattern("*", "*"), privileges);
        try {
            grantStmt.analyze(analyzer);
        } catch (UserException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        try {
            auth.grant(grantStmt);
        } catch (DdlException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        // 26. grant auth to role
        privileges = Lists.newArrayList(AccessPrivilege.DROP_PRIV, AccessPrivilege.SELECT_PRIV);
        grantStmt = new GrantStmt(null, "role1", new TablePattern("*", "*"), privileges);
        try {
            grantStmt.analyze(analyzer);
        } catch (UserException e1) {
            e1.printStackTrace();
            Assert.fail();
        }
        try {
            auth.grant(grantStmt);
        } catch (DdlException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        // 27. create user and set it as role1
        userIdentity = new UserIdentity("wangwu", "%");
        userDesc = new UserDesc(userIdentity, "12345", true);
        createUserStmt = new CreateUserStmt(false, userDesc, "role1");
        try {
            createUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.createUser(createUserStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":wangwu", "10.17.2.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db4",
                                           PrivPredicate.DROP));

        // 28. create user@domain and set it as role1
        userIdentity = new UserIdentity("chenliu", "palo.domain2", true);
        userDesc = new UserDesc(userIdentity, "12345", true);
        createUserStmt = new CreateUserStmt(false, userDesc, "role1");
        try {
            createUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.createUser(createUserStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":chenliu", "20.1.1.1", "12345", currentUser2);
        Assert.assertEquals(0, currentUser2.size());
        resolver.runAfterCatalogReady();
        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":chenliu", "20.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db4",
                                           PrivPredicate.DROP));

        // 29. revoke auth on non exist db from role1
        privileges = Lists.newArrayList(AccessPrivilege.DROP_PRIV);
        revokeStmt = new RevokeStmt(null, "role1", new TablePattern("nodb", "*"), privileges);
        try {
            revokeStmt.analyze(analyzer);
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }
        
        hasException = false;
        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e1) {
            e1.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 30. revoke auth from role1
        privileges = Lists.newArrayList(AccessPrivilege.DROP_PRIV);
        revokeStmt = new RevokeStmt(null, "role1", new TablePattern("*", "*"), privileges);
        try {
            revokeStmt.analyze(analyzer);
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":chenliu", "20.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertFalse(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db4",
                                           PrivPredicate.DROP));

        // 31. drop role, privs remain unchanged
        DropRoleStmt dropRoleStmt = new DropRoleStmt("role1");
        try {
            dropRoleStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.dropRole(dropRoleStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }
        currentUser2.clear();
        auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":chenliu", "20.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertFalse(auth.checkDbPriv(currentUser2.get(0), SystemInfoService.DEFAULT_CLUSTER + ":db4",
                                            PrivPredicate.DROP));

        // 32. drop user cmy@"%"
        DropUserStmt dropUserStmt = new DropUserStmt(new UserIdentity("cmy", "%"));
        try {
            dropUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            auth.dropUser(dropUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }

        Assert.assertFalse(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":cmy", "192.168.0.1", "12345", null));
        Assert.assertTrue(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "192.168.0.1",
                                                  "12345", null));

        // 33. drop user zhangsan@"192.%"
        dropUserStmt = new DropUserStmt(new UserIdentity("zhangsan", "192.%"));
        try {
            dropUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "192.168.0.1", "12345", null));
        
        try {
            auth.dropUser(dropUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }
        Assert.assertFalse(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "192.168.0.1", "12345", null));
        Assert.assertTrue(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.1", "12345", null));
        
        // 34. create user zhangsan@'10.1.1.1' to overwrite one of zhangsan@['palo.domain1']
        userIdentity = new UserIdentity("zhangsan", "10.1.1.1");
        userDesc = new UserDesc(userIdentity, "abcde", true);
        createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        
        Assert.assertTrue(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.1", "12345", null));

        try {
            auth.createUser(createUserStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertFalse(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.1", "12345", null));
        Assert.assertTrue(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.1", "abcde", null));
        
        // 35. drop user zhangsan@['palo.domain1']
        dropUserStmt = new DropUserStmt(new UserIdentity("zhangsan", "palo.domain1", true));
        try {
            dropUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.2", "12345", null));
        
        try {
            auth.dropUser(dropUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }
        Assert.assertTrue(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.2", "12345", null));
        
        resolver.runAfterCatalogReady();
        Assert.assertFalse(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.2", "12345", null));
        Assert.assertTrue(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.1", "abcde", null));

        // 36. drop user lisi@['palo.domain1']
        dropUserStmt = new DropUserStmt(new UserIdentity("lisi", "palo.domain2", true));
        try {
            dropUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "20.1.1.1", "123456", null));
        Assert.assertFalse(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "10.1.1.1", "123456", null));
        
        try {
            auth.dropUser(dropUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }
        Assert.assertTrue(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "20.1.1.1", "123456", null));
        Assert.assertFalse(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "10.1.1.1", "123456", null));
        
        resolver.runAfterCatalogReady();
        Assert.assertFalse(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "20.1.1.1", "123456", null));
        Assert.assertFalse(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":lisi", "10.1.1.1", "123456", null));
        
        // 37. drop zhangsan@'172.18.1.1' and zhangsan@'10.1.1.1'
        dropUserStmt = new DropUserStmt(new UserIdentity("zhangsan", "172.18.1.1", false));
        try {
            dropUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        
        try {
            auth.dropUser(dropUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }
        
        dropUserStmt = new DropUserStmt(new UserIdentity("zhangsan", "10.1.1.1", false));
        try {
            dropUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        
        try {
            auth.dropUser(dropUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }
        Assert.assertFalse(auth.checkPlainPassword(SystemInfoService.DEFAULT_CLUSTER + ":zhangsan", "10.1.1.1", "abcde", null));

    }

}
