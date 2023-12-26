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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.CreateRoleStmt;
import org.apache.doris.analysis.CreateUserStmt;
import org.apache.doris.analysis.DropRoleStmt;
import org.apache.doris.analysis.DropUserStmt;
import org.apache.doris.analysis.GrantStmt;
import org.apache.doris.analysis.ResourcePattern;
import org.apache.doris.analysis.RevokeStmt;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.analysis.WorkloadGroupPattern;
import org.apache.doris.catalog.AccessPrivilege;
import org.apache.doris.catalog.AccessPrivilegeWithCols;
import org.apache.doris.catalog.DomainResolver;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.MysqlPassword;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.PrivInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Set;

public class AuthTest {

    private Auth auth;
    private AccessControllerManager accessManager;
    @Mocked
    public Env env;
    @Mocked
    private Analyzer analyzer;
    @Mocked
    private EditLog editLog;
    @Mocked
    private ConnectContext ctx;

    private MockDomainResolver resolver;

    // Thread is not mockable in Jmockit, so use a subclass instead.
    private static final class MockDomainResolver extends DomainResolver {
        public MockDomainResolver(Auth auth) {
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
                default:
                    break;
            }
            return true;
        }
    }

    @Before
    public void setUp() throws NoSuchMethodException, SecurityException {
        auth = new Auth();
        accessManager = new AccessControllerManager(auth);
        new Expectations() {
            {

                analyzer.getDefaultCatalog();
                minTimes = 0;
                result = InternalCatalog.INTERNAL_CATALOG_NAME;

                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessManager;

                env.getAuth();
                minTimes = 0;
                result = auth;

                env.getEditLog();
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

        resolver = new MockDomainResolver(auth);
        Config.enable_col_auth = true;
    }

    @Test
    public void test()
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, UserException {
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
            Set<UserIdentity> roleUsers = auth.getRoleUsers(userIdentity.toDefaultRoleName());
            Assert.assertTrue(roleUsers.contains(userIdentity));
        } catch (DdlException e) {
            Assert.fail();
        }

        // 1.1 create cmy@% again with IF NOT EXISTS
        userIdentity = new UserIdentity("cmy", "%");
        userDesc = new UserDesc(userIdentity, "54321", true);
        createUserStmt = new CreateUserStmt(true, userDesc, null);
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

        // 1.2 create cmy@% again without IF NOT EXISTS
        userIdentity = new UserIdentity("cmy", "%");
        userDesc = new UserDesc(userIdentity, "54321", true);
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

        // 2. check if cmy from specified ip can access to palo
        List<UserIdentity> currentUser = Lists.newArrayList();
        Assert.assertTrue(
                auth.checkPlainPasswordForTest("cmy", "192.168.0.1", "12345",
                        currentUser));
        Assert.assertFalse(auth.checkPlainPasswordForTest("cmy", "192.168.0.1",
                "123456", null));
        Assert.assertFalse(auth.checkPlainPasswordForTest("other:cmy", "192.168.0.1", "12345", null));
        Assert.assertEquals(currentUser.get(0), userIdentity);

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

        // 4. check if zhangsan from specified ip can access to doris
        Assert.assertTrue(auth.checkPlainPasswordForTest("zhangsan", "192.168.0.1", "12345", null));
        Assert.assertFalse(auth.checkPlainPasswordForTest("zhangsan", "172.168.0.1", "12345", null));

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

        hasException = false;
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
        Assert.assertTrue(auth.checkPlainPasswordForTest("zhangsan", "172.18.1.1",
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
            Set<UserIdentity> roleUsers = auth.getRoleUsers(userIdentity.toDefaultRoleName());
            Assert.assertTrue(roleUsers.contains(userIdentity));
        } catch (DdlException e) {
            Assert.fail();
        }

        // 5.1 resolve domain [palo.domain1]
        resolver.runAfterCatalogReady();

        // 6. check if user from resolved ip can access to palo
        Assert.assertTrue(auth.checkPlainPasswordForTest("zhangsan", "10.1.1.1",
                "12345", null));
        Assert.assertFalse(auth.checkPlainPasswordForTest("zhangsan", "10.1.1.1",
                "123456", null));
        Assert.assertFalse(auth.checkPlainPasswordForTest("zhangsan", "11.1.1.1",
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

        Assert.assertTrue(auth.checkPlainPasswordForTest("lisi", "20.1.1.1",
                "123456", null));
        Assert.assertFalse(auth.checkPlainPasswordForTest("lisi", "10.1.1.1",
                "123456", null));
        Assert.assertFalse(auth.checkPlainPasswordForTest("lisi", "20.1.1.2",
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
        List<AccessPrivilegeWithCols> privileges = Lists
                .newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.CREATE_PRIV),
                        new AccessPrivilegeWithCols(AccessPrivilege.DROP_PRIV));
        GrantStmt grantStmt = new GrantStmt(new UserIdentity("cmy", "%"), null, tablePattern, privileges);

        try {
            grantStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        List<UserIdentity> currentUser2 = Lists.newArrayList();
        auth.checkPlainPasswordForTest("cmy", "172.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        // check auth before grant
        Assert.assertFalse(
                accessManager.checkDbPriv(currentUser2.get(0), "db1",
                        PrivPredicate.CREATE));

        try {
            auth.grant(grantStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // 9.1 check auth
        Assert.assertTrue(accessManager.checkDbPriv(currentUser2.get(0), "db1",
                PrivPredicate.CREATE));
        UserIdentity zhangsan1 = UserIdentity
                .createAnalyzedUserIdentWithIp("zhangsan",
                        "172.1.1.1");
        Assert.assertFalse(accessManager.checkDbPriv(zhangsan1, "db1",
                PrivPredicate.CREATE));

        // 10. grant auth for non exist user
        tablePattern = new TablePattern("*", "*");
        privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.CREATE_PRIV),
                new AccessPrivilegeWithCols(AccessPrivilege.DROP_PRIV));
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
        privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.SELECT_PRIV),
                new AccessPrivilegeWithCols(AccessPrivilege.DROP_PRIV));
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
        privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.SELECT_PRIV),
                new AccessPrivilegeWithCols(AccessPrivilege.DROP_PRIV));
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
        auth.checkPlainPasswordForTest("zhangsan", "192.168.1.1", "12345",
                currentUser2);
        Assert.assertEquals(1, currentUser2.size());

        Assert.assertTrue(accessManager.checkDbPriv(currentUser2.get(0), "db1",
                PrivPredicate.SELECT));
        Assert.assertFalse(accessManager.checkGlobalPriv(currentUser2.get(0), PrivPredicate.SELECT));
        Assert.assertTrue(
                accessManager.checkTblPriv(currentUser2.get(0), "db1",
                        "tbl1", PrivPredicate.SELECT));

        // 13. grant tbl auth to exist user
        tablePattern = new TablePattern("db2", "tbl2");
        privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.ALTER_PRIV),
                new AccessPrivilegeWithCols(AccessPrivilege.DROP_PRIV));
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
        auth.checkPlainPasswordForTest("zhangsan", "192.168.1.1", "12345",
                currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertFalse(
                accessManager.checkDbPriv(currentUser2.get(0), "db2",
                        PrivPredicate.SELECT));
        Assert.assertFalse(accessManager.checkGlobalPriv(currentUser2.get(0), PrivPredicate.SELECT));
        Assert.assertTrue(
                accessManager.checkTblPriv(currentUser2.get(0), "db2", "tbl2",
                        PrivPredicate.DROP));

        // 13.1 grant external ctl tbl auth to exist user
        tablePattern = new TablePattern("ext_ctl", "ext_db1", "ext_tbl1");
        privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.SELECT_PRIV));
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
        auth.checkPlainPasswordForTest("zhangsan", "192.168.1.1", "12345",
                currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(
                accessManager.checkTblPriv(currentUser2.get(0), "ext_ctl",
                        "ext_db1",
                        "ext_tbl1", PrivPredicate.SELECT));
        Assert.assertFalse(
                accessManager.checkTblPriv(currentUser2.get(0), "ext_db1",
                        "ext_tbl1",
                        PrivPredicate.SELECT));

        // 14. grant db auth to zhangsan@['palo.domain1']
        tablePattern = new TablePattern("db3", "*");
        privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.ALTER_PRIV),
                new AccessPrivilegeWithCols(AccessPrivilege.DROP_PRIV));
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
        auth.checkPlainPasswordForTest("zhangsan", "10.1.1.1", "12345",
                currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(accessManager.checkDbPriv(currentUser2.get(0), "db3",
                PrivPredicate.ALTER));
        // 15. grant new auth to exist priv entry (exist ALTER/DROP, add SELECT)
        tablePattern = new TablePattern("db3", "*");
        privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.SELECT_PRIV));
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
        auth.checkPlainPasswordForTest("zhangsan", "10.1.1.1", "12345",
                currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(accessManager.checkDbPriv(currentUser2.get(0), "db3",
                PrivPredicate.SELECT));

        currentUser2.clear();
        auth.checkPlainPasswordForTest("zhangsan", "10.1.1.2", "12345",
                currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(accessManager.checkDbPriv(currentUser2.get(0), "db3",
                PrivPredicate.ALTER));
        currentUser2.clear();
        auth.checkPlainPasswordForTest("zhangsan", "10.1.1.3", "12345",
                currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(accessManager.checkDbPriv(currentUser2.get(0), "db3",
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
        privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.SELECT_PRIV));
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
        privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.SELECT_PRIV));
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
        privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.SELECT_PRIV));
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
        privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.CREATE_PRIV));
        revokeStmt = new RevokeStmt(new UserIdentity("cmy", "%"), null, tablePattern, privileges);

        try {
            revokeStmt.analyze(analyzer);
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPasswordForTest("cmy", "172.1.1.1", "12345", currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(accessManager.checkDbPriv(currentUser2.get(0), "db",
                PrivPredicate.CREATE));
        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertFalse(accessManager.checkDbPriv(currentUser2.get(0), "db",
                PrivPredicate.CREATE));
        Assert.assertTrue(accessManager.checkDbPriv(currentUser2.get(0), "db",
                PrivPredicate.DROP));

        // 19. revoke tbl privs from user @ ip
        tablePattern = new TablePattern("db2", "tbl2");
        privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.ALTER_PRIV));
        revokeStmt = new RevokeStmt(new UserIdentity("zhangsan", "192.%"), null, tablePattern, privileges);

        try {
            revokeStmt.analyze(analyzer);
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPasswordForTest("zhangsan", "192.1.1.1", "12345",
                currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(
                accessManager.checkTblPriv(currentUser2.get(0), "db2",
                        "tbl2", PrivPredicate.ALTER));
        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPasswordForTest("zhangsan", "192.1.1.1", "12345",
                currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertFalse(
                accessManager.checkTblPriv(currentUser2.get(0), "db2",
                        "tbl2", PrivPredicate.ALTER));
        Assert.assertTrue(accessManager.checkDbPriv(currentUser2.get(0), "db1",
                PrivPredicate.SELECT));

        // 20. revoke privs from non exist user @ domain
        tablePattern = new TablePattern("db2", "tbl2");
        privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.ALTER_PRIV));
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
        privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.ALTER_PRIV));
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
        privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.DROP_PRIV));
        revokeStmt = new RevokeStmt(new UserIdentity("zhangsan", "palo.domain1", true), null, tablePattern, privileges);

        try {
            revokeStmt.analyze(analyzer);
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPasswordForTest("zhangsan", "10.1.1.1", "12345",
                currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(accessManager.checkDbPriv(currentUser2.get(0), "db3",
                PrivPredicate.DROP));

        try {
            auth.revoke(revokeStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        currentUser2.clear();
        auth.checkPlainPasswordForTest("zhangsan", "10.1.1.1", "12345",
                currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertFalse(
                accessManager.checkDbPriv(currentUser2.get(0), "db3",
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
        CreateRoleStmt roleStmt = new CreateRoleStmt(Role.ADMIN_ROLE);
        hasException = false;
        try {
            roleStmt.analyze(analyzer);
        } catch (UserException e1) {
            e1.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 23. create operator role, which is not allowed
        roleStmt = new CreateRoleStmt(Role.OPERATOR_ROLE);
        hasException = false;
        try {
            roleStmt.analyze(analyzer);
        } catch (UserException e1) {
            e1.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 23. create role start with RoleManager.DEFAULT_ROLE_PREFIX, which is not allowed
        roleStmt = new CreateRoleStmt(RoleManager.DEFAULT_ROLE_PREFIX + "role");
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

        // 24.1 create role again with IF NOT EXISTS
        roleStmt = new CreateRoleStmt(true, "role1");
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

        // 24.2 create role again without IF NOT EXISTS
        roleStmt = new CreateRoleStmt(false, "role1");
        try {
            roleStmt.analyze(analyzer);
        } catch (UserException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        hasException = false;
        try {
            auth.createRole(roleStmt);
        } catch (DdlException e1) {
            e1.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 25. grant auth to non exist role, will create this new role
        privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.DROP_PRIV),
                new AccessPrivilegeWithCols(AccessPrivilege.SELECT_PRIV));
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
        privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.DROP_PRIV),
                new AccessPrivilegeWithCols(AccessPrivilege.SELECT_PRIV));
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
        auth.checkPlainPasswordForTest("wangwu", "10.17.2.1", "12345",
                currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(accessManager.checkDbPriv(currentUser2.get(0), "db4",
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
        auth.checkPlainPasswordForTest("chenliu", "20.1.1.1", "12345",
                currentUser2);
        Assert.assertEquals(0, currentUser2.size());
        resolver.runAfterCatalogReady();
        currentUser2.clear();
        auth.checkPlainPasswordForTest("chenliu", "20.1.1.1", "12345",
                currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertTrue(accessManager.checkDbPriv(currentUser2.get(0), "db4",
                PrivPredicate.DROP));

        // 29. revoke auth on non exist db from role1
        privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.DROP_PRIV));
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
        privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.DROP_PRIV));
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
        auth.checkPlainPasswordForTest("chenliu", "20.1.1.1", "12345",
                currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertFalse(
                accessManager.checkDbPriv(currentUser2.get(0), "db4",
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
        auth.checkPlainPasswordForTest("chenliu", "20.1.1.1", "12345",
                currentUser2);
        Assert.assertEquals(1, currentUser2.size());
        Assert.assertFalse(
                accessManager.checkDbPriv(currentUser2.get(0), "db4",
                        PrivPredicate.DROP));

        // 31.1 drop role again with IF EXISTS
        dropRoleStmt = new DropRoleStmt(true, "role1");
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

        // 31.2 drop role again without IF EXISTS
        dropRoleStmt = new DropRoleStmt(false, "role1");
        try {
            dropRoleStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        hasException = false;
        try {
            auth.dropRole(dropRoleStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

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

        Assert.assertFalse(
                auth.checkPlainPasswordForTest("cmy", "192.168.0.1", "12345",
                        null));
        Assert.assertTrue(auth.checkPlainPasswordForTest("zhangsan", "192.168.0.1",
                "12345", null));

        // 32.1 drop user cmy@"%" again with IF EXISTS
        dropUserStmt = new DropUserStmt(true, new UserIdentity("cmy", "%"));
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

        // 32.2 drop user cmy@"%" again without IF EXISTS
        dropUserStmt = new DropUserStmt(false, new UserIdentity("cmy", "%"));
        try {
            dropUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        hasException = false;
        try {
            auth.dropUser(dropUserStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 33. drop user zhangsan@"192.%"
        dropUserStmt = new DropUserStmt(new UserIdentity("zhangsan", "192.%"));
        try {
            dropUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(
                auth.checkPlainPasswordForTest("zhangsan", "192.168.0.1", "12345",
                        null));

        try {
            auth.dropUser(dropUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }
        Assert.assertFalse(
                auth.checkPlainPasswordForTest("zhangsan", "192.168.0.1", "12345",
                        null));
        Assert.assertTrue(
                auth.checkPlainPasswordForTest("zhangsan", "10.1.1.1", "12345",
                        null));

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

        Assert.assertTrue(
                auth.checkPlainPasswordForTest("zhangsan", "10.1.1.1", "12345",
                        null));

        try {
            auth.createUser(createUserStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertFalse(
                auth.checkPlainPasswordForTest("zhangsan", "10.1.1.1", "12345",
                        null));
        Assert.assertTrue(
                auth.checkPlainPasswordForTest("zhangsan", "10.1.1.1", "abcde",
                        null));

        // 35. drop user zhangsan@['palo.domain1']
        dropUserStmt = new DropUserStmt(new UserIdentity("zhangsan", "palo.domain1", true));
        try {
            dropUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(
                auth.checkPlainPasswordForTest("zhangsan", "10.1.1.2", "12345",
                        null));

        try {
            auth.dropUser(dropUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }
        Assert.assertTrue(
                auth.checkPlainPasswordForTest("zhangsan", "10.1.1.2", "12345",
                        null));

        resolver.runAfterCatalogReady();
        Assert.assertFalse(
                auth.checkPlainPasswordForTest("zhangsan", "10.1.1.2", "12345",
                        null));
        Assert.assertTrue(
                auth.checkPlainPasswordForTest("zhangsan", "10.1.1.1", "abcde",
                        null));

        // 36. drop user lisi@['palo.domain1']
        dropUserStmt = new DropUserStmt(new UserIdentity("lisi", "palo.domain2", true));
        try {
            dropUserStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(
                auth.checkPlainPasswordForTest("lisi", "20.1.1.1", "123456",
                        null));
        Assert.assertFalse(
                auth.checkPlainPasswordForTest("lisi", "10.1.1.1", "123456",
                        null));

        try {
            auth.dropUser(dropUserStmt);
        } catch (DdlException e) {
            Assert.fail();
        }
        Assert.assertTrue(
                auth.checkPlainPasswordForTest("lisi", "20.1.1.1", "123456",
                        null));
        Assert.assertFalse(
                auth.checkPlainPasswordForTest("lisi", "10.1.1.1", "123456",
                        null));

        resolver.runAfterCatalogReady();
        Assert.assertFalse(
                auth.checkPlainPasswordForTest("lisi", "20.1.1.1", "123456",
                        null));
        Assert.assertFalse(
                auth.checkPlainPasswordForTest("lisi", "10.1.1.1", "123456",
                        null));

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
        Assert.assertFalse(
                auth.checkPlainPasswordForTest("zhangsan", "10.1.1.1", "abcde",
                        null));

        // 38. drop root user(not allowed)
        dropUserStmt = new DropUserStmt(new UserIdentity("root", "%"));
        try {
            dropUserStmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            e.printStackTrace();
        }

        // 39. create root user(not allowed)
        userIdentity = new UserIdentity("root", "%");
        userDesc = new UserDesc(userIdentity, "12345", true);
        createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            e.printStackTrace();
        }

        // 40. create new user and grant node_priv to it
        final UserIdentity opUser = new UserIdentity("op_user", "%");
        userDesc = new UserDesc(opUser, "12345", true);
        createUserStmt = new CreateUserStmt(false, userDesc, null);
        createUserStmt.analyze(analyzer);
        auth.createUser(createUserStmt);

        privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.NODE_PRIV));
        // 40.1 grant to non-global level, which is not allowed
        grantStmt = new GrantStmt(opUser, null, new TablePattern("db1", "*"), privileges);
        try {
            grantStmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            e.printStackTrace();
        }

        grantStmt = new GrantStmt(opUser, null, new TablePattern("db1", "tbl"), privileges);
        try {
            grantStmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            e.printStackTrace();
        }
        // 40.2 grant to global level
        new Expectations() {
            {
                ctx.getCurrentUserIdentity();
                minTimes = 1;
                result = opUser;
            }
        };
        Assert.assertFalse(accessManager.checkGlobalPriv(ctx, PrivPredicate.OPERATOR));
        grantStmt = new GrantStmt(opUser, null, new TablePattern("*", "*", "*"), privileges);
        // first, use op_user itself to grant node_priv, which is not allowed
        try {
            new Expectations() {
                {
                    ctx.getCurrentUserIdentity();
                    minTimes = 1;
                    result = opUser;
                }
            };
            grantStmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            e.printStackTrace();
        }
        // second, use root to grant node_priv
        try {
            new Expectations() {
                {
                    ctx.getCurrentUserIdentity();
                    minTimes = 1;
                    result = UserIdentity.ROOT;
                }
            };
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        // switch to op_user to check it has node_priv
        new Expectations() {
            {
                ctx.getCurrentUserIdentity();
                minTimes = 2;
                result = opUser;
            }
        };
        Assert.assertTrue(accessManager.checkGlobalPriv(ctx, PrivPredicate.OPERATOR));
        // Now, op_user only has node_priv, it can not grant node_priv to other user.
        // create otherOpUser first
        UserIdentity otherOpUser = new UserIdentity("other_op_user", "%");
        userDesc = new UserDesc(otherOpUser, "12345", true);
        createUserStmt = new CreateUserStmt(false, userDesc, null);
        new Expectations() {
            {
                ctx.getCurrentUserIdentity();
                minTimes = 1;
                result = UserIdentity.ROOT;
            }
        };
        createUserStmt.analyze(analyzer);
        auth.createUser(createUserStmt);
        // try grant, it should fail
        grantStmt = new GrantStmt(otherOpUser, null, new TablePattern("*", "*"), privileges);
        try {
            grantStmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            e.printStackTrace();
        }
        // Now, we grant grant_priv to opUser, and check if it can than grant node_priv to other user
        privileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.GRANT_PRIV));
        grantStmt = new GrantStmt(opUser, null, new TablePattern("*", "*"), privileges);
        try {
            new Expectations() {
                {
                    ctx.getCurrentUserIdentity();
                    minTimes = 2;
                    result = UserIdentity.ROOT;
                }
            };
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        // grant node_priv to other_op_user
        grantStmt = new GrantStmt(otherOpUser, null, new TablePattern("*", "*"), privileges);
        try {
            new Expectations() {
                {
                    ctx.getCurrentUserIdentity();
                    minTimes = 1;
                    result = opUser;
                }
            };
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // test domain override
        // 1. create a domain user
        new Expectations() {
            {
                ctx.getCurrentUserIdentity();
                minTimes = 1;
                result = UserIdentity.ROOT;
            }
        };
        UserIdentity domainUser = new UserIdentity("test_domain_user", "palo.domain1", true);
        userDesc = new UserDesc(domainUser, "12345", true);
        createUserStmt = new CreateUserStmt(false, userDesc, null);
        createUserStmt.analyze(analyzer);
        auth.createUser(createUserStmt);
        // 2. create a normal user with same ip in domain
        UserIdentity normalUser = new UserIdentity("test_domain_user", "10.1.1.1");
        userDesc = new UserDesc(normalUser, "12345", true);
        createUserStmt = new CreateUserStmt(false, userDesc, null);
        createUserStmt.analyze(analyzer);
        auth.createUser(createUserStmt);
        // 3. run resolve
        resolver.runAfterCatalogReady();
        // 4. user grant to test that normal user is not overwrite by domain resolve
        grantStmt = new GrantStmt(normalUser, null, new TablePattern("*", "*", "*"), privileges);
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testColAuth() {
        // create user
        UserIdentity userIdentity = new UserIdentity("colUser", "%");
        UserDesc userDesc = new UserDesc(userIdentity, "12345", true);
        CreateUserStmt createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
            auth.createUser(createUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // test table is *
        GrantStmt grantStmt = new GrantStmt(userIdentity, null, new TablePattern("db1", "*"), Lists.newArrayList(
                new AccessPrivilegeWithCols(AccessPrivilege.SELECT_PRIV, Lists.newArrayList("a", "b"))));
        try {
            grantStmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            e.printStackTrace();
        }

        // test CREATE_PRIV with col
        grantStmt = new GrantStmt(userIdentity, null, new TablePattern("db1", "tbl1"), Lists.newArrayList(
                new AccessPrivilegeWithCols(AccessPrivilege.CREATE_PRIV, Lists.newArrayList("a", "b"))));
        try {
            grantStmt.analyze(analyzer);
            Assert.fail();
        } catch (UserException e) {
            e.printStackTrace();
        }

        // test Select_PRIV with col
        grantStmt = new GrantStmt(userIdentity, null, new TablePattern("db1", "tbl1"), Lists.newArrayList(
                new AccessPrivilegeWithCols(AccessPrivilege.SELECT_PRIV, Lists.newArrayList("a", "b"))));
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        // check has select priv of column 'a'
        try {
            accessManager
                    .checkColumnsPriv(userIdentity, "db1", "tbl1",
                            Sets.newHashSet("a"), PrivPredicate.SELECT);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        // check has select priv of column 'c'
        try {
            accessManager
                    .checkColumnsPriv(userIdentity, "db1", "tbl1",
                            Sets.newHashSet("c"), PrivPredicate.SELECT);
            Assert.fail();
        } catch (UserException e) {
            e.printStackTrace();
        }
        // check has load priv of column 'a'
        try {
            accessManager
                    .checkColumnsPriv(userIdentity, "db1", "tbl1",
                            Sets.newHashSet("a"), PrivPredicate.LOAD);
            Assert.fail();
        } catch (UserException e) {
            e.printStackTrace();
        }
        // check 'create_priv' use checkColumnsPriv
        try {
            accessManager
                    .checkColumnsPriv(userIdentity, "db1", "tbl1",
                            Sets.newHashSet("a"), PrivPredicate.CREATE);
            Assert.fail();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // check show priv on ctl when has col priv
        boolean hasPriv = accessManager.checkCtlPriv(userIdentity, Auth.DEFAULT_CATALOG, PrivPredicate.SHOW);
        if (!hasPriv) {
            Assert.fail();
        }
        // check show priv on db when has col priv
        hasPriv = accessManager.checkDbPriv(userIdentity, Auth.DEFAULT_CATALOG, "db1", PrivPredicate.SHOW);
        if (!hasPriv) {
            Assert.fail();
        }
        // check show priv on tbl when has col priv
        hasPriv = accessManager.checkTblPriv(userIdentity, Auth.DEFAULT_CATALOG, "db1", "tbl1", PrivPredicate.SHOW);
        if (!hasPriv) {
            Assert.fail();
        }
        // check select priv on tbl when has col priv
        hasPriv = accessManager.checkTblPriv(userIdentity, Auth.DEFAULT_CATALOG, "db1", "tbl1", PrivPredicate.SELECT);
        if (hasPriv) {
            Assert.fail();
        }
    }

    @Test
    public void testGrantRole() {
        UserIdentity userIdentity = new UserIdentity("testUser", "%");
        UserDesc userDesc = new UserDesc(userIdentity, "12345", true);
        String role = "role1";
        // 1. create role
        CreateRoleStmt createRoleStmt = new CreateRoleStmt(role);
        try {
            createRoleStmt.analyze(analyzer);
            auth.createRole(createRoleStmt);
        } catch (UserException e1) {
            e1.printStackTrace();
            Assert.fail();
        }
        // grant select_priv on db 'db1' to role 'role1'
        GrantStmt grantStmt = new GrantStmt(null, role, new TablePattern("db1", "*"),
                Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.SELECT_PRIV)));
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // create user with no role
        CreateUserStmt createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
            auth.createUser(createUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertFalse(accessManager
                .checkDbPriv(userIdentity, "db1", PrivPredicate.SELECT));
        // grant 'role1' to testUser
        grantStmt = new GrantStmt(Lists.newArrayList(role), userIdentity);
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(accessManager
                .checkDbPriv(userIdentity, "db1", PrivPredicate.SELECT));
        // revoke 'role1' from testUser
        RevokeStmt revokeStmt = new RevokeStmt(Lists.newArrayList(role), userIdentity);
        try {
            revokeStmt.analyze(analyzer);
            auth.revoke(revokeStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertFalse(accessManager
                .checkDbPriv(userIdentity, "db1", PrivPredicate.SELECT));
        // grant not exist role to testUser
        grantStmt = new GrantStmt(Lists.newArrayList("norole"), userIdentity);
        try {
            grantStmt.analyze(analyzer);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        boolean hasException = false;
        try {
            auth.grant(grantStmt);
        } catch (DdlException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);
    }

    @Test
    public void testResource() {
        UserIdentity userIdentity = new UserIdentity("testUser", "%");
        String role = "role0";
        String resourceName = "spark0";
        ResourcePattern resourcePattern = new ResourcePattern(resourceName);
        String anyResource = "*";
        ResourcePattern anyResourcePattern = new ResourcePattern(anyResource);
        List<AccessPrivilegeWithCols> usagePrivileges = Lists
                .newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.USAGE_PRIV));
        UserDesc userDesc = new UserDesc(userIdentity, "12345", true);

        // ------ grant|revoke resource to|from user ------
        // 1. create user with no role
        CreateUserStmt createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
            auth.createUser(createUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // 2. grant usage_priv on resource 'spark0' to 'testUser'@'%'
        GrantStmt grantStmt = new GrantStmt(userIdentity, null, resourcePattern, usagePrivileges);
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(accessManager.checkResourcePriv(userIdentity, resourceName, PrivPredicate.USAGE));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 3. revoke usage_priv on resource 'spark0' from 'testUser'@'%'
        RevokeStmt revokeStmt = new RevokeStmt(userIdentity, null, resourcePattern, usagePrivileges);
        try {
            revokeStmt.analyze(analyzer);
            auth.revoke(revokeStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertFalse(accessManager.checkResourcePriv(userIdentity, resourceName, PrivPredicate.USAGE));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));
        // 3.1 grant 'notBelongToResourcePrivileges' on resource 'spark0' to 'testUser'@'%'
        for (int i = 0; i < Privilege.notBelongToResourcePrivileges.length; i++) {
            List<AccessPrivilegeWithCols> notAllowedPrivileges = Lists
                    .newArrayList(new AccessPrivilegeWithCols(
                            AccessPrivilege.fromName(Privilege.notBelongToResourcePrivileges[i].getName())));
            grantStmt = new GrantStmt(userIdentity, null, resourcePattern, notAllowedPrivileges);
            try {
                grantStmt.analyze(analyzer);
                Assert.fail(String.format("Can not grant/revoke %s to/from any other users or roles",
                        Privilege.notBelongToResourcePrivileges[i]));
            } catch (UserException e) {
                e.printStackTrace();
            }
        }
        // 4. drop user
        DropUserStmt dropUserStmt = new DropUserStmt(userIdentity);
        try {
            dropUserStmt.analyze(analyzer);
            auth.dropUser(dropUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // ------ grant|revoke resource to|from role ------
        // 1. create role
        CreateRoleStmt roleStmt = new CreateRoleStmt(role);
        try {
            roleStmt.analyze(analyzer);
            auth.createRole(roleStmt);
        } catch (UserException e1) {
            e1.printStackTrace();
            Assert.fail();
        }
        // grant usage_priv on resource 'spark0' to role 'role0'
        grantStmt = new GrantStmt(null, role, resourcePattern, usagePrivileges);
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        // 2. create user with role
        createUserStmt = new CreateUserStmt(false, userDesc, role);
        try {
            createUserStmt.analyze(analyzer);
            auth.createUser(createUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(accessManager.checkResourcePriv(userIdentity, resourceName, PrivPredicate.USAGE));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 3. revoke usage_priv on resource 'spark0' from role 'role0'
        revokeStmt = new RevokeStmt(null, role, resourcePattern, usagePrivileges);
        try {
            revokeStmt.analyze(analyzer);
            auth.revoke(revokeStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        // also revoke from user with this role
        Assert.assertFalse(accessManager.checkResourcePriv(userIdentity, resourceName, PrivPredicate.USAGE));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 4. drop user and role
        dropUserStmt = new DropUserStmt(userIdentity);
        try {
            dropUserStmt.analyze(analyzer);
            auth.dropUser(dropUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        DropRoleStmt dropRoleStmt = new DropRoleStmt(role);
        try {
            dropRoleStmt.analyze(analyzer);
            auth.dropRole(dropRoleStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // ------ grant|revoke any resource to|from user ------
        // 1. create user with no role
        createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
            auth.createUser(createUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // 2. grant usage_priv on resource '*' to 'testUser'@'%'
        grantStmt = new GrantStmt(userIdentity, null, anyResourcePattern, usagePrivileges);
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(accessManager.checkResourcePriv(userIdentity, resourceName, PrivPredicate.USAGE));
        Assert.assertTrue(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));
        Assert.assertTrue(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.SHOW_RESOURCES));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.SHOW));

        // 3. revoke usage_priv on resource '*' from 'testUser'@'%'
        revokeStmt = new RevokeStmt(userIdentity, null, anyResourcePattern, usagePrivileges);
        try {
            revokeStmt.analyze(analyzer);
            auth.revoke(revokeStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertFalse(accessManager.checkResourcePriv(userIdentity, resourceName, PrivPredicate.USAGE));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.SHOW_RESOURCES));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.SHOW));

        // 4. drop user
        dropUserStmt = new DropUserStmt(userIdentity);
        try {
            dropUserStmt.analyze(analyzer);
            auth.dropUser(dropUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // ------ grant|revoke any resource to|from role ------
        // 1. create role
        roleStmt = new CreateRoleStmt(role);
        try {
            roleStmt.analyze(analyzer);
            auth.createRole(roleStmt);
        } catch (UserException e1) {
            e1.printStackTrace();
            Assert.fail();
        }
        // grant usage_priv on resource '*' to role 'role0'
        grantStmt = new GrantStmt(null, role, anyResourcePattern, usagePrivileges);
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        // 2. create user with role
        createUserStmt = new CreateUserStmt(false, userDesc, role);
        try {
            createUserStmt.analyze(analyzer);
            auth.createUser(createUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(accessManager.checkResourcePriv(userIdentity, resourceName, PrivPredicate.USAGE));
        Assert.assertTrue(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 3. revoke usage_priv on resource '*' from role 'role0'
        revokeStmt = new RevokeStmt(null, role, anyResourcePattern, usagePrivileges);
        try {
            revokeStmt.analyze(analyzer);
            auth.revoke(revokeStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        // also revoke from user with this role
        Assert.assertFalse(accessManager.checkResourcePriv(userIdentity, resourceName, PrivPredicate.USAGE));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 4. drop user and role
        dropUserStmt = new DropUserStmt(userIdentity);
        try {
            dropUserStmt.analyze(analyzer);
            auth.dropUser(dropUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        dropRoleStmt = new DropRoleStmt(role);
        try {
            dropRoleStmt.analyze(analyzer);
            auth.dropRole(dropRoleStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // ------ error case ------
        boolean hasException = false;
        createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
            auth.createUser(createUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // 1. grant db table priv to resource
        List<AccessPrivilegeWithCols> privileges = Lists
                .newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.SELECT_PRIV));
        grantStmt = new GrantStmt(userIdentity, null, resourcePattern, privileges);
        hasException = false;
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 2. grant resource priv to db table
        TablePattern tablePattern = new TablePattern("db1", "*");
        GrantStmt grantStmt2 = new GrantStmt(userIdentity, null, tablePattern, usagePrivileges);
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "Can not grant/revoke USAGE_PRIV to/from database or table", () -> grantStmt2.analyze(analyzer));

        // 3. grant resource prov to role on db.table
        tablePattern = new TablePattern("db1", "*");
        GrantStmt grantStmt3 = new GrantStmt(userIdentity, "test_role", tablePattern, usagePrivileges);
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "Can not grant/revoke USAGE_PRIV to/from database or table", () -> grantStmt3.analyze(analyzer));
    }

    @Test
    public void testWorkloadGroupPriv() {
        UserIdentity userIdentity = new UserIdentity("testUser", "%");
        String role = "role0";
        String workloadGroupName = "g1";
        WorkloadGroupPattern workloadGroupPattern = new WorkloadGroupPattern(workloadGroupName);
        String anyWorkloadGroup = "%";
        WorkloadGroupPattern anyWorkloadGroupPattern = new WorkloadGroupPattern(anyWorkloadGroup);
        List<AccessPrivilegeWithCols> usagePrivileges = Lists
                .newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.USAGE_PRIV));
        UserDesc userDesc = new UserDesc(userIdentity, "12345", true);

        // ------ grant|revoke workload group to|from user ------
        // 1. create user with no role
        CreateUserStmt createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
            auth.createUser(createUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // 2. grant usage_priv on workload group to user
        GrantStmt grantStmt = new GrantStmt(userIdentity, null, workloadGroupPattern, usagePrivileges);
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(accessManager.checkWorkloadGroupPriv(userIdentity, workloadGroupName, PrivPredicate.USAGE));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 3. revoke usage_priv on workload group from user
        RevokeStmt revokeStmt = new RevokeStmt(userIdentity, null, workloadGroupPattern, usagePrivileges);
        try {
            revokeStmt.analyze(analyzer);
            auth.revoke(revokeStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertFalse(accessManager.checkWorkloadGroupPriv(userIdentity, workloadGroupName, PrivPredicate.USAGE));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));
        // 3.1 grant 'notBelongToWorkloadGroupPrivileges'
        for (int i = 0; i < Privilege.notBelongToWorkloadGroupPrivileges.length; i++) {
            List<AccessPrivilegeWithCols> notAllowedPrivileges = Lists.newArrayList(
                    new AccessPrivilegeWithCols(
                            AccessPrivilege.fromName(Privilege.notBelongToWorkloadGroupPrivileges[i].getName())));
            grantStmt = new GrantStmt(userIdentity, null, workloadGroupPattern, notAllowedPrivileges);
            try {
                grantStmt.analyze(analyzer);
                Assert.fail(String.format("Can not grant/revoke %s to/from any other users or roles",
                        Privilege.notBelongToWorkloadGroupPrivileges[i]));
            } catch (UserException e) {
                e.printStackTrace();
            }
        }
        // 4. drop user
        DropUserStmt dropUserStmt = new DropUserStmt(userIdentity);
        try {
            dropUserStmt.analyze(analyzer);
            auth.dropUser(dropUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // ------ grant|revoke workload group to|from role ------
        // 1. create role
        CreateRoleStmt roleStmt = new CreateRoleStmt(role);
        try {
            roleStmt.analyze(analyzer);
            auth.createRole(roleStmt);
        } catch (UserException e1) {
            e1.printStackTrace();
            Assert.fail();
        }
        // grant usage_priv on workload group to role
        grantStmt = new GrantStmt(null, role, workloadGroupPattern, usagePrivileges);
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        // 2. create user with role
        createUserStmt = new CreateUserStmt(false, userDesc, role);
        try {
            createUserStmt.analyze(analyzer);
            auth.createUser(createUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(accessManager.checkWorkloadGroupPriv(userIdentity, workloadGroupName, PrivPredicate.USAGE));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 3. revoke usage_priv on workload group from role
        revokeStmt = new RevokeStmt(null, role, workloadGroupPattern, usagePrivileges);
        try {
            revokeStmt.analyze(analyzer);
            auth.revoke(revokeStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        // also revoke from user with this role
        Assert.assertFalse(accessManager.checkWorkloadGroupPriv(userIdentity, workloadGroupName, PrivPredicate.USAGE));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 4. drop user and role
        dropUserStmt = new DropUserStmt(userIdentity);
        try {
            dropUserStmt.analyze(analyzer);
            auth.dropUser(dropUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        DropRoleStmt dropRoleStmt = new DropRoleStmt(role);
        try {
            dropRoleStmt.analyze(analyzer);
            auth.dropRole(dropRoleStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // ------ grant|revoke any workload group to|from user ------
        // 1. create user with no role
        createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
            auth.createUser(createUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // 2. grant usage_priv on workload group '%' to user
        grantStmt = new GrantStmt(userIdentity, null, anyWorkloadGroupPattern, usagePrivileges);
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(accessManager.checkWorkloadGroupPriv(userIdentity, workloadGroupName, PrivPredicate.USAGE));
        Assert.assertTrue(accessManager.checkWorkloadGroupPriv(userIdentity, anyWorkloadGroup, PrivPredicate.USAGE));
        Assert.assertTrue(accessManager.checkWorkloadGroupPriv(userIdentity, workloadGroupName,
                PrivPredicate.SHOW_WORKLOAD_GROUP));

        // 3. revoke usage_priv on workload group '%' from user
        revokeStmt = new RevokeStmt(userIdentity, null, anyWorkloadGroupPattern, usagePrivileges);
        try {
            revokeStmt.analyze(analyzer);
            auth.revoke(revokeStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertFalse(accessManager.checkWorkloadGroupPriv(userIdentity, workloadGroupName, PrivPredicate.USAGE));
        Assert.assertFalse(accessManager.checkWorkloadGroupPriv(userIdentity, anyWorkloadGroup, PrivPredicate.USAGE));
        Assert.assertFalse(accessManager.checkWorkloadGroupPriv(userIdentity, workloadGroupName,
                PrivPredicate.SHOW_WORKLOAD_GROUP));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.SHOW));

        // 4. drop user
        dropUserStmt = new DropUserStmt(userIdentity);
        try {
            dropUserStmt.analyze(analyzer);
            auth.dropUser(dropUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // ------ grant|revoke any workload group to|from role ------
        // 1. create role
        roleStmt = new CreateRoleStmt(role);
        try {
            roleStmt.analyze(analyzer);
            auth.createRole(roleStmt);
        } catch (UserException e1) {
            e1.printStackTrace();
            Assert.fail();
        }
        // grant usage_priv on workload group '%' to role
        grantStmt = new GrantStmt(null, role, anyWorkloadGroupPattern, usagePrivileges);
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        // 2. create user with role
        createUserStmt = new CreateUserStmt(false, userDesc, role);
        try {
            createUserStmt.analyze(analyzer);
            auth.createUser(createUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(accessManager.checkWorkloadGroupPriv(userIdentity, workloadGroupName, PrivPredicate.USAGE));
        Assert.assertTrue(accessManager.checkWorkloadGroupPriv(userIdentity, anyWorkloadGroup, PrivPredicate.USAGE));

        // 3. revoke usage_priv on workload group '%' from role
        revokeStmt = new RevokeStmt(null, role, anyWorkloadGroupPattern, usagePrivileges);
        try {
            revokeStmt.analyze(analyzer);
            auth.revoke(revokeStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        // also revoke from user with this role
        Assert.assertFalse(accessManager.checkWorkloadGroupPriv(userIdentity, workloadGroupName, PrivPredicate.USAGE));
        Assert.assertFalse(accessManager.checkWorkloadGroupPriv(userIdentity, anyWorkloadGroup, PrivPredicate.USAGE));

        // 4. drop user and role
        dropUserStmt = new DropUserStmt(userIdentity);
        try {
            dropUserStmt.analyze(analyzer);
            auth.dropUser(dropUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        dropRoleStmt = new DropRoleStmt(role);
        try {
            dropRoleStmt.analyze(analyzer);
            auth.dropRole(dropRoleStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // ------ error case ------
        boolean hasException = false;
        createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
            auth.createUser(createUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // 1. grant db table priv to workload group
        List<AccessPrivilegeWithCols> privileges = Lists
                .newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.SELECT_PRIV));
        grantStmt = new GrantStmt(userIdentity, null, workloadGroupPattern, privileges);
        hasException = false;
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 2. grant workload group priv to db table
        TablePattern tablePattern = new TablePattern("db1", "*");
        GrantStmt grantStmt2 = new GrantStmt(userIdentity, null, tablePattern, usagePrivileges);
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "Can not grant/revoke USAGE_PRIV to/from database or table", () -> grantStmt2.analyze(analyzer));

        // 3. grant workload group prov to role on db.table
        tablePattern = new TablePattern("db1", "*");
        GrantStmt grantStmt3 = new GrantStmt(userIdentity, "test_role", tablePattern, usagePrivileges);
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "Can not grant/revoke USAGE_PRIV to/from database or table", () -> grantStmt3.analyze(analyzer));
    }

    private void createUser(UserIdentity userIdentity) throws UserException {
        UserDesc userDesc = new UserDesc(userIdentity, "12345", true);
        CreateUserStmt createUserStmt = new CreateUserStmt(false, userDesc, null);
        createUserStmt.analyze(analyzer);
        auth.createUser(createUserStmt);
    }

    private void grant(GrantStmt grantStmt) throws UserException {
        grantStmt.analyze(analyzer);
        auth.grant(grantStmt);
    }

    private void revoke(RevokeStmt revokeStmt) throws UserException {
        revokeStmt.analyze(analyzer);
        auth.revoke(revokeStmt);
    }

    @Test
    public void testShowViewPriv() throws UserException {
        UserIdentity userIdentity = new UserIdentity("viewUser", "%");
        createUser(userIdentity);
        // `load_priv` and `select_priv` can not `show create view`
        GrantStmt grantStmt = new GrantStmt(userIdentity, null, new TablePattern("viewdb", "*"),
                Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.SELECT_PRIV),
                        new AccessPrivilegeWithCols(AccessPrivilege.LOAD_PRIV)));
        grant(grantStmt);
        Assert.assertFalse(accessManager
                .checkDbPriv(userIdentity, "viewdb", PrivPredicate.SHOW_VIEW));

        // `SHOW_VIEW_PRIV` can `show create view`
        grantStmt = new GrantStmt(userIdentity, null, new TablePattern("viewdb", "*"),
                Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.SHOW_VIEW_PRIV)));
        grant(grantStmt);
        Assert.assertTrue(accessManager
                .checkDbPriv(userIdentity, "viewdb", PrivPredicate.SHOW_VIEW));

        RevokeStmt revokeStmt = new RevokeStmt(userIdentity, null, new TablePattern("viewdb", "*"),
                Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.SHOW_VIEW_PRIV)));
        revoke(revokeStmt);

        // 'admin_priv' can `show create view`
        grantStmt = new GrantStmt(userIdentity, null, new TablePattern("*", "*", "*"),
                Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.ADMIN_PRIV)));
        grant(grantStmt);
        Assert.assertTrue(accessManager
                .checkDbPriv(userIdentity, "viewdb", PrivPredicate.SHOW_VIEW));

        revokeStmt = new RevokeStmt(userIdentity, null, new TablePattern("*", "*", "*"),
                Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.ADMIN_PRIV)));
        revoke(revokeStmt);

        // 'create_priv' can `show create view`
        grantStmt = new GrantStmt(userIdentity, null, new TablePattern("viewdb", "*"),
                Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.CREATE_PRIV)));
        grant(grantStmt);
        Assert.assertTrue(accessManager
                .checkDbPriv(userIdentity, "viewdb", PrivPredicate.SHOW_VIEW));

        revokeStmt = new RevokeStmt(userIdentity, null, new TablePattern("viewdb", "*"),
                Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.CREATE_PRIV)));
        revoke(revokeStmt);

        // 'alter_priv' can `show create view`
        grantStmt = new GrantStmt(userIdentity, null, new TablePattern("viewdb", "*"),
                Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.ALTER_PRIV)));
        grant(grantStmt);
        Assert.assertTrue(accessManager
                .checkDbPriv(userIdentity, "viewdb", PrivPredicate.SHOW_VIEW));

        revokeStmt = new RevokeStmt(userIdentity, null, new TablePattern("viewdb", "*"),
                Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.ALTER_PRIV)));
        revoke(revokeStmt);

        // 'drop_priv' can `show create view`
        grantStmt = new GrantStmt(userIdentity, null, new TablePattern("viewdb", "*"),
                Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.DROP_PRIV)));
        grant(grantStmt);
        Assert.assertTrue(accessManager
                .checkDbPriv(userIdentity, "viewdb", PrivPredicate.SHOW_VIEW));

        revokeStmt = new RevokeStmt(userIdentity, null, new TablePattern("viewdb", "*"),
                Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.DROP_PRIV)));
        revoke(revokeStmt);
    }

    @Test
    public void testSetInitialRootPassword() {
        // Skip set root password if `initial_root_password` set to empty string
        auth.setInitialRootPassword("");
        Assert.assertTrue(
                auth.checkPlainPasswordForTest("root", "192.168.0.1", null, null));
        // Skip set root password if `initial_root_password` is not valid 2-staged SHA-1 encrypted
        auth.setInitialRootPassword("invalidRootPassword");
        Assert.assertTrue(
                auth.checkPlainPasswordForTest("root", "192.168.0.1", null, null));
        // Set initial root password
        byte[] scrambled = MysqlPassword.makeScrambledPassword("validRootPassword");
        auth.setInitialRootPassword(new String(scrambled));
        Assert.assertTrue(
                auth.checkPlainPasswordForTest("root", "192.168.0.1", "validRootPassword", null));
    }
}
