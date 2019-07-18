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
import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.analysis.CreateRoleStmt;
import org.apache.doris.analysis.CreateUserStmt;
import org.apache.doris.analysis.DropRoleStmt;
import org.apache.doris.analysis.GrantStmt;
import org.apache.doris.analysis.SetPassVar;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AccessPrivilege;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.MysqlPassword;
import org.apache.doris.mysql.privilege.PaloAuth.PrivLevel;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.PrivInfo;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import mockit.Deencapsulation;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;

public class PrivTest {

    private PaloAuth auth;
    private byte[] passwd = new byte[] { 'a', 'c' };

    private Method grantGlobalPrivsM;
    private Method grantDbPrivsM;
    private Method grantTblPrivsM;
    private Method checkHasPrivM;

    @Before
    public void setUp() {
        auth = new PaloAuth();
       
        Method[] methods = PaloAuth.class.getDeclaredMethods();
        for (Method method : methods) {
            if (method.getName().equals("grantGlobalPrivs")) {
                method.setAccessible(true);
                grantGlobalPrivsM = method;
            } else if (method.getName().equals("grantDbPrivs")) {
                method.setAccessible(true);
                grantDbPrivsM = method;
            } else if (method.getName().equals("grantTblPrivs")) {
                method.setAccessible(true);
                grantTblPrivsM = method;
            } else if (method.getName().equals("checkHasPrivInternal")) {
                method.setAccessible(true);
                checkHasPrivM = method;
            }
        }

        new MockUp<Catalog>() {
            @Mock
            public int getCurrentCatalogJournalVersion() {
                return FeMetaVersion.VERSION_43;
            }
        };
    }

    public void grantGlobalPrivs(Object... params)
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        grantGlobalPrivsM.invoke(auth, params);
    }

    public void grantDbPrivs(Object... params)
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        grantDbPrivsM.invoke(auth, params);
    }

    public void grantTblPrivs(Object... params)
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        grantTblPrivsM.invoke(auth, params);
    }

    public boolean checkHasPriv(Object... params)
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        return (Boolean) checkHasPrivM.invoke(auth, params);
    }

    @Test
    public void testContainsPriv() {
        List<PaloPrivilege> privs = Lists.newArrayList(PaloPrivilege.GRANT_PRIV, PaloPrivilege.NODE_PRIV);
        Assert.assertFalse(privs.contains(PaloPrivilege.CREATE_PRIV));
        Assert.assertTrue(privs.contains(PaloPrivilege.NODE_PRIV));
    }

    @Test
    public void testGlobalPriv()
            throws DdlException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        // exact match
        grantGlobalPrivs("192.168.1.1", "cmy", passwd, false, false, false,
                         PrivBitSet.of(PaloPrivilege.GRANT_PRIV));
        Assert.assertFalse(auth.checkGlobalPriv("192.168.1.2", "cmy",
                                                PrivPredicate.of(PrivBitSet.of(PaloPrivilege.GRANT_PRIV),
                                                                 Operator.OR)));
        Assert.assertFalse(auth.checkGlobalPriv("192.168.1.1", "cmy2",
                                                PrivPredicate.of(PrivBitSet.of(PaloPrivilege.GRANT_PRIV),
                                                                 Operator.OR)));
        Assert.assertFalse(auth.checkGlobalPriv("192.168.1.1", "cmy",
                                                PrivPredicate.of(PrivBitSet.of(PaloPrivilege.NODE_PRIV),
                                                                 Operator.OR)));
        Assert.assertTrue(auth.checkGlobalPriv("192.168.1.1", "cmy",
                                               PrivPredicate.of(PrivBitSet.of(PaloPrivilege.GRANT_PRIV),
                                                                Operator.OR)));
        // fuzzy match
        auth.clear();
        grantGlobalPrivs("192.168._.%", "cmy\\_%", passwd, false, false, false,
                         PrivBitSet.of(PaloPrivilege.GRANT_PRIV,
                                                PaloPrivilege.NODE_PRIV));
        Assert.assertFalse(auth.checkGlobalPriv("192.168.1.1", "cmy",
                                                PrivPredicate.of(PrivBitSet.of(PaloPrivilege.GRANT_PRIV),
                                                                 Operator.OR)));
        Assert.assertFalse(auth.checkGlobalPriv("192.168.1.200", "cmy_123",
                                                PrivPredicate.of(PrivBitSet.of(PaloPrivilege.SELECT_PRIV),
                                                                 Operator.OR)));
        Assert.assertTrue(auth.checkGlobalPriv("192.168.1.200", "cmy_123",
                                               PrivPredicate.of(PrivBitSet.of(PaloPrivilege.GRANT_PRIV,
                                                                                       PaloPrivilege.NODE_PRIV),
                                                                Operator.OR)));
        Assert.assertTrue(auth.checkGlobalPriv("192.168.1.200", "cmy_",
                                               PrivPredicate.of(PrivBitSet.of(PaloPrivilege.NODE_PRIV),
                                                                Operator.OR)));

        auth.clear();
        grantGlobalPrivs("192.168.%", ".cmy", passwd, false, false, false,
                         PrivBitSet.of(PaloPrivilege.GRANT_PRIV));
        Assert.assertFalse(auth.checkGlobalPriv("192.10.1.1", ".cmy",
                                                PrivPredicate.of(PrivBitSet.of(PaloPrivilege.GRANT_PRIV),
                                                                 Operator.OR)));
        Assert.assertTrue(auth.checkGlobalPriv("192.168.1.200", ".cmy",
                                               PrivPredicate.of(PrivBitSet.of(PaloPrivilege.GRANT_PRIV),
                                                                Operator.OR)));

        // multi priv entries
        auth.clear();
        grantGlobalPrivs("%", "cmy", passwd, false, false, false, PrivBitSet.of(PaloPrivilege.GRANT_PRIV));
        grantGlobalPrivs("localhost", "cmy", passwd, false, false, false,
                         PrivBitSet.of(PaloPrivilege.NODE_PRIV));
        grantGlobalPrivs("127.0.0.1", "cmy", passwd, false, false, false,
                         PrivBitSet.of(PaloPrivilege.SELECT_PRIV));

        Assert.assertTrue(auth.checkGlobalPriv("127.0.0.1", "cmy",
                                               PrivPredicate.of(PrivBitSet.of(PaloPrivilege.SELECT_PRIV),
                                                                Operator.OR)));
        Assert.assertFalse(auth.checkGlobalPriv("127.0.0.1", "cmy",
                                                PrivPredicate.of(PrivBitSet.of(PaloPrivilege.GRANT_PRIV),
                                                                 Operator.OR)));
        Assert.assertTrue(auth.checkGlobalPriv("localhost", "cmy",
                                               PrivPredicate.of(PrivBitSet.of(PaloPrivilege.NODE_PRIV),
                                                                Operator.OR)));
        Assert.assertFalse(auth.checkGlobalPriv("localhost", "cmy",
                                                PrivPredicate.of(PrivBitSet.of(PaloPrivilege.GRANT_PRIV),
                                                                 Operator.OR)));
        Assert.assertTrue(auth.checkGlobalPriv("192.168.1.1", "cmy",
                                               PrivPredicate.of(PrivBitSet.of(PaloPrivilege.GRANT_PRIV),
                                                                Operator.OR)));

        // test persist
        auth = testPersist(auth);
        Assert.assertTrue(auth.checkGlobalPriv("127.0.0.1", "cmy",
                                               PrivPredicate.of(PrivBitSet.of(PaloPrivilege.SELECT_PRIV),
                                                                Operator.OR)));
        Assert.assertFalse(auth.checkGlobalPriv("127.0.0.1", "cmy",
                                                PrivPredicate.of(PrivBitSet.of(PaloPrivilege.GRANT_PRIV),
                                                                 Operator.OR)));
        Assert.assertTrue(auth.checkGlobalPriv("localhost", "cmy",
                                               PrivPredicate.of(PrivBitSet.of(PaloPrivilege.NODE_PRIV),
                                                                Operator.OR)));
        Assert.assertFalse(auth.checkGlobalPriv("localhost", "cmy",
                                                PrivPredicate.of(PrivBitSet.of(PaloPrivilege.GRANT_PRIV),
                                                                 Operator.OR)));
        Assert.assertTrue(auth.checkGlobalPriv("192.168.1.1", "cmy",
                                               PrivPredicate.of(PrivBitSet.of(PaloPrivilege.GRANT_PRIV),
                                                                Operator.OR)));
    }

    @Test
    public void testDbPriv()
            throws DdlException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        // normal
        grantDbPrivs("192.168.1.%", "my\\_database", "cmy", false, false, false,
                     PrivBitSet.of(PaloPrivilege.SELECT_PRIV,
                                            PaloPrivilege.ALTER_PRIV));
        Assert.assertTrue(auth.checkDbPriv("192.168.1.1", "my_database", "cmy",
                                           PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV),
                                                            Operator.OR)));
        Assert.assertTrue(auth.checkDbPriv("192.168.1.1", "my_database", "cmy",
                                           PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV,
                                                                                   PaloPrivilege.SELECT_PRIV),
                                                            Operator.OR)));
        Assert.assertFalse(auth.checkDbPriv("192.168.1.2", "my_database", "cmy",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV,
                                                                                    PaloPrivilege.LOAD_PRIV),
                                                             Operator.AND)));
        Assert.assertTrue(auth.checkDbPriv("192.168.1.2", "my_database", "cmy",
                                           PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV,
                                                                                   PaloPrivilege.LOAD_PRIV),
                                                            Operator.OR)));
        Assert.assertFalse(auth.checkDbPriv("192.168.1.1", "my_database2", "cmy",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV),
                                                             Operator.OR)));
        Assert.assertFalse(auth.checkDbPriv("192.168.2.1", "my_database2", "cmy",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV),
                                                             Operator.OR)));
        Assert.assertFalse(auth.checkDbPriv("192.168.1.1", "my_database2", "cmy2",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV),
                                                             Operator.OR)));

        // add global priv
        auth.clear();
        grantGlobalPrivs("%", "cmy", passwd, false, false, false, PrivBitSet.of(PaloPrivilege.SELECT_PRIV));
        grantDbPrivs("192.168.1.%", "database", "cmy", false, false, false,
                     PrivBitSet.of(PaloPrivilege.ALTER_PRIV));

        Assert.assertTrue(auth.checkDbPriv("192.168.1.1", "database", "cmy",
                                           PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV,
                                                                                   PaloPrivilege.SELECT_PRIV),
                                                            Operator.OR)));

        Assert.assertTrue(auth.checkDbPriv("192.168.1.1", "database2", "cmy",
                                           PrivPredicate.of(PrivBitSet.of(PaloPrivilege.SELECT_PRIV),
                                                            Operator.OR)));

        Assert.assertFalse(auth.checkDbPriv("192.168.2.1", "database", "cmy",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV),
                                                             Operator.OR)));

        Assert.assertFalse(auth.checkDbPriv("192.168.1.1", "database2", "cmy",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV),
                                                             Operator.OR)));

        // test persist
        auth = testPersist(auth);
        Assert.assertTrue(auth.checkDbPriv("192.168.1.1", "database", "cmy",
                                           PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV,
                                                                                   PaloPrivilege.SELECT_PRIV),
                                                            Operator.OR)));

        Assert.assertTrue(auth.checkDbPriv("192.168.1.1", "database2", "cmy",
                                           PrivPredicate.of(PrivBitSet.of(PaloPrivilege.SELECT_PRIV),
                                                            Operator.OR)));

        Assert.assertFalse(auth.checkDbPriv("192.168.2.1", "database", "cmy",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV),
                                                             Operator.OR)));

        Assert.assertFalse(auth.checkDbPriv("192.168.1.1", "database2", "cmy",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV),
                                                             Operator.OR)));
    }

    @Test
    public void testTblPriv()
            throws DdlException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        // normal
        grantTblPrivs("192.%.1.1", "db\\_%", "cmy%", "tbl%", false, false, false,
                      PrivBitSet.of(PaloPrivilege.SELECT_PRIV,
                                             PaloPrivilege.LOAD_PRIV));
        Assert.assertTrue(auth.checkTblPriv("192.168.1.1", "db_1", "cmy", "tbl",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.LOAD_PRIV,
                                                                                    PaloPrivilege.SELECT_PRIV),
                                                             Operator.OR)));
        Assert.assertFalse(auth.checkTblPriv("192.168.1.1", "db_1", "cmy", "tbl",
                                             PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV),
                                                              Operator.OR)));

        // add db priv
        grantDbPrivs("192.%", "db\\_123", "cmy", false, false, false, PrivBitSet.of(PaloPrivilege.ALTER_PRIV));
        Assert.assertTrue(auth.checkTblPriv("192.168.1.1", "db_123", "cmy", "tbl",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV),
                                                             Operator.OR)));

        Assert.assertTrue(auth.checkTblPriv("192.168.1.1", "db_123", "cmy", "tbl",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.SELECT_PRIV),
                                                             Operator.OR)));

        Assert.assertFalse(auth.checkTblPriv("10.168.1.1", "db_123", "cmy", "tbl",
                                             PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ALTER_PRIV),
                                                              Operator.OR)));

        // add global priv
        grantGlobalPrivs("192.168.2.1", "cmy\\_admin", passwd, false, false, false,
                         PrivBitSet.of(PaloPrivilege.DROP_PRIV));
        Assert.assertTrue(auth.checkTblPriv("192.168.2.1", "db_123", "cmy_admin", "tbl",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.DROP_PRIV),
                                                             Operator.OR)));

        Assert.assertFalse(auth.checkTblPriv("192.168.1.1", "db_123", "cmy_admin", "tbl",
                                             PrivPredicate.of(PrivBitSet.of(PaloPrivilege.DROP_PRIV),
                                                              Operator.OR)));

        // test persist
        auth = testPersist(auth);
        Assert.assertTrue(auth.checkTblPriv("192.168.2.1", "db_123", "cmy_admin", "tbl",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.DROP_PRIV),
                                                             Operator.OR)));

        Assert.assertFalse(auth.checkTblPriv("192.168.1.1", "db_123", "cmy_admin", "tbl",
                                             PrivPredicate.of(PrivBitSet.of(PaloPrivilege.DROP_PRIV),
                                                              Operator.OR)));

        // add global priv
        grantGlobalPrivs("%", "cmy2", passwd, false, false, false,
                         PrivBitSet.of(PaloPrivilege.DROP_PRIV));
        Assert.assertTrue(auth.checkTblPriv("", "db_123", "cmy2", "tbl",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.DROP_PRIV),
                                                             Operator.OR)));
        Assert.assertTrue(auth.checkTblPriv(null, "db_123", "cmy2", "tbl",
                                            PrivPredicate.of(PrivBitSet.of(PaloPrivilege.DROP_PRIV),
                                                             Operator.OR)));

    }

    @Test
    public void testGrantToUser(@Injectable Analyzer analyzer, @Injectable EditLog editlog)
            throws UserException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        // 1. no privs
        Assert.assertFalse(checkHasPriv("192.168.1.1", "cmy1", PrivPredicate.GRANT, new PrivLevel[] { PrivLevel.GLOBAL }));
        Assert.assertFalse(checkHasPriv("192.168.1.1", "cmy1", PrivPredicate.GRANT, new PrivLevel[] { PrivLevel.DATABASE }));
        Assert.assertFalse(checkHasPriv("192.168.1.1", "cmy1", PrivPredicate.GRANT, new PrivLevel[] { PrivLevel.GLOBAL, PrivLevel.DATABASE }));
        // 2. grant GRANT priv and check again
        grantGlobalPrivs("192.168.1.1", "cmy1", passwd, false, false, false, PrivBitSet.of(PaloPrivilege.GRANT_PRIV));
        Assert.assertTrue(checkHasPriv("192.168.1.1", "cmy1", PrivPredicate.GRANT, new PrivLevel[] { PrivLevel.GLOBAL }));
        Assert.assertFalse(checkHasPriv("192.168.1.1", "cmy1", PrivPredicate.GRANT, new PrivLevel[] { PrivLevel.DATABASE }));
        Assert.assertTrue(checkHasPriv("192.168.1.1", "cmy1", PrivPredicate.GRANT, new PrivLevel[] { PrivLevel.GLOBAL, PrivLevel.DATABASE }));
        // 3. revoke the priv and check again
        UserIdentity userIdent = new UserIdentity("cmy1", "192.168.1.1");
        userIdent.setIsAnalyzed();
        auth.revokePrivs(userIdent, TablePattern.ALL, PrivBitSet.of(PaloPrivilege.GRANT_PRIV), false, false, false);
        Assert.assertFalse(checkHasPriv("192.168.1.1", "cmy1", PrivPredicate.GRANT, new PrivLevel[] { PrivLevel.GLOBAL }));
        Assert.assertFalse(checkHasPriv("192.168.1.1", "cmy1", PrivPredicate.GRANT, new PrivLevel[] { PrivLevel.DATABASE }));
        Assert.assertFalse(checkHasPriv("192.168.1.1", "cmy1", PrivPredicate.GRANT, new PrivLevel[] { PrivLevel.GLOBAL, PrivLevel.DATABASE }));

        Catalog catalog = Deencapsulation.newInstance(Catalog.class);
        new Expectations(catalog) {
            {
                Catalog.getCurrentCatalog();
                result = catalog;

                catalog.getEditLog();
                result = editlog;

                catalog.getAuth();
                result = auth;

                editlog.logCreateUser((PrivInfo) any);
                minTimes = 0;
            }
        };

        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default_cluster";
            }
        };

        // 1. create user cmy1@%, cmy2@% and cmy3@%
        ConnectContext ctx = new ConnectContext(null);
        ctx.setRemoteIP("192.168.1.1");
        ctx.setQualifiedUser("root");
        ctx.setThreadLocalInfo();

        CreateUserStmt createUserStmt = new CreateUserStmt(new UserDesc(new UserIdentity("cmy1", "%"), "123", true));
        createUserStmt.analyze(analyzer);
        auth.createUser(createUserStmt);
        createUserStmt = new CreateUserStmt(new UserDesc(new UserIdentity("cmy2", "%"), "123", true));
        createUserStmt.analyze(analyzer);
        auth.createUser(createUserStmt);
        createUserStmt = new CreateUserStmt(new UserDesc(new UserIdentity("cmy3", "%"), "123", true));
        createUserStmt.analyze(analyzer);
        auth.createUser(createUserStmt);
        
        // 2. grant GRANT on *.* to cmy1, grant GRANT on db1.* to cmy2, grant GRANT on db1.tbl1 to cmy3, 
        GrantStmt grantStmt = new GrantStmt(new UserIdentity("cmy1", "%"), null, TablePattern.ALL, Lists.newArrayList(AccessPrivilege.GRANT_PRIV));
        grantStmt.analyze(analyzer);
        auth.grant(grantStmt);
        grantStmt = new GrantStmt(new UserIdentity("cmy2", "%"), null, new TablePattern("db1", "*"), Lists.newArrayList(AccessPrivilege.GRANT_PRIV));
        grantStmt.analyze(analyzer);
        auth.grant(grantStmt);
        grantStmt = new GrantStmt(new UserIdentity("cmy3", "%"), null, new TablePattern("db1", "tbl1"), Lists.newArrayList(AccessPrivilege.GRANT_PRIV));
        grantStmt.analyze(analyzer);
        auth.grant(grantStmt);

        // 3. use cmy2 to grant CREATE priv on db1 and db2 to cmy4 (cmy4 does not exist, but we just test GrantStmt in analyze phase, so it is ok)
        ctx = new ConnectContext(null);
        ctx.setRemoteIP("192.168.1.1");
        ctx.setQualifiedUser("default_cluster:cmy2");
        ctx.setThreadLocalInfo();
        // db1 should be ok
        grantStmt = new GrantStmt(new UserIdentity("cmy4", "%"), null, new TablePattern("db1", "*"),
                Lists.newArrayList(AccessPrivilege.CREATE_PRIV));
        try {
            grantStmt.analyze(analyzer);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        // db2 should not be allowed
        boolean hasException = false;
        grantStmt = new GrantStmt(new UserIdentity("cmy4", "%"), null, new TablePattern("db2", "*"),
                Lists.newArrayList(AccessPrivilege.CREATE_PRIV));
        try {
            grantStmt.analyze(analyzer);
        } catch (Exception e) {
            hasException = true;
        }
        Assert.assertTrue(hasException);
        
        // 4. use cmy2 to create cmy4
        createUserStmt = new CreateUserStmt(new UserDesc(new UserIdentity("cmy4", "%"), "123", true));
        try {
            createUserStmt.analyze(analyzer);
            auth.createUser(createUserStmt);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        // 5. use cmy3 to grant CREATE priv on db1.* and db1.tbl1 to cmy5 (cmy5 does not exist, but we just test GrantStmt in analyze phase, so it is ok)
        ctx = new ConnectContext(null);
        ctx.setRemoteIP("192.168.1.1");
        ctx.setQualifiedUser("default_cluster:cmy3");
        ctx.setThreadLocalInfo();
        // db1.* should not be allowed
        hasException = false;
        grantStmt = new GrantStmt(new UserIdentity("cmy5", "%"), null, new TablePattern("db1", "*"),
                Lists.newArrayList(AccessPrivilege.CREATE_PRIV));
        try {
            grantStmt.analyze(analyzer);
        } catch (Exception e) {
            hasException = true;
        }
        Assert.assertTrue(hasException);
        // db1.tbl1 should be ok
        grantStmt = new GrantStmt(new UserIdentity("cmy5", "%"), null, new TablePattern("db1", "tbl1"),
                Lists.newArrayList(AccessPrivilege.CREATE_PRIV));
        try {
            grantStmt.analyze(analyzer);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testGrantToRole(@Injectable Analyzer analyzer, @Injectable EditLog editlog) throws UserException {
        Catalog catalog = Deencapsulation.newInstance(Catalog.class);
        new Expectations(catalog) {
            {
                Catalog.getCurrentCatalog();
                result = catalog;

                catalog.getEditLog();
                result = editlog;

                catalog.getAuth();
                result = auth;

                editlog.logCreateUser((PrivInfo) any);
                minTimes = 0;
            }
        };

        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default_cluster";
            }
        };

        // 1. create user cmy1@%, cmy2@%
        ConnectContext ctx = new ConnectContext(null);
        ctx.setRemoteIP("192.168.1.1");
        ctx.setQualifiedUser("root");
        ctx.setThreadLocalInfo();

        CreateUserStmt createUserStmt = new CreateUserStmt(new UserDesc(new UserIdentity("cmy1", "%"), "123", true));
        createUserStmt.analyze(analyzer);
        auth.createUser(createUserStmt);
        createUserStmt = new CreateUserStmt(new UserDesc(new UserIdentity("cmy2", "%"), "123", true));
        createUserStmt.analyze(analyzer);
        auth.createUser(createUserStmt);

        // 2. grant GRANT on *.* to cmy1, grant GRANT on db1.* to cmy2
        GrantStmt grantStmt = new GrantStmt(new UserIdentity("cmy1", "%"), null, TablePattern.ALL,
                Lists.newArrayList(AccessPrivilege.GRANT_PRIV));
        grantStmt.analyze(analyzer);
        auth.grant(grantStmt);
        grantStmt = new GrantStmt(new UserIdentity("cmy2", "%"), null, new TablePattern("db1", "*"),
                Lists.newArrayList(AccessPrivilege.GRANT_PRIV));
        grantStmt.analyze(analyzer);
        auth.grant(grantStmt);

        // 3. use cmy1 to create role1
        ctx = new ConnectContext(null);
        ctx.setRemoteIP("192.168.1.1");
        ctx.setQualifiedUser("default_cluster:cmy1");
        ctx.setThreadLocalInfo();
        CreateRoleStmt createRoleStmt = new CreateRoleStmt("role1");
        try {
            createRoleStmt.analyze(analyzer);
            auth.createRole(createRoleStmt);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        // 4. use cmy2 to create role2, which should not be allowed
        ctx = new ConnectContext(null);
        ctx.setRemoteIP("192.168.1.1");
        ctx.setQualifiedUser("default_cluster:cmy2");
        ctx.setThreadLocalInfo();
        boolean hasException = false;
        createRoleStmt = new CreateRoleStmt("role2");
        try {
            createRoleStmt.analyze(analyzer);
        } catch (Exception e) {
            hasException = true;
        }
        Assert.assertTrue(hasException);
        
        // 5. use cmy2 to grant CREATE priv on db1.* to role, which should not be
        // allowed
        hasException = false;
        grantStmt = new GrantStmt(null, "role1", new TablePattern("db1", "*"), Lists.newArrayList(AccessPrivilege.CREATE_PRIV));
        try {
            grantStmt.analyze(analyzer);
        } catch (Exception e) {
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 6. use cmy2 to drop role1, which should not be allowed
        hasException = false;
        DropRoleStmt dropRoleStmt = new DropRoleStmt("role1");
        try {
            dropRoleStmt.analyze(analyzer);
        } catch (Exception e) {
            hasException = true;
        }
        Assert.assertTrue(hasException);
    }

    @Test
    public void testSetPassword(@Injectable Analyzer analyzer, @Injectable EditLog editlog) throws UserException {
        Catalog catalog = Deencapsulation.newInstance(Catalog.class);
        new Expectations(catalog) {
            {
                Catalog.getCurrentCatalog();
                result = catalog;

                catalog.getEditLog();
                result = editlog;

                catalog.getAuth();
                result = auth;

                editlog.logCreateUser((PrivInfo) any);
                minTimes = 0;
            }
        };

        new Expectations() {
            {
                analyzer.getClusterName();
                result = "default_cluster";
            }
        };

        new Expectations(MysqlPassword.class) {
            {
                MysqlPassword.checkPassword(anyString);
                result = new byte[2];
                minTimes = 0;
            }
        };

        // 1. create user cmy1@%, cmy2@%
        ConnectContext ctx = new ConnectContext(null);
        ctx.setRemoteIP("192.168.1.1");
        ctx.setQualifiedUser("root");
        ctx.setThreadLocalInfo();

        CreateUserStmt createUserStmt = new CreateUserStmt(new UserDesc(new UserIdentity("cmy1", "%"), "123", true));
        createUserStmt.analyze(analyzer);
        auth.createUser(createUserStmt);
        createUserStmt = new CreateUserStmt(new UserDesc(new UserIdentity("cmy2", "%"), "123", true));
        createUserStmt.analyze(analyzer);
        auth.createUser(createUserStmt);

        // 2. use cmy1 to set password for itself
        UserIdentity currentUserIdentity = new UserIdentity("cmy1", "%");
        currentUserIdentity.analyze("default_cluster");
        ctx = new ConnectContext(null);
        ctx.setRemoteIP("192.168.1.1");
        ctx.setQualifiedUser("default_cluster:cmy1");
        ctx.setCurrentUserIdentitfy(currentUserIdentity);
        ctx.setThreadLocalInfo();
        SetPassVar setPassVar = new SetPassVar(null, "12345");
        try {
            setPassVar.analyze(analyzer);
        } catch (Exception e) {
            Assert.fail();
        }

        // 2. use cmy1 to set password for cmy1@"%"
        setPassVar = new SetPassVar(new UserIdentity("cmy1", "%"), "12345");
        try {
            setPassVar.analyze(analyzer);
        } catch (Exception e) {
            Assert.fail();
        }

        // 3. use cmy1 to set password for cmy1@"192.168.1.1", which is not allowed
        boolean hasException = false;
        setPassVar = new SetPassVar(new UserIdentity("cmy1", "192.168.1.1"), "12345");
        try {
            setPassVar.analyze(analyzer);
        } catch (Exception e) {
            hasException = true;
        }
        Assert.assertTrue(hasException);
    }

    private PaloAuth testPersist(PaloAuth auth) {
        // 1. Write objects to file
        File file = new File("./paloAuth");
        try {
            file.createNewFile();
            DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));

            auth.write(dos);

            dos.flush();
            dos.close();

            // 2. Read objects from file
            DataInputStream dis = new DataInputStream(new FileInputStream(file));

            PaloAuth replayed = PaloAuth.read(dis);
            
            System.out.println(replayed.toString());
            
            return replayed;

        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            file.delete();
        }
        return null;
    }

}
