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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.lock.MonitoredReentrantLock;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.job.extensions.insert.InsertTask;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.MysqlPassword;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.BootstrapPasswordGuard;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Frontend;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class BootstrapAuthenticationTest {
    private Auth auth;
    private Env env;
    private ConnectContext context;
    private MockedStatic<Env> envStatic;
    private MockedStatic<MetricRepo> metrics;
    private boolean previousAllowEmpty;
    private boolean previousRecovery;

    @BeforeEach
    void setUp() {
        previousAllowEmpty = Config.allow_empty_password_for_root_admin;
        previousRecovery = Config.skip_localhost_auth_check;
        Config.allow_empty_password_for_root_admin = false;
        Config.skip_localhost_auth_check = false;
        env = Mockito.mock(Env.class);
        envStatic = Mockito.mockStatic(Env.class);
        envStatic.when(Env::getCurrentEnv).thenReturn(env);
        metrics = Mockito.mockStatic(MetricRepo.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(env.getEditLog()).thenReturn(Mockito.mock(EditLog.class));
        AccessControllerManager access = Mockito.mock(AccessControllerManager.class);
        Mockito.when(env.getAccessManager()).thenReturn(access);
        Mockito.when(access.checkGlobalPriv(Mockito.any(ConnectContext.class), Mockito.eq(PrivPredicate.GRANT)))
                .thenReturn(true);
        auth = new Auth();
        Mockito.when(env.getAuth()).thenReturn(auth);
        context = new ConnectContext();
        context.setEnv(env);
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setThreadLocalInfo();
    }

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
        envStatic.close();
        metrics.close();
        Config.allow_empty_password_for_root_admin = previousAllowEmpty;
        Config.skip_localhost_auth_check = previousRecovery;
    }

    @Test
    void testLegacyAuthenticationWithoutImageOrBootstrapJournal() throws Exception {
        Assertions.assertFalse(auth.isSecureInitialization());
        for (String name : List.of("root", "admin")) {
            List<UserIdentity> identities = new ArrayList<>();
            auth.checkPlainPassword(name, "10.0.0.8", "", identities);
            Assertions.assertEquals(1, identities.size());
        }
    }

    @Test
    void testRootBootstrapTransportAndAddressMatrix() throws Exception {
        auth.setSecureInitialization();
        for (String host : List.of("127.0.0.1", "::1", "0:0:0:0:0:0:0:1")) {
            List<UserIdentity> identities = new ArrayList<>();
            Assertions.assertTrue(auth.checkPassword("root", host, new byte[0], new byte[20], identities,
                    AuthenticationProtocol.MYSQL));
            Assertions.assertEquals(List.of(UserIdentity.ROOT), identities);
        }
        for (String host : List.of("127.0.0.6", "10.0.0.8", "localhost")) {
            AuthenticationException error = Assertions.assertThrows(AuthenticationException.class,
                    () -> auth.checkPassword("root", host, new byte[0], new byte[20], new ArrayList<>(),
                            AuthenticationProtocol.MYSQL));
            Assertions.assertEquals(1045, error.errorCode.getCode());
        }
        Assertions.assertThrows(AuthenticationException.class,
                () -> auth.checkPassword("root", "127.0.0.1", new byte[0], new byte[20], new ArrayList<>(),
                        AuthenticationProtocol.PROXIED_MYSQL));
        Assertions.assertThrows(AuthenticationException.class,
                () -> auth.checkPlainPassword("root", "127.0.0.1", "", new ArrayList<>()));
        Assertions.assertThrows(AuthenticationException.class,
                () -> auth.checkBootstrapAuthentication(UserIdentity.ROOT, "127.0.0.1",
                        AuthenticationProtocol.OTHER));
    }

    @Test
    void testAdminNeedsPasswordForEveryClientTransport() throws Exception {
        auth.setSecureInitialization();
        for (AuthenticationProtocol protocol : AuthenticationProtocol.values()) {
            for (String host : List.of("127.0.0.1", "::1", "10.0.0.8")) {
                Assertions.assertThrows(AuthenticationException.class,
                        () -> auth.checkPassword("admin", host, new byte[0], new byte[20],
                                new ArrayList<>(), protocol));
            }
            Assertions.assertThrows(AuthenticationException.class,
                    () -> auth.checkBootstrapAuthentication(UserIdentity.ADMIN, "127.0.0.1", protocol));
        }
        auth.setPassword(UserIdentity.ADMIN, MysqlPassword.makeScrambledPassword("admin-password"));
        auth.checkPlainPassword("admin", "10.0.0.8", "admin-password", new ArrayList<>());
        auth.setPassword(UserIdentity.ADMIN, new byte[0]);
        Assertions.assertThrows(AuthenticationException.class,
                () -> auth.checkPlainPassword("admin", "10.0.0.8", "", new ArrayList<>()));
    }

    @Test
    void testTestModeAndRecoveryHaveDifferentScopes() throws Exception {
        auth.setSecureInitialization();
        Config.allow_empty_password_for_root_admin = true;
        auth.checkPlainPassword("admin", "10.0.0.8", "", new ArrayList<>());
        auth.setPassword(UserIdentity.ROOT, MysqlPassword.makeScrambledPassword("root-password"));
        Assertions.assertThrows(AuthenticationException.class,
                () -> auth.checkPassword("root", "127.0.0.1", new byte[0], new byte[20], new ArrayList<>(),
                        AuthenticationProtocol.MYSQL));
        Config.allow_empty_password_for_root_admin = false;
        Config.skip_localhost_auth_check = true;
        Assertions.assertFalse(auth.checkPassword("root", "127.0.0.1", new byte[0], new byte[20], new ArrayList<>(),
                AuthenticationProtocol.MYSQL));
        Assertions.assertThrows(AuthenticationException.class,
                () -> auth.checkPassword("root", "127.0.0.1", new byte[0], new byte[20], new ArrayList<>(),
                        AuthenticationProtocol.PROXIED_MYSQL));
        Assertions.assertThrows(AuthenticationException.class,
                () -> auth.checkPassword("root", "127.0.0.1", new byte[0], new byte[20], new ArrayList<>(),
                        AuthenticationProtocol.OTHER));
        Assertions.assertThrows(AuthenticationException.class,
                () -> auth.checkPlainPassword("admin", "10.0.0.8", "", new ArrayList<>()));
    }

    @Test
    void testEmptyCredentialErrorDoesNotRevealStoredPassword() throws Exception {
        auth.setSecureInitialization();
        AuthenticationException empty = Assertions.assertThrows(AuthenticationException.class,
                () -> auth.checkPlainPassword("root", "10.0.0.8", "", new ArrayList<>()));
        auth.setPassword(UserIdentity.ROOT, MysqlPassword.makeScrambledPassword("root-password"));
        AuthenticationException configured = Assertions.assertThrows(AuthenticationException.class,
                () -> auth.checkPlainPassword("root", "10.0.0.8", "", new ArrayList<>()));
        Assertions.assertEquals(empty.getMessage(), configured.getMessage());
        Assertions.assertEquals(1045, configured.errorCode.getCode());
    }

    @Test
    void testOrdinaryEmptyPasswordUserIsUnaffected() throws Exception {
        auth.setSecureInitialization();
        UserIdentity worker = UserIdentity.createAnalyzedUserIdentWithIp("bootstrap_worker", "%");
        auth.getUserManager().createUser(worker, new byte[0], null, false, "");
        List<UserIdentity> identities = new ArrayList<>();
        auth.checkPlainPassword("bootstrap_worker", "10.0.0.8", "", identities);
        Assertions.assertEquals(List.of(worker), identities);
    }

    @Test
    void testLegacyExplicitRecoveryKeepsItsOriginalAddressRules() throws Exception {
        auth.setPassword(UserIdentity.ROOT, MysqlPassword.makeScrambledPassword("root-password"));
        Config.skip_localhost_auth_check = true;
        auth.checkPassword("root", "127.0.0.1", new byte[0], new byte[20], new ArrayList<>());
        Assertions.assertThrows(AuthenticationException.class,
                () -> auth.checkPassword("root", "::1", new byte[0], new byte[20], new ArrayList<>()));
    }

    @Test
    void testBootstrapMetadataRoundTripAndMissingLegacyField() throws Exception {
        UserManager legacy = GsonUtils.GSON.fromJson("{\"nameToUsers\":{}}", UserManager.class);
        Assertions.assertFalse(legacy.isSecureInitialization());
        auth.setSecureInitialization();
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        auth.write(new DataOutputStream(bytes));
        Auth restored = new Auth();
        restored.readFields(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
        Assertions.assertTrue(restored.isSecureInitialization());
        Frontend first = new Frontend(FrontendNodeType.FOLLOWER, "first-fe", "127.0.0.1", 9010);
        first.setSecureInitialization();
        bytes.reset();
        first.write(new DataOutputStream(bytes));
        Frontend replayed = Frontend.read(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
        Assertions.assertTrue(replayed.isSecureInitialization());
    }

    @Test
    void testFirstFrontendReplayAppliesPolicyEvenForAnExistingFrontend() {
        Frontend first = new Frontend(FrontendNodeType.FOLLOWER, "first-fe", "127.0.0.1", 9010);
        first.setSecureInitialization();
        Mockito.when(env.checkFeExist("127.0.0.1", 9010)).thenReturn(first);
        Deencapsulation.setField(env, "lock", new MonitoredReentrantLock(true));
        Mockito.doCallRealMethod().when(env).replayAddFrontend(first);
        Assertions.assertFalse(auth.isSecureInitialization());
        env.replayAddFrontend(first);
        Assertions.assertTrue(auth.isSecureInitialization());
    }

    @Test
    void testOwnPasswordChangeClearsOnlyTheChangingClientRestriction() throws Exception {
        auth.setSecureInitialization();
        context.setPasswordResetRequired(true);
        byte[] password = MysqlPassword.makeScrambledPassword("root-password");
        auth.setPassword(UserIdentity.ROOT, password);
        auth.afterPasswordChange(context, UserIdentity.ROOT, password);
        Assertions.assertFalse(context.isPasswordResetRequired());
        context.setPasswordResetRequired(true);
        auth.afterPasswordChange(context, UserIdentity.ROOT, new byte[0]);
        Assertions.assertTrue(context.isPasswordResetRequired());
    }

    @Test
    void testStatementWhitelistRejectsSubqueriesFunctionsAndFileSinks() throws Exception {
        context.setPasswordResetRequired(true);
        NereidsParser parser = new NereidsParser();
        for (String sql : List.of("SELECT 1", "SELECT @@version_comment", "SELECT @@version_comment LIMIT 1",
                "SELECT @@session.autocommit",
                "SELECT current_user(), session_user(), user(), database(), connection_id()",
                "SELECT version()", "SHOW VARIABLES", "SET NAMES utf8", "SET autocommit = 1",
                "SET PASSWORD = PASSWORD('root-password')", "ALTER USER root IDENTIFIED BY 'root-password'")) {
            Assertions.assertDoesNotThrow(() -> BootstrapPasswordGuard.check(context, parser.parseSingle(sql)), sql);
        }
        for (String sql : List.of("SELECT (SELECT secret FROM t LIMIT 1)", "SELECT arbitrary_udf()",
                "SELECT 1 INTO OUTFILE 'file:///tmp/bootstrap-guard-test'", "SELECT * FROM t",
                "SET GLOBAL autocommit = 1", "SET PASSWORD = PASSWORD('')",
                "SET PASSWORD FOR 'admin'@'%' = PASSWORD('admin-password')")) {
            UserException error = Assertions.assertThrows(UserException.class,
                    () -> BootstrapPasswordGuard.check(context, parser.parseSingle(sql)), sql);
            Assertions.assertEquals(ErrorCode.ERR_MUST_CHANGE_PASSWORD, error.getMysqlErrorCode(), sql);
        }
        Assertions.assertFalse(BootstrapPasswordGuard.isAllowedCommand(MysqlCommand.COM_FIELD_LIST));
        Assertions.assertFalse(BootstrapPasswordGuard.isAllowedCommand(MysqlCommand.COM_INIT_DB));
    }

    @Test
    void testInternalJobsAndMtmvDoNotInheritClientRestrictions() throws Exception {
        auth.setSecureInitialization();
        Assertions.assertTrue(auth.hasEmptyPassword(UserIdentity.ADMIN));
        ConnectContext job = InsertTask.makeConnectContext(UserIdentity.ADMIN, "");
        Assertions.assertFalse(job.isPasswordResetRequired());
        BootstrapPasswordGuard.check(job, new NereidsParser().parseSingle("SELECT * FROM internal_job_table"));
        ConnectContext mv = MTMVPlanUtil.createBasicMvContext(null, List.of(), Map.of());
        Assertions.assertEquals(UserIdentity.ADMIN, mv.getCurrentUserIdentity());
        Assertions.assertFalse(mv.isPasswordResetRequired());
        BootstrapPasswordGuard.check(mv, new NereidsParser().parseSingle("SELECT * FROM internal_mtmv_table"));
    }

    @Test
    void testInitialPasswordMustBeHashedAndCannotAppearInError() throws Exception {
        String password = "must-not-appear-in-an-error";
        Exception error = Assertions.assertThrows(Exception.class, () -> Auth.parseInitialRootPassword(password));
        Assertions.assertFalse(error.getMessage().contains(password));
        Assertions.assertNull(error.getCause());
        Assertions.assertEquals(0, Auth.parseInitialRootPassword("").length);
        String hash = new String(MysqlPassword.makeScrambledPassword("root-password"),
                java.nio.charset.StandardCharsets.UTF_8);
        Assertions.assertArrayEquals(MysqlPassword.makeScrambledPassword("root-password"),
                Auth.parseInitialRootPassword(hash));
    }
}
