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

package org.apache.doris.authentication;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.arrowflight.auth2.FlightAuthUtils;
import org.apache.doris.authentication.handler.AuthenticationPluginManager;
import org.apache.doris.authentication.spi.AuthenticationPlugin;
import org.apache.doris.authentication.spi.AuthenticationPluginFactory;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.MysqlPassword;
import org.apache.doris.mysql.authenticate.AuthenticateRequest;
import org.apache.doris.mysql.authenticate.AuthenticateResponse;
import org.apache.doris.mysql.authenticate.integration.AuthenticationIntegrationAuthenticator;
import org.apache.doris.mysql.authenticate.ldap.LdapAuthenticator;
import org.apache.doris.mysql.authenticate.ldap.LdapManager;
import org.apache.doris.mysql.authenticate.password.ClearPassword;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.User;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.CreateUserCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowCreateUserCommand;
import org.apache.doris.persist.AlterUserOperationLog;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.PrivInfo;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SqlModeHelper;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;

class UserAuthenticationBindingTest {
    private Env env;
    private Auth auth;
    private EditLog editLog;
    private AuthenticationIntegrationMgr integrations;
    private AuthenticationIntegrationRuntime runtime;
    private ConnectContext context;
    private MockedStatic<Env> envStatic;
    private MockedStatic<MetricRepo> metrics;

    @BeforeEach
    void setUp() throws Exception {
        env = Mockito.mock(Env.class);
        envStatic = Mockito.mockStatic(Env.class);
        envStatic.when(Env::getCurrentEnv).thenReturn(env);
        metrics = Mockito.mockStatic(MetricRepo.class);
        editLog = Mockito.mock(EditLog.class);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        AccessControllerManager access = Mockito.mock(AccessControllerManager.class);
        Mockito.when(env.getAccessManager()).thenReturn(access);
        Mockito.when(access.checkGlobalPriv(Mockito.any(ConnectContext.class), Mockito.eq(PrivPredicate.GRANT)))
                .thenReturn(true);
        Mockito.when(access.checkGlobalPriv(Mockito.any(UserIdentity.class), Mockito.eq(PrivPredicate.GRANT)))
                .thenReturn(true);
        auth = new Auth();
        Mockito.when(env.getAuth()).thenReturn(auth);
        Mockito.when(access.getAuth()).thenReturn(auth);
        integrations = new AuthenticationIntegrationMgr();
        Mockito.when(env.getAuthenticationIntegrationMgr()).thenReturn(integrations);
        Mockito.when(env.getRoleMappingMgr()).thenReturn(new RoleMappingMgr());
        AuthenticationPluginManager plugins = new AuthenticationPluginManager();
        plugins.registerFactory(new BindingFactory());
        runtime = new AuthenticationIntegrationRuntime(plugins);
        Mockito.when(env.getAuthenticationIntegrationRuntime()).thenReturn(runtime);
        integrations.createAuthenticationIntegration("bound", false,
                Collections.singletonMap("type", "binding_test"), "", "root");
        integrations.createAuthenticationIntegration("required", false,
                Collections.singletonMap("type", "binding_test"), "", "root");
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
    }

    @Test
    void testSyntaxAndNonReservedNames() throws Exception {
        CreateUserCommand create = (CreateUserCommand) new NereidsParser().parseSingle(
                "CREATE USER IF NOT EXISTS 'etl'@'10.%' IDENTIFIED WITH AUTHENTICATION INTEGRATION bound "
                        + "AS ' External ' DEFAULT ROLE 'etl_role' COMMENT 'nightly etl'");
        Assertions.assertEquals("bound", create.getInfo().getAuthIntegration());
        Assertions.assertEquals(" External ", create.getInfo().getAuthIdentity());
        Assertions.assertEquals("10.%", create.getInfo().getUserIdent().getHost());
        Assertions.assertEquals("etl_role", create.getInfo().getRole());
        Assertions.assertDoesNotThrow(() -> new NereidsParser().parseSingle(
                "SELECT authentication AS integration FROM authentication"));
        Assertions.assertThrows(ParseException.class, () -> new NereidsParser().parseSingle(
                "CREATE USER 'etl' IDENTIFIED WITH mysql_native_password AS '*hash'"));
    }

    @Test
    void testCreateAndUserOutputs() throws Exception {
        execute("CREATE USER 'alice' IDENTIFIED WITH AUTHENTICATION INTEGRATION bound AS ' External '");
        User user = user("alice");
        Assertions.assertEquals("external", user.getAuthIdentity());
        Assertions.assertEquals(41, user.getPassword().getPassword().length);
        Assertions.assertFalse(auth.getPasswdPolicyManager().getPolicyInfo(user.getUserIdentity())
                .get(8).get(1).equals("true"));
        String create = new ShowCreateUserCommand(user.getUserIdentity()).doRun(context, null)
                .getResultRows().get(0).get(1);
        Assertions.assertTrue(create.contains("IDENTIFIED WITH AUTHENTICATION INTEGRATION `bound` AS \"external\""));
        Assertions.assertFalse(create.contains("PASSWORD_HISTORY"));
        Assertions.assertEquals("No", auth.getAuthInfo(user.getUserIdentity()).get(0).get(2));
        Assertions.assertEquals("bound", auth.getAuthInfo(user.getUserIdentity()).get(0).get(4));
        java.util.List<String> mysqlRow = auth.getAllUserInfo(UserIdentity.ROOT).stream()
                .filter(row -> row.get(1).equals("alice")).findFirst().orElseThrow(AssertionError::new);
        Assertions.assertEquals("bound", mysqlRow.get(22));
        Assertions.assertEquals("external", mysqlRow.get(23));
    }

    @Test
    void testExclusiveInstanceAndExactExternalPrincipal() throws Exception {
        execute("CREATE USER 'alice' IDENTIFIED WITH AUTHENTICATION INTEGRATION bound AS 'external'");
        User binding = auth.getAuthenticationBinding("alice", "10.1.2.3");
        AuthenticationIntegrationAuthenticator authenticator = new AuthenticationIntegrationAuthenticator(binding);
        Assertions.assertTrue(authenticator.prepareBoundAuthentication());
        AuthenticateResponse accepted = authenticator.authenticate(request("alice", "external"));
        Assertions.assertTrue(accepted.isSuccess());
        Assertions.assertFalse(accepted.isTemp());
        Assertions.assertEquals("bound", accepted.getAuthenticationIntegration());
        Assertions.assertFalse(authenticator.authenticate(request("alice", "other")).isSuccess());
        Assertions.assertFalse(authenticator.authenticate(request("alice", "EXTERNAL")).isSuccess());
        Assertions.assertFalse(new AuthenticationIntegrationAuthenticator("required", "authentication_chain")
                .authenticate(request("alice", "external")).isSuccess());
        Assertions.assertThrows(org.apache.doris.common.AuthenticationException.class,
                () -> auth.checkPlainPassword("alice", "10.1.2.3", "external", null));
        Assertions.assertThrows(org.apache.doris.common.AuthenticationException.class,
                () -> auth.checkPassword("alice", "10.1.2.3", new byte[0], new byte[20], null));
        Assertions.assertThrows(DdlException.class,
                () -> auth.setPassword(user("alice").getUserIdentity(), MysqlPassword.makeScrambledPassword("new")));
        Assertions.assertThrows(FlightRuntimeException.class, () -> FlightAuthUtils.authenticateCredentials(
                "alice", "external", "10.1.2.3", LogManager.getLogger(getClass())));
    }

    @Test
    void testRebindUnbindAndManualLock() throws Exception {
        execute("CREATE USER 'alice' IDENTIFIED BY 'old' PASSWORD_HISTORY 3 ACCOUNT_LOCK");
        execute("ALTER USER 'alice' IDENTIFIED WITH AUTHENTICATION INTEGRATION bound AS ' External '");
        Assertions.assertEquals(1, context.getState().getWarningRows());
        execute("ALTER USER 'alice' COMMENT 'bound account'");
        Assertions.assertEquals("bound account", user("alice").getComment());
        Assertions.assertThrows(org.apache.doris.common.AuthenticationException.class,
                () -> auth.checkAccountLocked(user("alice").getUserIdentity()));
        execute("ALTER USER 'alice' ACCOUNT_UNLOCK");
        Assertions.assertDoesNotThrow(() -> auth.checkAccountLocked(user("alice").getUserIdentity()));
        execute("ALTER USER 'alice' IDENTIFIED WITH AUTHENTICATION INTEGRATION bound");
        Assertions.assertNull(user("alice").getAuthIdentity());
        execute("ALTER USER 'alice' COMMENT 'same binding'");
        Assertions.assertEquals("bound", user("alice").getAuthIntegration());
        execute("ALTER USER 'alice' IDENTIFIED BY 'new'");
        Assertions.assertFalse(user("alice").isAuthenticationBound());
        Assertions.assertTrue(auth.checkPlainPasswordForTest("alice", "10.1.2.3", "new", null));
    }

    @Test
    void testDdlConstraintsAndDropReferences() throws Exception {
        Assertions.assertThrows(DdlException.class,
                () -> execute("CREATE USER 'alice' IDENTIFIED WITH AUTHENTICATION INTEGRATION missing"));
        Assertions.assertThrows(DdlException.class,
                () -> execute("CREATE USER 'alice' IDENTIFIED WITH AUTHENTICATION INTEGRATION required"));
        Assertions.assertThrows(DdlException.class,
                () -> execute("ALTER USER 'admin' IDENTIFIED WITH AUTHENTICATION INTEGRATION bound"));
        Assertions.assertThrows(DdlException.class,
                () -> execute("ALTER USER 'root' IDENTIFIED WITH AUTHENTICATION INTEGRATION bound"));
        execute("CREATE USER 'alice' IDENTIFIED WITH AUTHENTICATION INTEGRATION bound");
        for (String policy : Arrays.asList("PASSWORD_EXPIRE NEVER", "PASSWORD_HISTORY 0",
                "FAILED_LOGIN_ATTEMPTS 0", "PASSWORD_LOCK_TIME 0 SECOND")) {
            Assertions.assertThrows(Exception.class, () -> execute("ALTER USER 'alice' " + policy));
        }
        DdlException drop = Assertions.assertThrows(DdlException.class,
                () -> integrations.dropAuthenticationIntegration("bound", false));
        Assertions.assertTrue(drop.getMessage().contains("'alice'@'%'"));
        execute("ALTER USER 'alice' IDENTIFIED BY 'local'");
        Assertions.assertDoesNotThrow(() -> integrations.dropAuthenticationIntegration("bound", false));
    }

    @Test
    void testFirstHostMatchAndDomainResolution() throws Exception {
        execute("CREATE USER 'alice'@'%' IDENTIFIED BY 'local'");
        execute("CREATE USER 'alice'@'10.%' IDENTIFIED WITH AUTHENTICATION INTEGRATION bound");
        Assertions.assertFalse(auth.checkPlainPasswordForTest("alice", "10.1.2.3", "local", null));
        Assertions.assertTrue(auth.checkPlainPasswordForTest("alice", "192.168.1.2", "local", null));
        execute("CREATE USER 'domain_user'@('example.test') IDENTIFIED BY 'local'");
        auth.getUserManager().addUserPrivEntriesByResolvedIPs(Collections.singletonMap("example.test",
                Collections.singleton("10.1.2.3")));
        execute("ALTER USER 'domain_user'@('example.test') "
                + "IDENTIFIED WITH AUTHENTICATION INTEGRATION bound");
        User domainBinding = auth.getAuthenticationBinding("domain_user", "10.1.2.3");
        Assertions.assertNotNull(domainBinding);
        AuthenticationIntegrationAuthenticator domainAuthenticator =
                new AuthenticationIntegrationAuthenticator(domainBinding);
        AuthenticateResponse domainResponse = domainAuthenticator.authenticate(request("domain_user", "external")
                .withUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("domain_user", "10.1.2.3")));
        Assertions.assertTrue(domainResponse.isSuccess());
        Assertions.assertTrue(domainResponse.getUserIdentity().isDomain());
        Assertions.assertFalse(auth.checkPlainPasswordForTest("domain_user", "10.1.2.3", "local", null));
        Auth checkpoint = loadImage(auth, false);
        Assertions.assertNotNull(checkpoint.getAuthenticationBinding("domain_user", "10.1.2.3"));
        Auth downgraded = loadImage(auth, true);
        Assertions.assertDoesNotThrow(() -> downgraded.checkAccountLocked(
                UserIdentity.createAnalyzedUserIdentWithIp("domain_user", "10.1.2.3")));
    }

    @Test
    void testShowCreateRoundTripsIdentityLockAndTlsRequirements() throws Exception {
        execute("CREATE USER 'alice' IDENTIFIED WITH AUTHENTICATION INTEGRATION bound "
                + "AS 'o''brien\\\\id' ACCOUNT_LOCK REQUIRE SAN 'DNS:bound.example'");
        String sql = new ShowCreateUserCommand(user("alice").getUserIdentity()).doRun(context, null)
                .getResultRows().get(0).get(1);
        CreateUserCommand restored = (CreateUserCommand) new NereidsParser().parseSingle(sql);
        Assertions.assertEquals(user("alice").getAuthIdentity(), restored.getInfo().getAuthIdentity());
        Assertions.assertTrue(sql.indexOf("ACCOUNT_LOCK") < sql.indexOf("REQUIRE SAN"));
        Assertions.assertDoesNotThrow(() -> restored.getInfo().validate());
    }

    @Test
    void testDomainBindingAndTlsAlterRefreshExistingAliases() throws Exception {
        execute("CREATE USER 'domain_user'@('example.test') IDENTIFIED BY 'old'");
        auth.getUserManager().addUserPrivEntriesByResolvedIPs(Collections.singletonMap("example.test",
                Collections.singleton("10.1.2.3")));
        execute("ALTER USER 'domain_user'@('example.test') IDENTIFIED WITH AUTHENTICATION INTEGRATION bound");
        execute("ALTER USER 'domain_user'@('example.test') REQUIRE SAN 'DNS:bound.example'");
        UserIdentity ip = UserIdentity.createAnalyzedUserIdentWithIp("domain_user", "10.1.2.3");
        User alias = auth.getUserManager().getUserByUserIdentity(ip);
        Assertions.assertEquals("DNS:bound.example", alias.getUserIdentity().getSanRequirementSql());
        Assertions.assertEquals("DNS:bound.example", alias.getDomainUserIdentity().getSanRequirementSql());
        execute("ALTER USER 'domain_user'@('example.test') REQUIRE SAN 'DNS:changed.example'");
        Assertions.assertEquals("DNS:changed.example", alias.getUserIdentity().getSanRequirementSql());
        Assertions.assertEquals("bound", auth.getAuthenticationBinding("domain_user", "10.1.2.3")
                .getAuthIntegration());
    }

    @Test
    void testShowCreateRoundTripsIdentityInBothSqlModes() throws Exception {
        execute("CREATE USER 'alice' IDENTIFIED WITH AUTHENTICATION INTEGRATION bound "
                + "AS 'o''brien\\\\id'");
        String identity = user("alice").getAuthIdentity();
        for (long mode : new long[] {0, SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES}) {
            context.getSessionVariable().setSqlMode(mode);
            String sql = new ShowCreateUserCommand(user("alice").getUserIdentity()).doRun(context, null)
                    .getResultRows().get(0).get(1);
            execute("DROP USER 'alice'");
            execute(sql);
            Assertions.assertEquals(identity, user("alice").getAuthIdentity());
            Assertions.assertTrue(new AuthenticationIntegrationAuthenticator(
                    auth.getAuthenticationBinding("alice", "10.1.2.3"))
                    .authenticate(request("alice", identity)).isSuccess());
        }
    }

    @Test
    void testShowCreateSnapshotSurvivesRebind() throws Exception {
        assertShowCreateSnapshotSurvivesAlter("ALTER USER 'alice' IDENTIFIED WITH AUTHENTICATION "
                + "INTEGRATION required AS 'replacement'");
        execute("ALTER USER 'alice' ACCOUNT_UNLOCK");
        execute("ALTER USER 'alice' REQUIRE SAN 'DNS:after.example'");
        execute("ALTER USER 'alice' COMMENT 'after'");
        Assertions.assertEquals("required", user("alice").getAuthIntegration());
        Assertions.assertEquals("replacement", user("alice").getAuthIdentity());
        Assertions.assertEquals("after", user("alice").getComment());
        Assertions.assertDoesNotThrow(() -> auth.checkAccountLocked(user("alice").getUserIdentity()));
    }

    @Test
    void testShowCreateSnapshotSurvivesUnbind() throws Exception {
        assertShowCreateSnapshotSurvivesAlter("ALTER USER 'alice' IDENTIFIED BY 'local'");
        Assertions.assertFalse(user("alice").isAuthenticationBound());
    }

    @Test
    void testShowCreateLocalAccountUsesStoredTlsAfterPasswordOptions() throws Exception {
        execute("CREATE USER 'alice' IDENTIFIED BY 'local' PASSWORD_HISTORY 3 ACCOUNT_LOCK "
                + "REQUIRE SAN 'DNS:local.example'");
        UserIdentity lookup = UserIdentity.createAnalyzedUserIdentWithIp("alice", "%");
        String sql = new ShowCreateUserCommand(lookup).doRun(context, null).getResultRows().get(0).get(1);
        Assertions.assertTrue(sql.contains("PASSWORD_HISTORY 3"));
        Assertions.assertTrue(sql.contains("REQUIRE SAN 'DNS:local.example'"));
        Assertions.assertTrue(sql.indexOf("ACCOUNT_LOCK") < sql.indexOf("REQUIRE SAN"));
    }

    private void assertShowCreateSnapshotSurvivesAlter(String alter) throws Exception {
        execute("CREATE USER 'alice' IDENTIFIED WITH AUTHENTICATION INTEGRATION bound AS 'original' "
                + "ACCOUNT_LOCK REQUIRE SAN 'DNS:before.example' COMMENT 'before'");
        UserIdentity identity = user("alice").getUserIdentity();
        Auth observed = Mockito.spy(auth);
        Mockito.when(env.getAuth()).thenReturn(observed);
        // Execute the actual DDL in the gap between snapshot acquisition and rendering.
        // No timing assumptions or thread-local mocks on another thread are needed.
        Mockito.doAnswer(invocation -> {
            Object snapshot = invocation.callRealMethod();
            execute(alter);
            return snapshot;
        }).when(observed).getUserForShowCreate(identity);
        String sql = new ShowCreateUserCommand(identity).doRun(context, null).getResultRows().get(0).get(1);
        Assertions.assertEquals("CREATE USER 'alice'@'%' IDENTIFIED WITH AUTHENTICATION INTEGRATION `bound` "
                + "AS \"original\" ACCOUNT_LOCK REQUIRE SAN 'DNS:before.example' COMMENT \"before\"", sql);
    }

    @Test
    void testLocalAlterKeepsLegacySingleOperationRule() throws Exception {
        execute("CREATE USER 'alice' IDENTIFIED BY 'old'");
        Mockito.clearInvocations(editLog);
        for (String option : Arrays.asList("ACCOUNT_LOCK", "REQUIRE SAN 'DNS:bound.example'",
                "PASSWORD_HISTORY 3", "COMMENT 'changed'")) {
            Assertions.assertThrows(org.apache.doris.common.AnalysisException.class,
                    () -> execute("ALTER USER 'alice' IDENTIFIED BY 'new' " + option));
        }
        Mockito.verifyNoInteractions(editLog);
        Assertions.assertTrue(auth.checkPlainPasswordForTest("alice", "10.1.2.3", "old", null));
    }

    @Test
    void testBindingMetadataAlsoReplaysOnLegacyFes() throws Exception {
        execute("CREATE USER 'alice' IDENTIFIED BY 'old'");
        execute("ALTER USER 'alice' IDENTIFIED WITH AUTHENTICATION INTEGRATION bound AS 'external'");
        execute("ALTER USER 'alice' REQUIRE SAN 'DNS:bound.example'");
        execute("ALTER USER 'alice' COMMENT 'replayed'");
        InOrder journalOrder = Mockito.inOrder(editLog);
        journalOrder.verify(editLog).logCreateUser(Mockito.any());
        journalOrder.verify(editLog).logSetPassword(Mockito.any());
        journalOrder.verify(editLog, Mockito.times(2)).logAlterUser(Mockito.any());
        journalOrder.verifyNoMoreInteractions();
        Auth legacy = replayLegacyJournal();
        UserIdentity identity = UserIdentity.createAnalyzedUserIdentWithIp("alice", "%");
        User restored = legacy.getUserManager().getUserByUserIdentity(identity);
        Assertions.assertEquals("DNS:bound.example", restored.getUserIdentity().getSanRequirementSql());
        Assertions.assertEquals("replayed", restored.getComment());
        Assertions.assertDoesNotThrow(() -> legacy.checkAccountLocked(identity));
        Mockito.when(env.getAuth()).thenReturn(legacy);
        Assertions.assertFalse(legacy.checkPlainPasswordForTest("alice", "10.1.2.3", "old", null));
        Mockito.when(env.getAuth()).thenReturn(auth);
        execute("ALTER USER 'alice' IDENTIFIED BY 'new'");
        Auth unbound = replayLegacyJournal();
        Mockito.when(env.getAuth()).thenReturn(unbound);
        Assertions.assertTrue(unbound.checkPlainPasswordForTest("alice", "10.1.2.3", "new", null));
    }

    @Test
    void testLegacyDomainAliasesRefuseBindingUntilExplicitlyRecreated() throws Exception {
        execute("CREATE USER 'domain_user'@('example.test') IDENTIFIED BY 'old'");
        execute("CREATE USER 'domain_user'@'%' IDENTIFIED BY 'independent'");
        auth.getUserManager().addUserPrivEntriesByResolvedIPs(Collections.singletonMap("example.test",
                Collections.singleton("10.1.2.3")));
        auth = loadImage(auth, true);
        Mockito.when(env.getAuth()).thenReturn(auth);
        String bind = "ALTER USER 'domain_user'@('example.test') "
                + "IDENTIFIED WITH AUTHENTICATION INTEGRATION bound";
        DdlException error = Assertions.assertThrows(DdlException.class, () -> execute(bind));
        Assertions.assertTrue(error.getMessage().contains("'domain_user'@'10.1.2.3'"));
        Assertions.assertNull(auth.getUserManager().getUserByUserIdentity(
                UserIdentity.createAnalyzedUserIdentWithDomain("domain_user", "example.test")).getAuthIntegration());
        // A current-version checkpoint must not erase the unresolved legacy ambiguity.
        auth = loadImage(auth, false);
        Mockito.when(env.getAuth()).thenReturn(auth);
        Assertions.assertThrows(DdlException.class, () -> execute(bind));
        execute("DROP USER 'domain_user'@'10.1.2.3'");
        execute("CREATE USER 'domain_user'@'10.1.2.3' IDENTIFIED BY 'explicit'");
        execute(bind);
        Assertions.assertTrue(auth.checkPlainPasswordForTest("domain_user", "10.1.2.3", "explicit", null));
        Assertions.assertTrue(auth.checkPlainPasswordForTest("domain_user", "192.168.1.2", "independent", null));
    }

    @Test
    void testExplicitIpOverrideStartsIndependentPasswordPolicy() throws Exception {
        for (String lock : Arrays.asList("ACCOUNT_LOCK", "ACCOUNT_UNLOCK")) {
            execute("CREATE USER 'domain_user'@('example.test') IDENTIFIED WITH "
                    + "AUTHENTICATION INTEGRATION bound " + lock);
            auth.getUserManager().addUserPrivEntriesByResolvedIPs(Collections.singletonMap("example.test",
                    Collections.singleton("10.1.2.3")));
            execute("CREATE USER 'domain_user'@'10.1.2.3' IDENTIFIED BY 'local' ACCOUNT_UNLOCK");
            UserIdentity identity = UserIdentity.createAnalyzedUserIdentWithIp("domain_user", "10.1.2.3");
            User explicit = auth.getUserManager().getUserByUserIdentity(identity);
            Assertions.assertFalse(explicit.isSetByDomainResolver());
            Assertions.assertTrue(explicit.hasKnownDomainResolverOrigin());
            Assertions.assertNull(explicit.getAuthIntegration());
            Assertions.assertTrue(auth.checkPlainPasswordForTest("domain_user", "10.1.2.3", "local", null));
            Auth legacy = loadImage(auth, true);
            Assertions.assertDoesNotThrow(() -> legacy.checkAccountLocked(identity));
            Auth replay = replayLegacyJournal();
            Assertions.assertDoesNotThrow(() -> replay.checkAccountLocked(identity));
            execute("DROP USER 'domain_user'@'10.1.2.3'");
            execute("DROP USER 'domain_user'@('example.test')");
            Mockito.clearInvocations(editLog);
        }
    }

    @Test
    void testJournalReplayWithoutPluginsAndRandomPasswordRollback() throws Exception {
        execute("CREATE USER 'alice' IDENTIFIED WITH AUTHENTICATION INTEGRATION bound AS 'external'");
        ArgumentCaptor<PrivInfo> creates = ArgumentCaptor.forClass(PrivInfo.class);
        Mockito.verify(editLog).logCreateUser(creates.capture());
        execute("ALTER USER 'alice' IDENTIFIED WITH AUTHENTICATION INTEGRATION bound AS 'second'");
        ArgumentCaptor<PrivInfo> passwords = ArgumentCaptor.forClass(PrivInfo.class);
        Mockito.verify(editLog).logSetPassword(passwords.capture());
        execute("ALTER USER 'alice' COMMENT 'replayed'");
        ArgumentCaptor<AlterUserOperationLog> alters = ArgumentCaptor.forClass(AlterUserOperationLog.class);
        Mockito.verify(editLog).logAlterUser(alters.capture());
        AuthenticationIntegrationRuntime unloaded = Mockito.mock(AuthenticationIntegrationRuntime.class);
        Mockito.when(env.getAuthenticationIntegrationRuntime()).thenReturn(unloaded);
        Auth replay = new Auth();
        Mockito.when(env.getAuth()).thenReturn(replay);
        replay.replayCreateUser(creates.getValue());
        replay.replaySetPassword(passwords.getValue());
        replay.replayAlterUser(alters.getValue());
        UserIdentity identity = UserIdentity.createAnalyzedUserIdentWithIp("alice", "%");
        User restored = replay.getUserManager().getUserByUserIdentity(identity);
        Assertions.assertEquals("second", restored.getAuthIdentity());
        Assertions.assertEquals("replayed", restored.getComment());
        Assertions.assertDoesNotThrow(() -> replay.checkAccountLocked(identity));
        Mockito.verifyNoInteractions(unloaded);
        Auth checkpoint = loadImage(replay, false);
        Assertions.assertEquals("second", checkpoint.getAuthenticationBinding("alice", "10.1.2.3").getAuthIdentity());
        Auth downgraded = loadImage(replay, true);
        Mockito.when(env.getAuth()).thenReturn(downgraded);
        Assertions.assertNull(downgraded.getAuthenticationBinding("alice", "10.1.2.3"));
        Assertions.assertDoesNotThrow(() -> downgraded.checkAccountLocked(identity));
        Assertions.assertArrayEquals(restored.getPassword().getPassword(), downgraded.getUserManager()
                .getUserByUserIdentity(identity).getPassword().getPassword());
        Assertions.assertFalse(downgraded.checkPlainPasswordForTest("alice", "10.1.2.3", "external", null));
    }

    @Test
    void testLegacyJournalPreservesManualLocksAndLocalLoginAfterUnbind() throws Exception {
        execute("CREATE USER 'alice' IDENTIFIED BY 'old'");
        execute("ALTER USER 'alice' IDENTIFIED WITH AUTHENTICATION INTEGRATION bound");
        execute("ALTER USER 'alice' ACCOUNT_LOCK");
        Auth locked = replayLegacyJournal();
        Assertions.assertThrows(org.apache.doris.common.AuthenticationException.class,
                () -> locked.checkAccountLocked(user("alice").getUserIdentity()));
        execute("ALTER USER 'alice' ACCOUNT_UNLOCK");
        Auth legacy = replayLegacyJournal();
        Assertions.assertDoesNotThrow(() -> legacy.checkAccountLocked(user("alice").getUserIdentity()));
        Assertions.assertArrayEquals(user("alice").getPassword().getPassword(), legacy.getUserManager()
                .getUserByUserIdentity(user("alice").getUserIdentity()).getPassword().getPassword());
        Mockito.when(env.getAuth()).thenReturn(legacy);
        Assertions.assertFalse(legacy.checkPlainPasswordForTest("alice", "10.1.2.3", "old", null));
        Mockito.when(env.getAuth()).thenReturn(auth);
        execute("ALTER USER 'alice' IDENTIFIED BY 'new'");
        Auth unbound = replayLegacyJournal();
        Mockito.when(env.getAuth()).thenReturn(unbound);
        Assertions.assertTrue(unbound.checkPlainPasswordForTest("alice", "10.1.2.3", "new", null));
    }

    @Test
    void testBindingAlterKeepsLegacySingleOperationRule() throws Exception {
        execute("CREATE USER 'alice' IDENTIFIED BY 'old'");
        Mockito.clearInvocations(editLog);
        for (String option : Arrays.asList("ACCOUNT_LOCK", "REQUIRE SAN 'DNS:bound.example'", "COMMENT 'changed'")) {
            Assertions.assertThrows(org.apache.doris.common.AnalysisException.class, () -> execute(
                    "ALTER USER 'alice' IDENTIFIED WITH AUTHENTICATION INTEGRATION bound " + option));
        }
        Mockito.verifyNoInteractions(editLog);
        Assertions.assertNull(user("alice").getAuthIntegration());
        Assertions.assertTrue(auth.checkPlainPasswordForTest("alice", "10.1.2.3", "old", null));
    }

    @Test
    void testJitForDifferentHostKeepsExistingSemantics() throws Exception {
        execute("CREATE USER 'alice'@'10.%' IDENTIFIED WITH AUTHENTICATION INTEGRATION bound");
        java.util.Map<String, String> properties = new java.util.HashMap<>();
        properties.put("type", "binding_test");
        properties.put("enable_jit_user", "true");
        integrations.createAuthenticationIntegration("jit", false, properties, "", "root");
        AuthenticationIntegrationAuthenticator authenticator =
                new AuthenticationIntegrationAuthenticator("jit", "authentication_chain");
        AuthenticateResponse temporary = authenticator.authenticate(
                new AuthenticateRequest("alice", new ClearPassword("external"), "192.168.1.2"));
        Assertions.assertTrue(temporary.isSuccess());
        Assertions.assertTrue(temporary.isTemp());
        Assertions.assertEquals(UserIdentity.createAnalyzedUserIdentWithIp("alice", "192.168.1.2"),
                temporary.getUserIdentity());
        Assertions.assertFalse(authenticator.authenticate(request("alice", "external")).isSuccess());
    }

    @Test
    void testLegacyLdapTemporaryUserForDifferentHostKeepsExistingSemantics() throws Exception {
        execute("CREATE USER 'alice'@'10.%' IDENTIFIED WITH AUTHENTICATION INTEGRATION bound");
        LdapManager ldap = Mockito.mock(LdapManager.class);
        Mockito.when(ldap.checkUserPasswd("alice", "external")).thenReturn(true);
        Auth observed = Mockito.spy(auth);
        Mockito.doReturn(ldap).when(observed).getLdapManager();
        Mockito.when(env.getAuth()).thenReturn(observed);
        AuthenticateResponse response = new LdapAuthenticator().authenticate(
                new AuthenticateRequest("alice", new ClearPassword("external"), "192.168.1.2"));
        Assertions.assertTrue(response.isSuccess());
        Assertions.assertTrue(response.isTemp());
        Assertions.assertEquals(UserIdentity.createAnalyzedUserIdentWithIp("alice", "192.168.1.2"),
                response.getUserIdentity());
        Assertions.assertNotNull(observed.getAuthenticationBinding("alice", "10.1.2.3"));
    }

    private Auth replayLegacyJournal() {
        Auth target = new Auth();
        Mockito.mockingDetails(editLog).getInvocations().forEach(invocation -> {
            String method = invocation.getMethod().getName();
            if (Arrays.asList("logCreateUser", "logAlterUser", "logSetPassword").contains(method)) {
                JsonElement record = GsonUtils.GSON.toJsonTree(invocation.getArgument(0));
                removeBindingFields(record);
                if (method.equals("logCreateUser")) {
                    target.replayCreateUser(GsonUtils.GSON.fromJson(record, PrivInfo.class));
                } else if (method.equals("logAlterUser")) {
                    target.replayAlterUser(GsonUtils.GSON.fromJson(record, AlterUserOperationLog.class));
                } else {
                    target.replaySetPassword(GsonUtils.GSON.fromJson(record, PrivInfo.class));
                }
            }
        });
        return target;
    }

    private Auth loadImage(Auth original, boolean legacy) throws Exception {
        ByteArrayOutputStream image = new ByteArrayOutputStream();
        original.write(new DataOutputStream(image));
        if (legacy) {
            ByteArrayOutputStream rewritten = new ByteArrayOutputStream();
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(image.toByteArray()));
            DataOutputStream output = new DataOutputStream(rewritten);
            while (input.available() > 0) {
                JsonElement component = GsonUtils.GSON.fromJson(Text.readString(input), JsonElement.class);
                removeBindingFields(component);
                Text.writeString(output, GsonUtils.GSON.toJson(component));
            }
            image = rewritten;
        }
        Auth loaded = new Auth();
        loaded.readFields(new DataInputStream(new ByteArrayInputStream(image.toByteArray())));
        return loaded;
    }

    private void execute(String sql) throws Exception {
        context.getState().reset();
        Command command = (Command) new NereidsParser().parseSingle(sql);
        command.run(context, null);
    }

    private User user(String name) {
        return auth.getUserManager().getUserByUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp(name, "%"));
    }

    private AuthenticateRequest request(String name, String external) {
        return new AuthenticateRequest(name, new ClearPassword(external), "10.1.2.3");
    }

    private void removeBindingFields(JsonElement element) {
        if (element.isJsonObject()) {
            JsonObject object = element.getAsJsonObject();
            for (String field : Arrays.asList("authIntegration", "authIdentity", "domainUserIdentity", "isSetByDomainResolver")) {
                object.remove(field);
            }
            object.entrySet().forEach(entry -> removeBindingFields(entry.getValue()));
        } else if (element.isJsonArray()) {
            element.getAsJsonArray().forEach(this::removeBindingFields);
        }
    }

    private static class BindingFactory implements AuthenticationPluginFactory {
        @Override
        public String name() {
            return "binding_test";
        }

        @Override
        public AuthenticationPlugin create() {
            return new AuthenticationPlugin() {
                private boolean required;

                @Override
                public String name() {
                    return "binding_test";
                }

                @Override
                public void initialize(AuthenticationIntegration integration) {
                    required = "required".equals(integration.getName());
                }

                @Override
                public boolean supports(AuthenticationRequest request) {
                    return CredentialType.CLEAR_TEXT_PASSWORD.equals(request.getCredentialType());
                }

                @Override
                public boolean requiresClearPassword() {
                    return true;
                }

                @Override
                public boolean requiresBoundIdentity() {
                    return required;
                }

                @Override
                public String normalizeBoundIdentity(String identity) {
                    return identity == null ? null : identity.trim().toLowerCase(Locale.ROOT);
                }

                @Override
                public AuthenticationResult authenticate(AuthenticationRequest request,
                        AuthenticationIntegration integration) {
                    return AuthenticationResult.success(BasicPrincipal.builder().name(request.getUsername())
                            .authenticator(name()).externalPrincipal(
                                    new String(request.getCredential(), StandardCharsets.UTF_8)).build());
                }
            };
        }
    }
}
