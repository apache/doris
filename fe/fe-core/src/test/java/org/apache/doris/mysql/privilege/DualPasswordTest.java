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

import org.apache.doris.alter.AlterUserOpType;
import org.apache.doris.analysis.PasswordOptions;
import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.UserDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.metric.LongCounterMetric;
import org.apache.doris.metric.Metric.MetricUnit;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.MysqlPassword;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.AlterUserCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateUserCommand;
import org.apache.doris.nereids.trees.plans.commands.SetOptionsCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateUserInfo;
import org.apache.doris.persist.AlterUserOperationLog;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.PrivInfo;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

/**
 * MySQL-compatible dual password:
 * ALTER USER ... IDENTIFIED BY ... RETAIN CURRENT PASSWORD keeps the previous
 * password valid (secondary slot) until the next password change without
 * RETAIN, or an explicit ALTER USER ... DISCARD OLD PASSWORD.
 */
public class DualPasswordTest {

    private Auth auth;
    private Env env = Mockito.mock(Env.class);
    private EditLog editLog = Mockito.mock(EditLog.class);
    private AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
    private InternalCatalog internalCatalog = Mockito.mock(InternalCatalog.class);
    private MockedStatic<Env> mockedEnvStatic;

    @BeforeEach
    public void setUp() throws Exception {
        auth = new Auth();
        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getAuth()).thenReturn(auth);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        // ConnectContext.setEnv reads the internal catalog name
        Mockito.when(internalCatalog.getName()).thenReturn("internal");
        Mockito.when(env.getInternalCatalog()).thenReturn(internalCatalog);
    }

    @AfterEach
    public void tearDown() {
        mockedEnvStatic.close();
        ConnectContext.remove();
    }

    /** A connected session for executing parsed commands. */
    private ConnectContext ctxFor(UserIdentity currentUser) {
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(env);
        ctx.setCurrentUserIdentity(currentUser);
        ctx.setThreadLocalInfo();
        return ctx;
    }

    private void grantPriv(boolean hasGrantPriv) {
        Mockito.when(accessManager.checkGlobalPriv(Mockito.any(ConnectContext.class),
                Mockito.eq(PrivPredicate.GRANT))).thenReturn(hasGrantPriv);
    }

    private UserIdentity createUser(String name) throws DdlException {
        UserIdentity userIdentity = new UserIdentity(name, "%");
        userIdentity.setIsAnalyzed();
        CreateUserCommand createUserCommand = new CreateUserCommand(new CreateUserInfo(new UserDesc(userIdentity)));
        auth.createUser(createUserCommand.getInfo());
        return userIdentity;
    }

    private boolean canLogin(String user, String plainPassword) {
        try {
            auth.checkPlainPassword(user, "192.168.1.1", plainPassword, null);
            return true;
        } catch (AuthenticationException e) {
            return false;
        }
    }

    @Test
    public void testRetainEvictAndDiscard() throws DdlException {
        UserIdentity user = createUser("rot");

        // initial password p1
        auth.setPassword(user, MysqlPassword.makeScrambledPassword("p1"));
        Assertions.assertTrue(canLogin("rot", "p1"));
        Assertions.assertFalse(canLogin("rot", "p2"));

        // p2 RETAIN CURRENT PASSWORD -> p1 and p2 both authenticate
        auth.setPasswordInternal(user, MysqlPassword.makeScrambledPassword("p2"), null,
                true, false, true /* retain */, false);
        Assertions.assertTrue(canLogin("rot", "p2"));
        Assertions.assertTrue(canLogin("rot", "p1"));
        Assertions.assertFalse(canLogin("rot", "p0"));

        // p3 RETAIN -> the one-secondary rule evicts p1; p2 + p3 authenticate
        auth.setPasswordInternal(user, MysqlPassword.makeScrambledPassword("p3"), null,
                true, false, true /* retain */, false);
        Assertions.assertTrue(canLogin("rot", "p3"));
        Assertions.assertTrue(canLogin("rot", "p2"));
        Assertions.assertFalse(canLogin("rot", "p1"));

        // p4 WITHOUT retain -> the secondary REMAINS UNCHANGED (MySQL: "the
        // secondary password remains unchanged"); the replaced primary p3 is
        // simply gone -> p4 + p2 authenticate, p3 does not
        auth.setPasswordInternal(user, MysqlPassword.makeScrambledPassword("p4"), null,
                true, false, false /* no retain */, false);
        Assertions.assertTrue(canLogin("rot", "p4"));
        Assertions.assertFalse(canLogin("rot", "p3"));
        Assertions.assertTrue(canLogin("rot", "p2"));

        // p5 RETAIN, then DISCARD OLD PASSWORD (via the replay path, which is
        // also what a follower executes) -> only p5 remains
        auth.setPasswordInternal(user, MysqlPassword.makeScrambledPassword("p5"), null,
                true, false, true /* retain */, false);
        Assertions.assertTrue(canLogin("rot", "p4"));
        auth.replayAlterUser(new AlterUserOperationLog(AlterUserOpType.DISCARD_OLD_PASSWORD,
                user, null, null, PasswordOptions.UNSET_OPTION, null));
        Assertions.assertTrue(canLogin("rot", "p5"));
        Assertions.assertFalse(canLogin("rot", "p4"));

        // DISCARD with no secondary present: silent no-op (MySQL: discards
        // the secondary password, "if one exists")
        auth.replayAlterUser(new AlterUserOperationLog(AlterUserOpType.DISCARD_OLD_PASSWORD,
                user, null, null, PasswordOptions.UNSET_OPTION, null));
        Assertions.assertTrue(canLogin("rot", "p5"));
    }

    @Test
    public void testRetainRequiresNonEmptyCurrentPassword() throws DdlException {
        UserIdentity user = createUser("empty_cur");
        // MySQL: "If you specify RETAIN CURRENT PASSWORD for an account that
        // has an empty primary password, the statement fails."
        Assertions.assertThrows(DdlException.class, () -> auth.setPasswordInternal(user,
                MysqlPassword.makeScrambledPassword("p1"), null, true, false, true /* retain */, false));
    }

    @Test
    public void testEmptyNewPasswordEmptiesSecondary() throws DdlException {
        UserIdentity user = createUser("empty_new");
        auth.setPassword(user, MysqlPassword.makeScrambledPassword("p1"));
        auth.setPasswordInternal(user, MysqlPassword.makeScrambledPassword("p2"), null,
                true, false, true /* retain */, false);
        Assertions.assertTrue(canLogin("empty_new", "p1"));

        // MySQL: "If the new password ... is empty, the secondary password
        // becomes empty as well, even if RETAIN CURRENT PASSWORD is given."
        auth.setPasswordInternal(user, new byte[0], null,
                true, false, true /* retain */, false);
        Assertions.assertTrue(canLogin("empty_new", ""));
        Assertions.assertFalse(canLogin("empty_new", "p1"));
        Assertions.assertFalse(canLogin("empty_new", "p2"));
    }

    @Test
    public void testRetainReplay() throws DdlException {
        UserIdentity user = createUser("replayer");
        auth.setPassword(user, MysqlPassword.makeScrambledPassword("p1"));

        // a follower replaying OP_SET_PASSWORD with retainPasswd=true must
        // reach the same dual-slot state as the master
        auth.replaySetPassword(new PrivInfo(user, null,
                MysqlPassword.makeScrambledPassword("p2"), null, null, true /* retain */));
        Assertions.assertTrue(canLogin("replayer", "p2"));
        Assertions.assertTrue(canLogin("replayer", "p1"));

        // and a replay without the flag (all journals written before this
        // feature, plus any plain password change) behaves like a plain
        // change: the primary is replaced, the secondary remains UNCHANGED
        // (MySQL: "the secondary password remains unchanged")
        auth.replaySetPassword(new PrivInfo(user, null,
                MysqlPassword.makeScrambledPassword("p3"), null, null));
        Assertions.assertTrue(canLogin("replayer", "p3"));
        Assertions.assertFalse(canLogin("replayer", "p2"));
        Assertions.assertTrue(canLogin("replayer", "p1"));
    }

    @Test
    public void testGsonCompat() {
        // round trip preserves the secondary slot
        Password password = new Password(MysqlPassword.makeScrambledPassword("p2"));
        password.setSecondaryPassword(MysqlPassword.makeScrambledPassword("p1"));
        Password reloaded = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(password), Password.class);
        Assertions.assertArrayEquals(password.getPassword(), reloaded.getPassword());
        Assertions.assertArrayEquals(password.getSecondaryPassword(), reloaded.getSecondaryPassword());
        Assertions.assertTrue(reloaded.hasSecondaryPassword());

        // an image/journal written BEFORE this feature deserializes with an
        // absent secondary slot -> unchanged single-password behavior
        Password legacy = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(new Password(MysqlPassword.makeScrambledPassword("p1"))), Password.class);
        Assertions.assertFalse(legacy.hasSecondaryPassword());
        Assertions.assertNull(legacy.getSecondaryPassword());
    }

    @Test
    public void testParser() {
        NereidsParser parser = new NereidsParser();

        AlterUserCommand retain = (AlterUserCommand) parser.parseSingle(
                "ALTER USER u1 IDENTIFIED BY 'x' RETAIN CURRENT PASSWORD");
        Assertions.assertTrue(retain.getAlterUserInfo().isRetainCurrentPassword());
        Assertions.assertFalse(retain.getAlterUserInfo().isDiscardOldPassword());

        AlterUserCommand discard = (AlterUserCommand) parser.parseSingle(
                "ALTER USER u1 DISCARD OLD PASSWORD");
        Assertions.assertTrue(discard.getAlterUserInfo().isDiscardOldPassword());
        Assertions.assertFalse(discard.getAlterUserInfo().isRetainCurrentPassword());

        AlterUserCommand plain = (AlterUserCommand) parser.parseSingle(
                "ALTER USER u1 IDENTIFIED BY 'x'");
        Assertions.assertFalse(plain.getAlterUserInfo().isRetainCurrentPassword());
        Assertions.assertFalse(plain.getAlterUserInfo().isDiscardOldPassword());

        // DISCARD and OLD are nonReserved: still valid as identifiers
        parser.parseSingle("SELECT old, discard FROM discard.old");
    }

    @Test
    public void testValidateRejectsRetainWithoutPasswordChange() {
        // the clause parses on its own; the semantic rejection (RETAIN
        // requires a password change) lives in AlterUserInfo.validate()
        NereidsParser parser = new NereidsParser();
        AlterUserCommand cmd = (AlterUserCommand) parser.parseSingle(
                "ALTER USER u1 RETAIN CURRENT PASSWORD");
        Assertions.assertTrue(cmd.getAlterUserInfo().isRetainCurrentPassword());
        Assertions.assertThrows(AnalysisException.class, () -> cmd.getAlterUserInfo().validate());
    }

    @Test
    public void testSetPasswordRetainOverload() throws DdlException {
        // SET PASSWORD ... RETAIN CURRENT PASSWORD routes through
        // Auth.setPassword(user, pw, retain=true) — the self-service path a
        // service account uses to rotate its own credential with an overlap.
        UserIdentity user = createUser("setpw");
        auth.setPassword(user, MysqlPassword.makeScrambledPassword("p1"));
        Assertions.assertTrue(canLogin("setpw", "p1"));

        auth.setPassword(user, MysqlPassword.makeScrambledPassword("p2"), true /* retain */);
        Assertions.assertTrue(canLogin("setpw", "p2"));         // new primary
        Assertions.assertTrue(canLogin("setpw", "p1"));         // retained secondary

        // a plain SET PASSWORD (retain=false) preserves the secondary (MySQL)
        auth.setPassword(user, MysqlPassword.makeScrambledPassword("p3"), false);
        Assertions.assertTrue(canLogin("setpw", "p3"));
        Assertions.assertTrue(canLogin("setpw", "p1"));         // still valid
        Assertions.assertFalse(canLogin("setpw", "p2"));        // replaced primary gone
    }

    @Test
    public void testSetPasswordRetainParses() {
        // The clause parses on SET PASSWORD; no exception, RETAIN accepted.
        NereidsParser parser = new NereidsParser();
        parser.parseSingle("SET PASSWORD = PASSWORD('x') RETAIN CURRENT PASSWORD");
        parser.parseSingle("SET PASSWORD FOR u1 = PASSWORD('x') RETAIN CURRENT PASSWORD");
        // still parses WITHOUT the clause (back-compat)
        parser.parseSingle("SET PASSWORD = PASSWORD('x')");
    }

    @Test
    public void testSetPasswordRetainRequiresGrantPriv() throws Exception {
        // RETAIN CURRENT PASSWORD is privileged EVEN on one's own account
        // (MySQL requires APPLICATION_PASSWORD_ADMIN for the clause):
        // otherwise anyone briefly holding a password could park their own in
        // the secondary slot as a persistent hidden credential.
        UserIdentity user = createUser("gated");
        auth.setPassword(user, MysqlPassword.makeScrambledPassword("p1"));
        ConnectContext ctx = ctxFor(user);

        NereidsParser parser = new NereidsParser();
        SetOptionsCommand cmd = (SetOptionsCommand) parser.parseSingle(
                "SET PASSWORD = PASSWORD('p2') RETAIN CURRENT PASSWORD");
        // a password change must always forward to master
        Assertions.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, cmd.toRedirectStatus());

        grantPriv(false);
        Assertions.assertThrows(AnalysisException.class, () -> cmd.run(ctx, null));
        // nothing changed: p1 still the only valid password
        Assertions.assertTrue(canLogin("gated", "p1"));
        Assertions.assertFalse(canLogin("gated", "p2"));

        grantPriv(true);
        cmd.run(ctx, null);
        Assertions.assertTrue(canLogin("gated", "p2"));
        Assertions.assertTrue(canLogin("gated", "p1"));

        // the journaled entry carries the retain flag
        ArgumentCaptor<PrivInfo> captor = ArgumentCaptor.forClass(PrivInfo.class);
        Mockito.verify(editLog, Mockito.atLeastOnce()).logSetPassword(captor.capture());
        PrivInfo journaled = captor.getValue();
        Assertions.assertTrue(journaled.isRetainPasswd());
        // DISCARD never rides OP_SET_PASSWORD
        Mockito.verify(editLog, Mockito.never()).logAlterUser(Mockito.any(AlterUserOperationLog.class));
    }

    @Test
    public void testPlainSelfSetPasswordStaysUnprivileged() throws Exception {
        // a plain self-service SET PASSWORD (no RETAIN) still requires no
        // privilege — unchanged behavior
        UserIdentity user = createUser("selfplain");
        auth.setPassword(user, MysqlPassword.makeScrambledPassword("p1"));
        ConnectContext ctx = ctxFor(user);
        grantPriv(false);

        NereidsParser parser = new NereidsParser();
        SetOptionsCommand cmd = (SetOptionsCommand) parser.parseSingle("SET PASSWORD = PASSWORD('p2')");
        cmd.run(ctx, null);
        Assertions.assertTrue(canLogin("selfplain", "p2"));
        Assertions.assertFalse(canLogin("selfplain", "p1"));
    }

    @Test
    public void testAlterUserRetainExecution() throws Exception {
        // execute the PARSED ALTER USER command end to end (validate + run),
        // not just the parser flags
        UserIdentity admin = createUser("adm");
        UserIdentity user = createUser("target");
        auth.setPassword(user, MysqlPassword.makeScrambledPassword("p1"));
        ConnectContext ctx = ctxFor(admin);

        NereidsParser parser = new NereidsParser();
        AlterUserCommand cmd = (AlterUserCommand) parser.parseSingle(
                "ALTER USER 'target'@'%' IDENTIFIED BY 'p2' RETAIN CURRENT PASSWORD");

        grantPriv(false);
        Assertions.assertThrows(AnalysisException.class, () -> cmd.doRun(ctx, null));
        Assertions.assertTrue(canLogin("target", "p1"));
        Assertions.assertFalse(canLogin("target", "p2"));

        grantPriv(true);
        cmd.doRun(ctx, null);
        Assertions.assertTrue(canLogin("target", "p2"));
        Assertions.assertTrue(canLogin("target", "p1"));

        ArgumentCaptor<PrivInfo> captor = ArgumentCaptor.forClass(PrivInfo.class);
        Mockito.verify(editLog, Mockito.atLeastOnce()).logSetPassword(captor.capture());
        Assertions.assertTrue(captor.getValue().isRetainPasswd());
    }

    @Test
    public void testDiscardJournalsAsNoOpPolicyEntry() throws Exception {
        // DISCARD OLD PASSWORD journals an OP_ALTER_USER entry whose carrier
        // op is SET_PASSWORD_POLICY with every option UNSET plus the discard
        // marker. A pre-feature binary ignores the marker and replays a policy
        // update that changes nothing; it must NOT ride OP_SET_PASSWORD (an
        // old binary's set-password replay appends to the password history
        // and refreshes the creation time) nor a new AlterUserOpType name (an
        // old binary deserializes it as null and fails replay).
        UserIdentity admin = createUser("adm2");
        UserIdentity user = createUser("dsc");
        auth.setPassword(user, MysqlPassword.makeScrambledPassword("p1"));
        auth.setPasswordInternal(user, MysqlPassword.makeScrambledPassword("p2"), null,
                true, false, true /* retain */, false);
        Assertions.assertTrue(canLogin("dsc", "p1"));
        ConnectContext ctx = ctxFor(admin);
        grantPriv(true);

        NereidsParser parser = new NereidsParser();
        AlterUserCommand cmd = (AlterUserCommand) parser.parseSingle("ALTER USER 'dsc'@'%' DISCARD OLD PASSWORD");
        Mockito.clearInvocations(editLog); // the setup above journaled set-password entries of its own
        cmd.doRun(ctx, null);
        Assertions.assertTrue(canLogin("dsc", "p2"));
        Assertions.assertFalse(canLogin("dsc", "p1"));

        // never journaled as a set-password
        Mockito.verify(editLog, Mockito.never()).logSetPassword(Mockito.any(PrivInfo.class));
        ArgumentCaptor<AlterUserOperationLog> captor = ArgumentCaptor.forClass(AlterUserOperationLog.class);
        Mockito.verify(editLog, Mockito.times(1)).logAlterUser(captor.capture());
        AlterUserOperationLog journaled = captor.getValue();
        Assertions.assertTrue(journaled.isDiscardOldPassword());
        Assertions.assertEquals(AlterUserOpType.SET_PASSWORD_POLICY, journaled.getOp());
        Assertions.assertNull(journaled.getPassword());
        Assertions.assertNull(journaled.getRole());
        Assertions.assertNull(journaled.getComment());
        Assertions.assertEquals(PasswordOptions.UNSET, journaled.getPasswordOptions().getExpirePolicySecond());
        Assertions.assertEquals(PasswordOptions.UNSET, journaled.getPasswordOptions().getHistoryPolicy());
        Assertions.assertEquals(PasswordOptions.UNSET, journaled.getPasswordOptions().getLoginAttempts());
        Assertions.assertEquals(PasswordOptions.UNSET, journaled.getPasswordOptions().getPasswordLockSecond());
        Assertions.assertEquals(PasswordOptions.UNSET, journaled.getPasswordOptions().getAccountUnlocked());

        // the marker survives the journal round trip, and the carrier op is
        // what a pre-feature binary (which drops the unknown field) sees
        AlterUserOperationLog reloaded = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(journaled),
                AlterUserOperationLog.class);
        Assertions.assertTrue(reloaded.isDiscardOldPassword());
        Assertions.assertEquals(AlterUserOpType.SET_PASSWORD_POLICY, reloaded.getOp());

        // a CURRENT binary replaying the entry discards the secondary
        UserIdentity follower = createUser("dsc2");
        auth.setPassword(follower, MysqlPassword.makeScrambledPassword("p1"));
        auth.setPasswordInternal(follower, MysqlPassword.makeScrambledPassword("p2"), null,
                true, false, true /* retain */, false);
        auth.replayAlterUser(AlterUserOperationLog.discardOldPassword(follower));
        Assertions.assertTrue(canLogin("dsc2", "p2"));
        Assertions.assertFalse(canLogin("dsc2", "p1"));
    }

    @Test
    public void testDiscardReplayLeavesPasswordPolicyUntouched() throws Exception {
        // The exact concern behind the carrier choice: replaying the DISCARD
        // entry must leave the password policy state alone on BOTH a current
        // binary and a pre-feature one (history entries, creation time).
        UserIdentity user = createUser("pol");
        auth.setPassword(user, MysqlPassword.makeScrambledPassword("p1"));
        // an expire policy makes the creation time live (0 = never refreshed)
        auth.getPasswdPolicyManager().updatePolicy(user, null, new PasswordOptions(
                86400 /* expire */, 3 /* history */, PasswordOptions.UNSET, PasswordOptions.UNSET,
                PasswordOptions.UNSET, PasswordOptions.UNSET));
        auth.setPasswordInternal(user, MysqlPassword.makeScrambledPassword("p2"), null,
                true, false, true /* retain */, false);
        PasswordPolicy policy = policyOf(user);
        // a sentinel a refresh would overwrite (recent enough not to expire the password)
        long sentinel = System.currentTimeMillis() - 12_345L;
        policy.getExpirePolicy().passwordCreateTime = sentinel;
        List<List<String>> before = auth.getPasswdPolicyManager().getPolicyInfo(user);
        int historySize = historySizeOf(user);

        // the pre-feature binary's view of the entry: the same carrier fields
        // with the marker dropped (this IS the pre-feature replay code path:
        // alterUserInternal(SET_PASSWORD_POLICY) is unchanged)
        AlterUserOperationLog journaled = AlterUserOperationLog.discardOldPassword(user);
        auth.replayAlterUser(new AlterUserOperationLog(journaled.getOp(), journaled.getUserIdent(),
                journaled.getPassword(), journaled.getRole(), journaled.getPasswordOptions(),
                journaled.getComment()));
        Assertions.assertTrue(canLogin("pol", "p2"));
        Assertions.assertTrue(canLogin("pol", "p1")); // a pre-feature binary keeps the secondary
        Assertions.assertEquals(historySize, historySizeOf(user));
        Assertions.assertEquals(sentinel, policy.getExpirePolicy().passwordCreateTime);
        Assertions.assertEquals(before, auth.getPasswdPolicyManager().getPolicyInfo(user));

        // the current binary's replay: the secondary goes, the policy stays
        auth.replayAlterUser(journaled);
        Assertions.assertTrue(canLogin("pol", "p2"));
        Assertions.assertFalse(canLogin("pol", "p1"));
        Assertions.assertEquals(historySize, historySizeOf(user));
        Assertions.assertEquals(sentinel, policy.getExpirePolicy().passwordCreateTime);
        Assertions.assertEquals(before, auth.getPasswdPolicyManager().getPolicyInfo(user));

        // the contrast that ruled out an OP_SET_PASSWORD carrier: a plain
        // set-password replay of the unchanged primary (what a pre-feature
        // binary would have run) appends to the history and refreshes the
        // creation time
        auth.replaySetPassword(new PrivInfo(user, null, MysqlPassword.makeScrambledPassword("p2"), null, null));
        Assertions.assertEquals(historySize + 1, historySizeOf(user));
        Assertions.assertNotEquals(sentinel, policy.getExpirePolicy().passwordCreateTime);
    }

    private PasswordPolicy policyOf(UserIdentity user) throws Exception {
        Field field = PasswordPolicyManager.class.getDeclaredField("policyMap");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<UserIdentity, PasswordPolicy> policyMap = (Map<UserIdentity, PasswordPolicy>) field.get(
                auth.getPasswdPolicyManager());
        return policyMap.get(user);
    }

    private int historySizeOf(UserIdentity user) throws Exception {
        Field field = PasswordPolicy.class.getDeclaredField("historyPolicy");
        field.setAccessible(true);
        return ((PasswordPolicy.HistoryPolicy) field.get(policyOf(user))).historyPasswords.size();
    }

    @Test
    public void testSecondaryPasswordSubjectToAccountLock() throws Exception {
        // account lock/expiration policy dominates the secondary slot: a
        // locked account must reject BOTH passwords, and the secondary-auth
        // success telemetry (log + metric) must NOT fire for a rejected
        // attempt — it is emitted only after the policy check passes (see
        // UserManager.reportSecondaryPasswordAuth)
        boolean oldIsInit = MetricRepo.isInit;
        LongCounterMetric oldCounter = MetricRepo.COUNTER_SECONDARY_PASSWORD_AUTH;
        MetricRepo.COUNTER_SECONDARY_PASSWORD_AUTH = new LongCounterMetric("secondary_password_auth_total",
                MetricUnit.REQUESTS, "test");
        MetricRepo.isInit = true;
        try {
            UserIdentity admin = createUser("adm3");
            UserIdentity user = createUser("lockacc");
            auth.setPassword(user, MysqlPassword.makeScrambledPassword("p1"));
            auth.setPasswordInternal(user, MysqlPassword.makeScrambledPassword("p2"), null,
                    true, false, true /* retain */, false);
            Assertions.assertTrue(canLogin("lockacc", "p1"));
            // an accepted secondary-slot authentication counts
            Assertions.assertEquals(Long.valueOf(1L), MetricRepo.COUNTER_SECONDARY_PASSWORD_AUTH.getValue());

            ConnectContext ctx = ctxFor(admin);
            grantPriv(true);
            NereidsParser parser = new NereidsParser();
            AlterUserCommand policy = (AlterUserCommand) parser.parseSingle(
                    "ALTER USER 'lockacc'@'%' FAILED_LOGIN_ATTEMPTS 1 PASSWORD_LOCK_TIME 1 DAY");
            policy.doRun(ctx, null);

            // one failed attempt locks the account
            Assertions.assertFalse(canLogin("lockacc", "wrong"));
            // both slots now reject: the retained password does not bypass policy
            Assertions.assertFalse(canLogin("lockacc", "p2"));
            Assertions.assertFalse(canLogin("lockacc", "p1"));
            // ... and the REJECTED secondary attempt did not count as a
            // successful secondary authentication
            Assertions.assertEquals(Long.valueOf(1L), MetricRepo.COUNTER_SECONDARY_PASSWORD_AUTH.getValue());
        } finally {
            MetricRepo.isInit = oldIsInit;
            MetricRepo.COUNTER_SECONDARY_PASSWORD_AUTH = oldCounter;
        }
    }

    @Test
    public void testDomainResolverRefreshCarriesSecondary() throws Exception {
        // resolver-materialized IP users must carry BOTH password slots:
        // rebuilding them from the primary alone would evict the retained
        // password on the next resolver refresh after a rotation
        UserIdentity domainIdent = new UserIdentity("domuser", "mydomain.example", true);
        domainIdent.setIsAnalyzed();
        CreateUserCommand create = new CreateUserCommand(new CreateUserInfo(new UserDesc(domainIdent)));
        auth.createUser(create.getInfo());
        auth.setPassword(domainIdent, MysqlPassword.makeScrambledPassword("p1"));

        auth.refreshUserPrivEntriesByResovledIPs(
                ImmutableMap.of("mydomain.example", Sets.newHashSet("192.168.1.1")));
        Assertions.assertTrue(canLogin("domuser", "p1"));

        // rotate the domain user with RETAIN, then refresh (what the
        // DomainResolver does periodically)
        auth.setPasswordInternal(domainIdent, MysqlPassword.makeScrambledPassword("p2"), null,
                true, false, true /* retain */, false);
        auth.refreshUserPrivEntriesByResovledIPs(
                ImmutableMap.of("mydomain.example", Sets.newHashSet("192.168.1.1")));

        Assertions.assertTrue(canLogin("domuser", "p2"));
        // the retained password survives the refresh
        Assertions.assertTrue(canLogin("domuser", "p1"));
        Assertions.assertFalse(canLogin("domuser", "p0"));
    }
}
