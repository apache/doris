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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

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

    @Before
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

    @After
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
        Assert.assertTrue(canLogin("rot", "p1"));
        Assert.assertFalse(canLogin("rot", "p2"));

        // p2 RETAIN CURRENT PASSWORD -> p1 and p2 both authenticate
        auth.setPasswordInternal(user, MysqlPassword.makeScrambledPassword("p2"), null,
                true, false, true /* retain */, false);
        Assert.assertTrue(canLogin("rot", "p2"));
        Assert.assertTrue(canLogin("rot", "p1"));
        Assert.assertFalse(canLogin("rot", "p0"));

        // p3 RETAIN -> the one-secondary rule evicts p1; p2 + p3 authenticate
        auth.setPasswordInternal(user, MysqlPassword.makeScrambledPassword("p3"), null,
                true, false, true /* retain */, false);
        Assert.assertTrue(canLogin("rot", "p3"));
        Assert.assertTrue(canLogin("rot", "p2"));
        Assert.assertFalse(canLogin("rot", "p1"));

        // p4 WITHOUT retain -> the secondary REMAINS UNCHANGED (MySQL: "the
        // secondary password remains unchanged"); the replaced primary p3 is
        // simply gone -> p4 + p2 authenticate, p3 does not
        auth.setPasswordInternal(user, MysqlPassword.makeScrambledPassword("p4"), null,
                true, false, false /* no retain */, false);
        Assert.assertTrue(canLogin("rot", "p4"));
        Assert.assertFalse(canLogin("rot", "p3"));
        Assert.assertTrue(canLogin("rot", "p2"));

        // p5 RETAIN, then DISCARD OLD PASSWORD (via the replay path, which is
        // also what a follower executes) -> only p5 remains
        auth.setPasswordInternal(user, MysqlPassword.makeScrambledPassword("p5"), null,
                true, false, true /* retain */, false);
        Assert.assertTrue(canLogin("rot", "p4"));
        auth.replayAlterUser(new AlterUserOperationLog(AlterUserOpType.DISCARD_OLD_PASSWORD,
                user, null, null, PasswordOptions.UNSET_OPTION, null));
        Assert.assertTrue(canLogin("rot", "p5"));
        Assert.assertFalse(canLogin("rot", "p4"));

        // DISCARD with no secondary present: silent no-op (MySQL: discards
        // the secondary password, "if one exists")
        auth.replayAlterUser(new AlterUserOperationLog(AlterUserOpType.DISCARD_OLD_PASSWORD,
                user, null, null, PasswordOptions.UNSET_OPTION, null));
        Assert.assertTrue(canLogin("rot", "p5"));
    }

    @Test
    public void testRetainRequiresNonEmptyCurrentPassword() throws DdlException {
        UserIdentity user = createUser("empty_cur");
        // MySQL: "If you specify RETAIN CURRENT PASSWORD for an account that
        // has an empty primary password, the statement fails."
        Assert.assertThrows(DdlException.class, () -> auth.setPasswordInternal(user,
                MysqlPassword.makeScrambledPassword("p1"), null, true, false, true /* retain */, false));
    }

    @Test
    public void testEmptyNewPasswordEmptiesSecondary() throws DdlException {
        UserIdentity user = createUser("empty_new");
        auth.setPassword(user, MysqlPassword.makeScrambledPassword("p1"));
        auth.setPasswordInternal(user, MysqlPassword.makeScrambledPassword("p2"), null,
                true, false, true /* retain */, false);
        Assert.assertTrue(canLogin("empty_new", "p1"));

        // MySQL: "If the new password ... is empty, the secondary password
        // becomes empty as well, even if RETAIN CURRENT PASSWORD is given."
        auth.setPasswordInternal(user, new byte[0], null,
                true, false, true /* retain */, false);
        Assert.assertTrue(canLogin("empty_new", ""));
        Assert.assertFalse(canLogin("empty_new", "p1"));
        Assert.assertFalse(canLogin("empty_new", "p2"));
    }

    @Test
    public void testRetainReplay() throws DdlException {
        UserIdentity user = createUser("replayer");
        auth.setPassword(user, MysqlPassword.makeScrambledPassword("p1"));

        // a follower replaying OP_SET_PASSWORD with retainPasswd=true must
        // reach the same dual-slot state as the master
        auth.replaySetPassword(new PrivInfo(user, null,
                MysqlPassword.makeScrambledPassword("p2"), null, null, true /* retain */, false));
        Assert.assertTrue(canLogin("replayer", "p2"));
        Assert.assertTrue(canLogin("replayer", "p1"));

        // and a replay without the flag (all journals written before this
        // feature, plus any plain password change) behaves like a plain
        // change: the primary is replaced, the secondary remains UNCHANGED
        // (MySQL: "the secondary password remains unchanged")
        auth.replaySetPassword(new PrivInfo(user, null,
                MysqlPassword.makeScrambledPassword("p3"), null, null));
        Assert.assertTrue(canLogin("replayer", "p3"));
        Assert.assertFalse(canLogin("replayer", "p2"));
        Assert.assertTrue(canLogin("replayer", "p1"));
    }

    @Test
    public void testGsonCompat() {
        // round trip preserves the secondary slot
        Password password = new Password(MysqlPassword.makeScrambledPassword("p2"));
        password.setSecondaryPassword(MysqlPassword.makeScrambledPassword("p1"));
        Password reloaded = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(password), Password.class);
        Assert.assertArrayEquals(password.getPassword(), reloaded.getPassword());
        Assert.assertArrayEquals(password.getSecondaryPassword(), reloaded.getSecondaryPassword());
        Assert.assertTrue(reloaded.hasSecondaryPassword());

        // an image/journal written BEFORE this feature deserializes with an
        // absent secondary slot -> unchanged single-password behavior
        Password legacy = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(new Password(MysqlPassword.makeScrambledPassword("p1"))), Password.class);
        Assert.assertFalse(legacy.hasSecondaryPassword());
        Assert.assertNull(legacy.getSecondaryPassword());
    }

    @Test
    public void testParser() {
        NereidsParser parser = new NereidsParser();

        AlterUserCommand retain = (AlterUserCommand) parser.parseSingle(
                "ALTER USER u1 IDENTIFIED BY 'x' RETAIN CURRENT PASSWORD");
        Assert.assertTrue(retain.getAlterUserInfo().isRetainCurrentPassword());
        Assert.assertFalse(retain.getAlterUserInfo().isDiscardOldPassword());

        AlterUserCommand discard = (AlterUserCommand) parser.parseSingle(
                "ALTER USER u1 DISCARD OLD PASSWORD");
        Assert.assertTrue(discard.getAlterUserInfo().isDiscardOldPassword());
        Assert.assertFalse(discard.getAlterUserInfo().isRetainCurrentPassword());

        AlterUserCommand plain = (AlterUserCommand) parser.parseSingle(
                "ALTER USER u1 IDENTIFIED BY 'x'");
        Assert.assertFalse(plain.getAlterUserInfo().isRetainCurrentPassword());
        Assert.assertFalse(plain.getAlterUserInfo().isDiscardOldPassword());

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
        Assert.assertTrue(cmd.getAlterUserInfo().isRetainCurrentPassword());
        Assert.assertThrows(AnalysisException.class, () -> cmd.getAlterUserInfo().validate());
    }

    @Test
    public void testSetPasswordRetainOverload() throws DdlException {
        // SET PASSWORD ... RETAIN CURRENT PASSWORD routes through
        // Auth.setPassword(user, pw, retain=true) — the self-service path a
        // service account uses to rotate its own credential with an overlap.
        UserIdentity user = createUser("setpw");
        auth.setPassword(user, MysqlPassword.makeScrambledPassword("p1"));
        Assert.assertTrue(canLogin("setpw", "p1"));

        auth.setPassword(user, MysqlPassword.makeScrambledPassword("p2"), true /* retain */);
        Assert.assertTrue(canLogin("setpw", "p2"));         // new primary
        Assert.assertTrue(canLogin("setpw", "p1"));         // retained secondary

        // a plain SET PASSWORD (retain=false) preserves the secondary (MySQL)
        auth.setPassword(user, MysqlPassword.makeScrambledPassword("p3"), false);
        Assert.assertTrue(canLogin("setpw", "p3"));
        Assert.assertTrue(canLogin("setpw", "p1"));         // still valid
        Assert.assertFalse(canLogin("setpw", "p2"));        // replaced primary gone
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
        Assert.assertEquals(RedirectStatus.FORWARD_WITH_SYNC, cmd.toRedirectStatus());

        grantPriv(false);
        Assert.assertThrows(AnalysisException.class, () -> cmd.run(ctx, null));
        // nothing changed: p1 still the only valid password
        Assert.assertTrue(canLogin("gated", "p1"));
        Assert.assertFalse(canLogin("gated", "p2"));

        grantPriv(true);
        cmd.run(ctx, null);
        Assert.assertTrue(canLogin("gated", "p2"));
        Assert.assertTrue(canLogin("gated", "p1"));

        // the journaled entry carries the retain flag
        ArgumentCaptor<PrivInfo> captor = ArgumentCaptor.forClass(PrivInfo.class);
        Mockito.verify(editLog, Mockito.atLeastOnce()).logSetPassword(captor.capture());
        PrivInfo journaled = captor.getValue();
        Assert.assertTrue(journaled.isRetainPasswd());
        Assert.assertFalse(journaled.isDiscardPasswd());
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
        Assert.assertTrue(canLogin("selfplain", "p2"));
        Assert.assertFalse(canLogin("selfplain", "p1"));
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
        Assert.assertThrows(AnalysisException.class, () -> cmd.doRun(ctx, null));
        Assert.assertTrue(canLogin("target", "p1"));
        Assert.assertFalse(canLogin("target", "p2"));

        grantPriv(true);
        cmd.doRun(ctx, null);
        Assert.assertTrue(canLogin("target", "p2"));
        Assert.assertTrue(canLogin("target", "p1"));

        ArgumentCaptor<PrivInfo> captor = ArgumentCaptor.forClass(PrivInfo.class);
        Mockito.verify(editLog, Mockito.atLeastOnce()).logSetPassword(captor.capture());
        Assert.assertTrue(captor.getValue().isRetainPasswd());
    }

    @Test
    public void testDiscardJournalsAsSetPasswordPrivInfo() throws Exception {
        // DISCARD OLD PASSWORD must journal via OP_SET_PASSWORD/PrivInfo
        // (passwd = the unchanged primary), NEVER via OP_ALTER_USER: a
        // pre-feature FE binary deserializes an unknown AlterUserOpType as
        // null and fails replay, but replays the PrivInfo form as a plain
        // set-password to the value the account already has — a no-op.
        UserIdentity admin = createUser("adm2");
        UserIdentity user = createUser("dsc");
        auth.setPassword(user, MysqlPassword.makeScrambledPassword("p1"));
        auth.setPasswordInternal(user, MysqlPassword.makeScrambledPassword("p2"), null,
                true, false, true /* retain */, false);
        Assert.assertTrue(canLogin("dsc", "p1"));
        ConnectContext ctx = ctxFor(admin);
        grantPriv(true);

        NereidsParser parser = new NereidsParser();
        AlterUserCommand cmd = (AlterUserCommand) parser.parseSingle("ALTER USER 'dsc'@'%' DISCARD OLD PASSWORD");
        cmd.doRun(ctx, null);
        Assert.assertTrue(canLogin("dsc", "p2"));
        Assert.assertFalse(canLogin("dsc", "p1"));

        // never journaled as an ALTER USER operation
        Mockito.verify(editLog, Mockito.never()).logAlterUser(Mockito.any(AlterUserOperationLog.class));
        ArgumentCaptor<PrivInfo> captor = ArgumentCaptor.forClass(PrivInfo.class);
        Mockito.verify(editLog, Mockito.atLeastOnce()).logSetPassword(captor.capture());
        PrivInfo journaled = captor.getValue();
        Assert.assertTrue(journaled.isDiscardPasswd());
        Assert.assertFalse(journaled.isRetainPasswd());
        // passwd rides the CURRENT primary so an old binary's plain
        // set-password replay changes nothing
        Assert.assertArrayEquals(MysqlPassword.makeScrambledPassword("p2"), journaled.getPasswd());

        // a CURRENT binary replaying the entry discards the secondary
        UserIdentity follower = createUser("dsc2");
        auth.setPassword(follower, MysqlPassword.makeScrambledPassword("p1"));
        auth.setPasswordInternal(follower, MysqlPassword.makeScrambledPassword("p2"), null,
                true, false, true /* retain */, false);
        auth.replaySetPassword(new PrivInfo(follower, null,
                MysqlPassword.makeScrambledPassword("p2"), null, null, false, true /* discard */));
        Assert.assertTrue(canLogin("dsc2", "p2"));
        Assert.assertFalse(canLogin("dsc2", "p1"));

        // a PRE-FEATURE binary interprets the same entry as a plain
        // set-password (flags unknown to it): the primary stays valid
        UserIdentity legacy = createUser("dsc3");
        auth.setPassword(legacy, MysqlPassword.makeScrambledPassword("p2"));
        auth.replaySetPassword(new PrivInfo(legacy, null,
                MysqlPassword.makeScrambledPassword("p2"), null, null));
        Assert.assertTrue(canLogin("dsc3", "p2"));
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
            Assert.assertTrue(canLogin("lockacc", "p1"));
            // an accepted secondary-slot authentication counts
            Assert.assertEquals(Long.valueOf(1L), MetricRepo.COUNTER_SECONDARY_PASSWORD_AUTH.getValue());

            ConnectContext ctx = ctxFor(admin);
            grantPriv(true);
            NereidsParser parser = new NereidsParser();
            AlterUserCommand policy = (AlterUserCommand) parser.parseSingle(
                    "ALTER USER 'lockacc'@'%' FAILED_LOGIN_ATTEMPTS 1 PASSWORD_LOCK_TIME 1 DAY");
            policy.doRun(ctx, null);

            // one failed attempt locks the account
            Assert.assertFalse(canLogin("lockacc", "wrong"));
            // both slots now reject: the retained password does not bypass policy
            Assert.assertFalse(canLogin("lockacc", "p2"));
            Assert.assertFalse(canLogin("lockacc", "p1"));
            // ... and the REJECTED secondary attempt did not count as a
            // successful secondary authentication
            Assert.assertEquals(Long.valueOf(1L), MetricRepo.COUNTER_SECONDARY_PASSWORD_AUTH.getValue());
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
        Assert.assertTrue(canLogin("domuser", "p1"));

        // rotate the domain user with RETAIN, then refresh (what the
        // DomainResolver does periodically)
        auth.setPasswordInternal(domainIdent, MysqlPassword.makeScrambledPassword("p2"), null,
                true, false, true /* retain */, false);
        auth.refreshUserPrivEntriesByResovledIPs(
                ImmutableMap.of("mydomain.example", Sets.newHashSet("192.168.1.1")));

        Assert.assertTrue(canLogin("domuser", "p2"));
        // the retained password survives the refresh
        Assert.assertTrue(canLogin("domuser", "p1"));
        Assert.assertFalse(canLogin("domuser", "p0"));
    }
}
