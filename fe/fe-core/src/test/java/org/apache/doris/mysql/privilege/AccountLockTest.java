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
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MysqlDBTable;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.ShowCreateUserCommand;
import org.apache.doris.persist.AlterUserOperationLog;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.utframe.TestWithFeService;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

/**
 * MySQL-compatible administrative account lock: {@code CREATE USER ... ACCOUNT_LOCK},
 * {@code ALTER USER ... ACCOUNT_LOCK | ACCOUNT_UNLOCK}. Persisted and journaled, enforced at
 * password authentication only (an authentication check, not a session check, as in MySQL).
 */
public class AccountLockTest extends TestWithFeService {

    private static final String HOST = "192.168.1.1";

    private Auth auth() {
        return Env.getCurrentEnv().getAuth();
    }

    private UserIdentity ident(String name) {
        UserIdentity user = new UserIdentity(name, "%");
        user.setIsAnalyzed();
        return user;
    }

    private void run(String sql) throws Exception {
        ((Command) new NereidsParser().parseSingle(sql)).run(connectContext, null);
    }

    private String loginError(String user, String password) {
        try {
            auth().checkPlainPassword(user, HOST, password, null);
            return null;
        } catch (AuthenticationException e) {
            return e.getMessage();
        }
    }

    private boolean canLogin(String user, String password) {
        return loginError(user, password) == null;
    }

    private String showCreateUser(String name) throws Exception {
        List<List<String>> rows = new ShowCreateUserCommand(ident(name)).doRun(connectContext, null)
                .getResultRows();
        Assertions.assertEquals(1, rows.size());
        return rows.get(0).get(1);
    }

    private boolean policySaysLocked(String name) {
        List<List<String>> info = auth().getPasswdPolicyManager().getPolicyInfo(ident(name));
        Assertions.assertEquals("password_policy.account_locked", info.get(8).get(0));
        return Boolean.parseBoolean(info.get(8).get(1));
    }

    private static final List<String> MYSQL_USER_COLUMNS = MysqlDBTable.TABLE_MAP.get("user").getFullSchema()
            .stream().map(c -> c.getName()).collect(Collectors.toList());

    private List<String> mysqlUserRow(String name) {
        return auth().getAllUserInfo(UserIdentity.ROOT).stream()
                .filter(row -> name.equals(row.get(MYSQL_USER_COLUMNS.indexOf("User"))))
                .findFirst().orElseThrow(AssertionError::new);
    }

    /** The value of the mysql.user column {@code column} in the row of {@code name}, resolved by name. */
    private String mysqlUserColumn(String name, String column) {
        int index = MYSQL_USER_COLUMNS.indexOf(column);
        Assertions.assertTrue(index >= 0, "mysql.user has no column " + column);
        return mysqlUserRow(name).get(index);
    }

    // mysql.user is filled from the policy rows by name. The former "copy if exactly 8 rows" guard
    // left EVERY password_policy column empty for every user once the administrative lock added a
    // ninth row (#67792); the lock itself is now a column of its own.
    @Test
    public void testMysqlUserCarriesThePolicyColumnsAndTheLock() throws Exception {
        run("CREATE USER 'mu1'@'%' IDENTIFIED BY 'p1' PASSWORD_HISTORY 5 PASSWORD_EXPIRE INTERVAL 30 DAY"
                + " FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1 DAY");
        // The policy reports its rows under exactly the names mysql.user's trailing columns carry.
        List<String> policyRowNames = auth().getPasswdPolicyManager().getPolicyInfo(ident("mu1")).stream()
                .map(r -> r.get(0)).collect(Collectors.toList());
        Assertions.assertEquals(PasswordPolicy.MYSQL_USER_COLUMNS, policyRowNames);
        Assertions.assertEquals(MYSQL_USER_COLUMNS.subList(MYSQL_USER_COLUMNS.size() - policyRowNames.size(),
                MYSQL_USER_COLUMNS.size()), policyRowNames);
        Assertions.assertEquals(MYSQL_USER_COLUMNS.size(), mysqlUserRow("mu1").size());

        Assertions.assertEquals("2592000", mysqlUserColumn("mu1", "password_policy.expiration_seconds"));
        Assertions.assertFalse(mysqlUserColumn("mu1", "password_policy.password_creation_time").isEmpty());
        Assertions.assertEquals("5", mysqlUserColumn("mu1", "password_policy.history_num"));
        Assertions.assertEquals("***", mysqlUserColumn("mu1", "password_policy.history_passwords"));
        Assertions.assertEquals("3", mysqlUserColumn("mu1", "password_policy.num_failed_login"));
        Assertions.assertEquals("86400", mysqlUserColumn("mu1", "password_policy.password_lock_seconds"));
        Assertions.assertEquals("0", mysqlUserColumn("mu1", "password_policy.failed_login_counter"));
        Assertions.assertEquals("", mysqlUserColumn("mu1", "password_policy.lock_time"));
        Assertions.assertEquals("false", mysqlUserColumn("mu1", "password_policy.account_locked"));

        run("ALTER USER 'mu1'@'%' ACCOUNT_LOCK");
        Assertions.assertEquals("true", mysqlUserColumn("mu1", "password_policy.account_locked"));
        run("ALTER USER 'mu1'@'%' ACCOUNT_UNLOCK");
        Assertions.assertEquals("false", mysqlUserColumn("mu1", "password_policy.account_locked"));
    }

    @Test
    public void testAlterLockRefusesLoginAndUnlockRestoresIt() throws Exception {
        run("CREATE USER 'lk1'@'%' IDENTIFIED BY 'p1'");
        Assertions.assertTrue(canLogin("lk1", "p1"));
        Assertions.assertFalse(showCreateUser("lk1").contains("ACCOUNT_LOCK"));

        run("ALTER USER 'lk1'@'%' ACCOUNT_LOCK");
        Assertions.assertTrue(policySaysLocked("lk1"));
        Assertions.assertTrue(showCreateUser("lk1").contains(" ACCOUNT_LOCK"), showCreateUser("lk1"));
        // the right password is refused with MySQL's ER_ACCOUNT_HAS_BEEN_LOCKED text ...
        String refused = loginError("lk1", "p1");
        Assertions.assertNotNull(refused);
        Assertions.assertTrue(refused.contains("Account is locked"), refused);
        // ... and a wrong one stays a plain access-denied (the lock leaks nothing extra)
        Assertions.assertFalse(canLogin("lk1", "wrong"));

        // a policy edit does NOT clear an administrative lock
        run("ALTER USER 'lk1'@'%' FAILED_LOGIN_ATTEMPTS 3");
        Assertions.assertTrue(policySaysLocked("lk1"));
        Assertions.assertFalse(canLogin("lk1", "p1"));

        run("ALTER USER 'lk1'@'%' ACCOUNT_UNLOCK");
        Assertions.assertFalse(policySaysLocked("lk1"));
        Assertions.assertTrue(canLogin("lk1", "p1"));
        Assertions.assertFalse(showCreateUser("lk1").contains("ACCOUNT_LOCK"));
    }

    @Test
    public void testCreateUserAccountLockIsHonored() throws Exception {
        run("CREATE USER 'lk2'@'%' IDENTIFIED BY 'p2' ACCOUNT_LOCK");
        Assertions.assertTrue(policySaysLocked("lk2"));
        String refused = loginError("lk2", "p2");
        Assertions.assertNotNull(refused);
        Assertions.assertTrue(refused.contains("Account is locked"), refused);

        run("ALTER USER 'lk2'@'%' ACCOUNT_UNLOCK");
        Assertions.assertTrue(canLogin("lk2", "p2"));
    }

    @Test
    public void testUnlockAlsoClearsTheFailedLoginLock() throws Exception {
        run("CREATE USER 'lk3'@'%' IDENTIFIED BY 'p3' FAILED_LOGIN_ATTEMPTS 1 PASSWORD_LOCK_TIME UNBOUNDED");
        Assertions.assertTrue(canLogin("lk3", "p3"));
        Assertions.assertFalse(canLogin("lk3", "wrong"));
        // one failure with FAILED_LOGIN_ATTEMPTS 1: the failed-login lock is on
        String blocked = loginError("lk3", "p3");
        Assertions.assertNotNull(blocked);
        Assertions.assertTrue(blocked.contains("Account is blocked"), blocked);
        Assertions.assertFalse(policySaysLocked("lk3")); // not an administrative lock

        run("ALTER USER 'lk3'@'%' ACCOUNT_UNLOCK");
        Assertions.assertTrue(canLogin("lk3", "p3"));
    }

    @Test
    public void testLockIsJournaledAndPersisted() throws Exception {
        // a follower replays the OP_ALTER_USER entry the master journaled for ACCOUNT_LOCK
        run("CREATE USER 'lk4'@'%' IDENTIFIED BY 'p4'");
        Assertions.assertTrue(canLogin("lk4", "p4"));
        auth().replayAlterUser(new AlterUserOperationLog(AlterUserOpType.LOCK_ACCOUNT, ident("lk4"),
                null, null, PasswordOptions.UNSET_OPTION, null));
        Assertions.assertTrue(policySaysLocked("lk4"));
        Assertions.assertFalse(canLogin("lk4", "p4"));
        auth().replayAlterUser(new AlterUserOperationLog(AlterUserOpType.UNLOCK_ACCOUNT, ident("lk4"),
                null, null, PasswordOptions.UNSET_OPTION, null));
        Assertions.assertTrue(canLogin("lk4", "p4"));

        // the flag rides the password policy's image serialization ...
        PasswordPolicy locked = PasswordPolicy.createDefault();
        locked.lockAccount();
        PasswordPolicy reloaded = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(locked), PasswordPolicy.class);
        Assertions.assertTrue(reloaded.isAccountLocked());

        // ... and an image written before the field deserializes UNLOCKED
        JsonObject legacyJson = JsonParser.parseString(GsonUtils.GSON.toJson(locked)).getAsJsonObject();
        Assertions.assertNotNull(legacyJson.getAsJsonObject("failedLoginPolicy").remove("manuallyLocked"));
        PasswordPolicy legacy = GsonUtils.GSON.fromJson(legacyJson, PasswordPolicy.class);
        Assertions.assertFalse(legacy.isAccountLocked());
    }

    @Test
    public void testLockCombinedWithPolicyOptionsIsRejectedNotHalfApplied() throws Exception {
        run("CREATE USER 'lk6'@'%' IDENTIFIED BY 'p6'");
        Exception e = Assertions.assertThrows(Exception.class, () ->
                run("ALTER USER 'lk6'@'%' FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 60 SECOND ACCOUNT_LOCK"));
        Assertions.assertTrue(e.getMessage().contains("one type of operation"), e.getMessage());
        // nothing was applied: not locked, and the policy options stayed untouched
        Assertions.assertFalse(policySaysLocked("lk6"));
        Assertions.assertTrue(canLogin("lk6", "p6"));
        List<List<String>> info = auth().getPasswdPolicyManager().getPolicyInfo(ident("lk6"));
        Assertions.assertEquals("DISABLED", info.get(4).get(1)); // NUM_FAILED_LOGIN
        Assertions.assertEquals("DISABLED", info.get(5).get(1)); // PASSWORD_LOCK_SECONDS
        // the same combination on ACCOUNT_UNLOCK is rejected the same way
        e = Assertions.assertThrows(Exception.class, () ->
                run("ALTER USER 'lk6'@'%' FAILED_LOGIN_ATTEMPTS 3 ACCOUNT_UNLOCK"));
        Assertions.assertTrue(e.getMessage().contains("one type of operation"), e.getMessage());
    }

    @Test
    public void testRootCannotBeLocked() throws Exception {
        Exception e = Assertions.assertThrows(Exception.class, () -> run("ALTER USER 'root'@'%' ACCOUNT_LOCK"));
        Assertions.assertTrue(e.getMessage().contains("Can not lock root user"), e.getMessage());
        Assertions.assertTrue(canLogin("root", ""));
    }

    @Test
    public void testRootAtSpecificHostCanBeLocked() throws Exception {
        // only root@'%' is the system root; root@'<host>' is an ordinary account. Not HOST: that would
        // put root@HOST ahead of root@'%' for the other tests' root logins from HOST
        run("CREATE USER 'root'@'8.8.20.39' IDENTIFIED BY 'p8'");
        run("ALTER USER 'root'@'8.8.20.39' ACCOUNT_LOCK");
        UserIdentity rootAtHost = new UserIdentity("root", "8.8.20.39");
        rootAtHost.setIsAnalyzed();
        Exception e = Assertions.assertThrows(AuthenticationException.class,
                () -> auth().checkPlainPasswordForUserIdentity(rootAtHost, "p8", null));
        Assertions.assertTrue(e.getMessage().contains("Account is locked"), e.getMessage());
        Assertions.assertTrue(canLogin("root", ""));
    }

    @Test
    public void testCheckAccountLockedIsTheSharedPredicateForEveryAuthenticator() throws Exception {
        // the check every authentication path runs after its authenticator accepted the credential
        run("CREATE USER 'lk7'@'%' IDENTIFIED BY 'p7'");
        Assertions.assertDoesNotThrow(() -> auth().checkAccountLocked(ident("lk7")));
        Assertions.assertDoesNotThrow(() -> auth().checkAccountLocked(ident("nobody_without_a_policy")));
        Assertions.assertDoesNotThrow(() -> auth().checkAccountLocked(null));
        run("ALTER USER 'lk7'@'%' ACCOUNT_LOCK");
        AuthenticationException refused = Assertions.assertThrows(AuthenticationException.class,
                () -> auth().checkAccountLocked(ident("lk7")));
        Assertions.assertTrue(refused.getMessage().contains("Account is locked"), refused.getMessage());
        run("ALTER USER 'lk7'@'%' ACCOUNT_UNLOCK");
        Assertions.assertDoesNotThrow(() -> auth().checkAccountLocked(ident("lk7")));
    }
}
