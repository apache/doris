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
}
