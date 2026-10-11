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
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Only the full identity root@'%' is the system root user. An account created as root@'<host>' is an
 * ordinary account: a user with GRANT privilege manages it like any other account, and it has no more
 * say over root@'%' than any other non-root user.
 */
public class SystemRootUserTest extends TestWithFeService {

    private static final String HOST = "8.8.20.39";

    @Override
    protected void runBeforeAll() throws Exception {
        run("CREATE USER 'root'@'" + HOST + "' IDENTIFIED BY 'Pwd_a1'");
        run("GRANT GRANT_PRIV ON *.*.* TO 'root'@'" + HOST + "'");
        run("CREATE USER 'grant_admin'@'%'");
        run("GRANT GRANT_PRIV ON *.*.* TO 'grant_admin'@'%'");
    }

    @Override
    protected void runBeforeEach() throws Exception {
        useUser(Auth.ROOT_USER);
    }

    private void run(String sql) throws Exception {
        ((Command) new NereidsParser().parseSingle(sql)).run(connectContext, null);
    }

    private void assertRejected(String sql, String expectedMessage) {
        Exception e = Assertions.assertThrows(Exception.class, () -> run(sql));
        Assertions.assertTrue(e.getMessage().contains(expectedMessage), e.getMessage());
    }

    private void assertPassword(UserIdentity user, String password) {
        Assertions.assertDoesNotThrow(
                () -> Env.getCurrentEnv().getAuth().checkPlainPasswordForUserIdentity(user, password, null));
    }

    private UserIdentity rootAtHost() {
        UserIdentity user = new UserIdentity(Auth.ROOT_USER, HOST);
        user.setIsAnalyzed();
        return user;
    }

    @Test
    public void testRootCanSetPasswordForRootAtSpecificHost() throws Exception {
        run("SET PASSWORD FOR 'root'@'" + HOST + "' = PASSWORD('Pwd_f6')");
        assertPassword(rootAtHost(), "Pwd_f6");
    }

    @Test
    public void testGrantUserCanSetPasswordForRootAtSpecificHost() throws Exception {
        useUser("grant_admin");
        run("SET PASSWORD FOR 'root'@'" + HOST + "' = PASSWORD('Pwd_b2')");
        assertPassword(rootAtHost(), "Pwd_b2");
    }

    @Test
    public void testGrantUserCanAlterRootAtSpecificHost() throws Exception {
        useUser("grant_admin");
        run("ALTER USER 'root'@'" + HOST + "' IDENTIFIED BY 'Pwd_c3'");
        assertPassword(rootAtHost(), "Pwd_c3");
    }

    @Test
    public void testGrantUserStillCannotModifySystemRoot() throws Exception {
        useUser("grant_admin");
        assertRejected("SET PASSWORD FOR 'root'@'%' = PASSWORD('Pwd_d4')",
                "Can not set password for root user, except root itself");
        assertRejected("ALTER USER 'root'@'%' IDENTIFIED BY 'Pwd_d4'", "Only root user can modify root user");
        assertPassword(UserIdentity.ROOT, "");
    }

    @Test
    public void testRootAtSpecificHostCannotModifySystemRoot() throws Exception {
        useUser(Auth.ROOT_USER, HOST);
        assertRejected("SET PASSWORD FOR 'root'@'%' = PASSWORD('Pwd_e5')",
                "Can not set password for root user, except root itself");
        assertRejected("ALTER USER 'root'@'%' IDENTIFIED BY 'Pwd_e5'", "Only root user can modify root user");
        assertPassword(UserIdentity.ROOT, "");
    }
}
