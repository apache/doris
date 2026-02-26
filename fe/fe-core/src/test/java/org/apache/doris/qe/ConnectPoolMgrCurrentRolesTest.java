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

package org.apache.doris.qe;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.PrivilegeContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class ConnectPoolMgrCurrentRolesTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createRole("grant_role");
        grantPriv("GRANT GRANT_PRIV ON *.*.* TO ROLE 'grant_role';");
        addUser("u1", true);
        addUser("u2", true);
    }

    @Test
    public void testListConnectionForRpcGrantRoleCanViewOthers() throws IOException {
        UserIdentity targetUser = UserIdentity.createAnalyzedUserIdentWithIp("u1", "%");
        UserIdentity callerUser = UserIdentity.createAnalyzedUserIdentWithIp("u2", "%");

        ConnectPoolMgr connectPoolMgr = new ConnectPoolMgr(10);
        ConnectContext targetCtx = TestWithFeService.createCtx(targetUser, "127.0.0.1");
        targetCtx.setConnectionId(1001);
        targetCtx.setCommand(MysqlCommand.COM_QUERY);
        targetCtx.setStartTime();
        connectPoolMgr.registerConnection(targetCtx);

        List<List<String>> denied = connectPoolMgr.listConnectionForRpc(callerUser, null,
                false, Optional.empty());
        Assertions.assertTrue(denied.isEmpty());

        List<List<String>> allowedWithGrantRole = connectPoolMgr.listConnectionForRpc(callerUser,
                Sets.newHashSet("grant_role"), false, Optional.empty());
        Assertions.assertEquals(1, allowedWithGrantRole.size());

        connectPoolMgr.unregisterConnection(targetCtx);
        targetCtx.cleanup();
        connectContext.setThreadLocalInfo();
    }

    @Test
    public void testListConnectionForRpcAllowAdminReadOnlyRole() throws IOException {
        UserIdentity targetUser = UserIdentity.createAnalyzedUserIdentWithIp("u1", "%");
        UserIdentity callerUser = UserIdentity.createAnalyzedUserIdentWithIp("u2", "%");

        ConnectPoolMgr connectPoolMgr = new ConnectPoolMgr(10);
        ConnectContext targetCtx = TestWithFeService.createCtx(targetUser, "127.0.0.1");
        targetCtx.setConnectionId(1002);
        targetCtx.setCommand(MysqlCommand.COM_QUERY);
        targetCtx.setStartTime();
        connectPoolMgr.registerConnection(targetCtx);

        List<List<String>> denied = connectPoolMgr.listConnectionForRpc(callerUser, null,
                false, Optional.empty());
        Assertions.assertTrue(denied.isEmpty());

        List<List<String>> allowed = connectPoolMgr.listConnectionForRpc(callerUser,
                Sets.newHashSet("admin_readonly"), false, Optional.empty());
        Assertions.assertEquals(1, allowed.size());

        connectPoolMgr.unregisterConnection(targetCtx);
        targetCtx.cleanup();
        connectContext.setThreadLocalInfo();
    }

    @Test
    public void testListConnectionLocalOnlyContainsCurrentUser() throws IOException {
        UserIdentity targetUser = UserIdentity.createAnalyzedUserIdentWithIp("u1", "%");
        UserIdentity callerUser = UserIdentity.createAnalyzedUserIdentWithIp("u2", "%");

        ConnectPoolMgr connectPoolMgr = new ConnectPoolMgr(10);
        ConnectContext targetCtx = TestWithFeService.createCtx(targetUser, "127.0.0.1");
        targetCtx.setConnectionId(1003);
        targetCtx.setCommand(MysqlCommand.COM_QUERY);
        targetCtx.setStartTime();
        connectPoolMgr.registerConnection(targetCtx);

        ConnectContext callerCtx = TestWithFeService.createCtx(callerUser, "127.0.0.1");
        callerCtx.setConnectionId(1004);
        callerCtx.setCommand(MysqlCommand.COM_QUERY);
        callerCtx.setStartTime();
        connectPoolMgr.registerConnection(callerCtx);

        callerCtx.setThreadLocalInfo();
        List<ConnectContext.ThreadInfo> rows = connectPoolMgr.listConnection(callerUser.getQualifiedUser(), false);
        Assertions.assertEquals(1, rows.size());

        connectPoolMgr.unregisterConnection(targetCtx);
        connectPoolMgr.unregisterConnection(callerCtx);
        targetCtx.cleanup();
        callerCtx.cleanup();
        connectContext.setThreadLocalInfo();
    }

    @Test
    public void testListConnectionLocalAllowAdminReadOnlyRole() throws IOException {
        UserIdentity targetUser = UserIdentity.createAnalyzedUserIdentWithIp("u1", "%");
        UserIdentity callerUser = UserIdentity.createAnalyzedUserIdentWithIp("u2", "%");

        ConnectPoolMgr connectPoolMgr = new ConnectPoolMgr(10);
        ConnectContext targetCtx = TestWithFeService.createCtx(targetUser, "127.0.0.1");
        targetCtx.setConnectionId(1005);
        targetCtx.setCommand(MysqlCommand.COM_QUERY);
        targetCtx.setStartTime();
        connectPoolMgr.registerConnection(targetCtx);

        ConnectContext callerCtx = TestWithFeService.createCtx(callerUser, "127.0.0.1");
        callerCtx.setConnectionId(1006);
        callerCtx.setCommand(MysqlCommand.COM_QUERY);
        callerCtx.setStartTime();
        callerCtx.setCurrentRoles(Sets.newHashSet("admin_readonly"));
        connectPoolMgr.registerConnection(callerCtx);

        callerCtx.setThreadLocalInfo();
        List<ConnectContext.ThreadInfo> rows = connectPoolMgr.listConnection(callerUser.getQualifiedUser(), false);
        Assertions.assertEquals(2, rows.size());

        connectPoolMgr.unregisterConnection(targetCtx);
        connectPoolMgr.unregisterConnection(callerCtx);
        targetCtx.cleanup();
        callerCtx.cleanup();
        connectContext.setThreadLocalInfo();
    }

    @Test
    public void testCheckGlobalPrivExplicitRolesWithoutThreadContext() {
        UserIdentity callerUser = UserIdentity.createAnalyzedUserIdentWithIp("u2", "%");

        ConnectContext.remove();
        Assertions.assertTrue(Env.getCurrentEnv().getAccessManager()
                .checkGlobalPriv(PrivilegeContext.of(callerUser, Sets.newHashSet("grant_role")),
                        PrivPredicate.GRANT));
        Assertions.assertFalse(Env.getCurrentEnv().getAccessManager()
                .checkGlobalPriv(PrivilegeContext.of(callerUser, Collections.singleton("no_such_role")),
                        PrivPredicate.GRANT));

        connectContext.setThreadLocalInfo();
    }
}
