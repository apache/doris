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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateRoleCommand;
import org.apache.doris.nereids.trees.plans.commands.DropRoleCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectPoolMgr;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

public class AdminReadOnlyRoleTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
    }

    @Test
    public void testAdminReadOnlyPrivileges() throws Exception {
        addUser("u_ro", true);
        grantRole("GRANT 'admin_readonly' TO 'u_ro'@'%'");

        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("u_ro", "%");
        AccessControllerManager accessManager = Env.getCurrentEnv().getAccessManager();

        PrivilegeContext privilegeContext = PrivilegeContext.of(user);
        Assertions.assertTrue(accessManager.checkDbPriv(privilegeContext, InternalCatalog.INTERNAL_CATALOG_NAME,
                "test", PrivPredicate.SHOW));
        Assertions.assertTrue(accessManager.checkResourcePriv(privilegeContext, "res1", PrivPredicate.SHOW_RESOURCES));
        Assertions.assertTrue(accessManager.checkWorkloadGroupPriv(
                privilegeContext, "wg1", PrivPredicate.SHOW_WORKLOAD_GROUP));
        Assertions.assertFalse(accessManager.checkGlobalPriv(privilegeContext, PrivPredicate.ADMIN));
    }

    @Test
    public void testAdminReadOnlyProcessListForRpc() throws Exception {
        addUser("u_ro_process", true);
        grantRole("GRANT 'admin_readonly' TO 'u_ro_process'@'%'");
        addUser("u_process_target", true);

        UserIdentity caller = UserIdentity.createAnalyzedUserIdentWithIp("u_ro_process", "%");
        UserIdentity target = UserIdentity.createAnalyzedUserIdentWithIp("u_process_target", "%");

        ConnectPoolMgr connectPoolMgr = new ConnectPoolMgr(10);
        ConnectContext targetCtx = TestWithFeService.createCtx(target, "127.0.0.1");
        targetCtx.setConnectionId(2001);
        targetCtx.setCommand(MysqlCommand.COM_QUERY);
        targetCtx.setStartTime();
        connectPoolMgr.registerConnection(targetCtx);

        List<List<String>> rows = connectPoolMgr.listConnectionForRpc(caller, null,
                false, Optional.empty());
        Assertions.assertEquals(1, rows.size());

        connectPoolMgr.unregisterConnection(targetCtx);
        targetCtx.cleanup();
        connectContext.setThreadLocalInfo();
    }

    @Test
    public void testAdminReadOnlyProcessListLocal() throws Exception {
        addUser("u_ro_process_local", true);
        grantRole("GRANT 'admin_readonly' TO 'u_ro_process_local'@'%'");
        addUser("u_process_target_local", true);

        UserIdentity caller = UserIdentity.createAnalyzedUserIdentWithIp("u_ro_process_local", "%");
        UserIdentity target = UserIdentity.createAnalyzedUserIdentWithIp("u_process_target_local", "%");

        ConnectPoolMgr connectPoolMgr = new ConnectPoolMgr(10);
        ConnectContext targetCtx = TestWithFeService.createCtx(target, "127.0.0.1");
        targetCtx.setConnectionId(2002);
        targetCtx.setCommand(MysqlCommand.COM_QUERY);
        targetCtx.setStartTime();
        connectPoolMgr.registerConnection(targetCtx);

        ConnectContext callerCtx = TestWithFeService.createCtx(caller, "127.0.0.1");
        callerCtx.setConnectionId(2003);
        callerCtx.setCommand(MysqlCommand.COM_QUERY);
        callerCtx.setStartTime();
        connectPoolMgr.registerConnection(callerCtx);

        callerCtx.setThreadLocalInfo();
        List<ConnectContext.ThreadInfo> rows = connectPoolMgr.listConnection(caller.getQualifiedUser(), false);
        Assertions.assertEquals(2, rows.size());

        connectPoolMgr.unregisterConnection(targetCtx);
        connectPoolMgr.unregisterConnection(callerCtx);
        targetCtx.cleanup();
        callerCtx.cleanup();
        connectContext.setThreadLocalInfo();
    }

    @Test
    public void testAdminReadOnlyProtected() {
        NereidsParser parser = new NereidsParser();

        LogicalPlan create = parser.parseSingle("CREATE ROLE admin_readonly");
        Assertions.assertTrue(create instanceof CreateRoleCommand);
        Assertions.assertThrows(AnalysisException.class, () -> ((CreateRoleCommand) create).run(connectContext, null));

        LogicalPlan drop = parser.parseSingle("DROP ROLE admin_readonly");
        Assertions.assertTrue(drop instanceof DropRoleCommand);
        Assertions.assertThrows(AnalysisException.class, () -> ((DropRoleCommand) drop).run(connectContext, null));
    }
}
