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

import org.apache.doris.analysis.ResourcePattern;
import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.UserDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AccessPrivilege;
import org.apache.doris.catalog.AccessPrivilegeWithCols;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.catalog.ComputeGroup;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.CreateRoleCommand;
import org.apache.doris.nereids.trees.plans.commands.CreateUserCommand;
import org.apache.doris.nereids.trees.plans.commands.DropRoleCommand;
import org.apache.doris.nereids.trees.plans.commands.DropUserCommand;
import org.apache.doris.nereids.trees.plans.commands.GrantResourcePrivilegeCommand;
import org.apache.doris.nereids.trees.plans.commands.GrantTablePrivilegeCommand;
import org.apache.doris.nereids.trees.plans.commands.RevokeResourcePrivilegeCommand;
import org.apache.doris.nereids.trees.plans.commands.ShowGrantsCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateUserInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.PrivInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.List;
import java.util.Optional;

public class CloudAuthTest extends TestWithFeService {

    private Auth auth;
    private AccessControllerManager accessManager;
    @Mocked
    public CloudEnv env;
    @Mocked
    private EditLog editLog;
    @Mocked
    private ConnectContext ctx;
    private CloudSystemInfoService systemInfoService = new CloudSystemInfoService();

    @Before
    public void setUp() throws NoSuchMethodException, SecurityException {
        auth = new Auth();
        accessManager = new AccessControllerManager(auth);
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessManager;

                env.getAuth();
                minTimes = 0;
                result = auth;

                env.getEditLog();
                minTimes = 0;
                result = editLog;

                editLog.logCreateUser((PrivInfo) any);
                minTimes = 0;

                ConnectContext.get();
                minTimes = 0;
                result = ctx;

                ctx.getQualifiedUser();
                minTimes = 0;
                result = "root";

                ctx.getRemoteIP();
                minTimes = 0;
                result = "192.168.1.1";

                ctx.getState();
                minTimes = 0;
                result = new QueryState();

                ctx.getCurrentUserIdentity();
                minTimes = 0;
                result = UserIdentity.createAnalyzedUserIdentWithIp("root", "%");

                Env.getCurrentSystemInfo();
                minTimes = 0;
                result = systemInfoService;
            }
        };

    }

    @Test
    public void testComputeGroup() throws Exception {
        UserIdentity userIdentity = new UserIdentity("testUser", "%");
        String computeGroup1 = "cg1";
        ResourcePattern resourcePattern = new ResourcePattern(computeGroup1, ResourceTypeEnum.CLUSTER);
        CreateRoleCommand createRoleCommand = new CreateRoleCommand(false, "role0", "");
        createRoleCommand.run(connectContext, null);
        List<AccessPrivilegeWithCols> usagePrivileges = Lists.newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.USAGE_PRIV));
        UserDesc userDesc = new UserDesc(userIdentity, "12345", true);

        // ------ grant|revoke cluster to|from user ------
        // 1. create user with no role
        CreateUserInfo createUserInfo = new CreateUserInfo(false, userDesc, "role0", null, "");
        try {
            createUserInfo.validate();
            auth.createUser(createUserInfo);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        // 2. grant usage_priv on cluster 'cg1' to 'testUser'@'%'
        GrantResourcePrivilegeCommand grantResourcePrivilegeCommand = new GrantResourcePrivilegeCommand(usagePrivileges,
                Optional.of(resourcePattern), Optional.empty(), Optional.of("role0"), Optional.of(userIdentity));
        try {
            grantResourcePrivilegeCommand.validate();
            auth.grantResourcePrivilegeCommand(grantResourcePrivilegeCommand);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(accessManager.checkCloudPriv(userIdentity, computeGroup1,
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 3. revoke usage_priv on cluster 'cg1' from 'testUser'@'%'
        RevokeResourcePrivilegeCommand revokeResourcePrivilegeCommand = new RevokeResourcePrivilegeCommand(usagePrivileges,
                Optional.of(resourcePattern), Optional.empty(), Optional.of("role0"), Optional.of(userIdentity));
        try {
            revokeResourcePrivilegeCommand.validate();
            auth.revokeResourcePrivilegeCommand(revokeResourcePrivilegeCommand);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertFalse(accessManager.checkCloudPriv(userIdentity, computeGroup1,
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));
        // 3.1 grant 'notBelongToResourcePrivileges' on cluster 'cg1' to 'testUser'@'%'
        for (int i = 0; i < Privilege.notBelongToResourcePrivileges.length; i++) {
            List<AccessPrivilegeWithCols> notAllowedPrivileges = Lists
                    .newArrayList(new AccessPrivilegeWithCols(
                    AccessPrivilege.fromName(Privilege.notBelongToResourcePrivileges[i].getName())));
            grantResourcePrivilegeCommand = new GrantResourcePrivilegeCommand(notAllowedPrivileges,
                    Optional.of(resourcePattern), Optional.empty(), Optional.of(""), Optional.of(userIdentity));
            try {
                grantResourcePrivilegeCommand.validate();
                Assert.fail(String.format("Can not grant/revoke %s to/from any other users or roles",
                        Privilege.notBelongToWorkloadGroupPrivileges[i]));
            } catch (AnalysisException e) {
                e.printStackTrace();
            }
        }
        // 4. drop user
        DropUserCommand dropUserCommand = new DropUserCommand(userIdentity, true);
        try {
            dropUserCommand.doRun(connectContext, null);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

        // ------ grant|revoke cluster to|from role ------
        // 1. create role
        String createRoleSql = "CREATE ROLE role1";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalRolePlan1 = nereidsParser.parseSingle(createRoleSql);
        Assertions.assertDoesNotThrow(() -> ((CreateRoleCommand) logicalRolePlan1).run(connectContext, null));
        String grantCgPrivilegeSql = "GRANT usage_priv ON cluster 'cg1' to role 'role1'";
        LogicalPlan plan = nereidsParser.parseSingle(grantCgPrivilegeSql);
        Assertions.assertTrue(plan instanceof GrantResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((GrantResourcePrivilegeCommand) plan).run(connectContext, null));

        // 2. create user with role
        UserIdentity userWithRole = new UserIdentity("test_user1", "%");
        userWithRole.analyze();
        CreateUserCommand createUserCommand1 = new CreateUserCommand(new CreateUserInfo(false, new UserDesc(userWithRole), "role1", null, null));
        CreateUserInfo info2 = createUserCommand1.getInfo();
        Assertions.assertDoesNotThrow(() -> info2.validate());
        Assertions.assertEquals(new String(info2.getRole()), "role1");
        Env.getCurrentEnv().getAuth().createUser(createUserCommand1.getInfo());
        Assert.assertTrue(accessManager.checkCloudPriv(userWithRole, "cg1",
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));
        Assert.assertFalse(accessManager.checkGlobalPriv(userWithRole, PrivPredicate.USAGE));

        // 3. revoke usage_priv on cluster 'cg1' from role 'role1'
        String revokeSql = "REVOKE USAGE_PRIV ON CLUSTER 'cg1' FROM ROLE 'role1';";
        LogicalPlan revokeplan1 = nereidsParser.parseSingle(revokeSql);
        Assertions.assertTrue(revokeplan1 instanceof RevokeResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((RevokeResourcePrivilegeCommand) revokeplan1).run(connectContext, null));
        // also revoke from user with this role
        Assert.assertFalse(accessManager.checkResourcePriv(userWithRole, "cg1", PrivPredicate.USAGE));
        Assert.assertFalse(accessManager.checkGlobalPriv(userWithRole, PrivPredicate.USAGE));

        // 4. drop user and role
        String dropUserSql = "DROP USER test_user1";
        LogicalPlan dropplan1 = nereidsParser.parseSingle(dropUserSql);
        Assertions.assertTrue(dropplan1 instanceof DropUserCommand);
        Assertions.assertDoesNotThrow(() -> ((DropUserCommand) dropplan1).run(connectContext, null));

        String dropRoleSql = "DROP Role role1";
        LogicalPlan dropRolePlan1 = nereidsParser.parseSingle(dropRoleSql);
        Assertions.assertTrue(dropRolePlan1 instanceof DropRoleCommand);
        Assertions.assertDoesNotThrow(() -> ((DropRoleCommand) dropRolePlan1).run(connectContext, null));


        // ------ grant|revoke any compute group to|from user ------
        // 1. create user with no role
        String createUserSql1 = "CREATE USER testUser";
        LogicalPlan createUserPlan1 = nereidsParser.parseSingle(createUserSql1);
        Assertions.assertTrue(createUserPlan1 instanceof CreateUserCommand);
        Assertions.assertDoesNotThrow(() -> ((CreateUserCommand) createUserPlan1).run(connectContext, null));

        // 2. grant usage_priv on cluster '*' to 'testUser'@'%'
        String grantAnyCgUser = "grant usage_priv on cluster '*' to 'testUser'@'%'";
        LogicalPlan grantAnyPlan1 = nereidsParser.parseSingle(grantAnyCgUser);
        Assertions.assertTrue(grantAnyPlan1 instanceof GrantResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((GrantResourcePrivilegeCommand) grantAnyPlan1).run(connectContext, null));
        Assert.assertTrue(accessManager.checkCloudPriv(userIdentity, "cg1",
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));
        // anyResource not belong to global auth
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.SHOW_RESOURCES));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.SHOW));

        // 3. revoke usage_priv on cluster '*' from 'testUser'@'%'
        String revokeAnyCgUser = "revoke usage_priv on cluster '*' from 'testUser'@'%'";
        LogicalPlan revokeAnyPlan1 = nereidsParser.parseSingle(revokeAnyCgUser);
        Assertions.assertTrue(revokeAnyPlan1 instanceof RevokeResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> (RevokeResourcePrivilegeCommand) revokeAnyPlan1).run(connectContext, null);
        Assert.assertFalse(accessManager.checkCloudPriv(userIdentity, "cg1",
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.SHOW_RESOURCES));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.SHOW));

        // 4. drop user
        String dropUserSql1 = "DROP USER testUser";
        LogicalPlan dropplan2 = nereidsParser.parseSingle(dropUserSql1);
        Assertions.assertTrue(dropplan2 instanceof DropUserCommand);
        Assertions.assertDoesNotThrow(() -> ((DropUserCommand) dropplan2).run(connectContext, null));

        // ------ grant|revoke any cluster to|from role ------
        // 1. create role
        String createRoleSql1 = "CREATE Role role1";
        LogicalPlan logicalRolePlan2 = nereidsParser.parseSingle(createRoleSql1);
        Assertions.assertDoesNotThrow(() -> ((CreateRoleCommand) logicalRolePlan2).run(connectContext, null));

        // grant usage_priv on cluster '*' to role 'role0'
        String grantAnySql = "grant usage_priv on cluster '*' to role 'role1'";
        LogicalPlan grantAnyPlan2 = nereidsParser.parseSingle(grantAnySql);
        Assertions.assertTrue(grantAnyPlan2 instanceof GrantResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((GrantResourcePrivilegeCommand) grantAnyPlan2).run(connectContext, null));

        // 2. create user with role
        String createUserSql3 = "CREATE USER testUser3 default role 'role1'";
        LogicalPlan createUserPlan3 = nereidsParser.parseSingle(createUserSql3);
        Assertions.assertTrue(createUserPlan3 instanceof CreateUserCommand);
        Assertions.assertDoesNotThrow(() -> ((CreateUserCommand) createUserPlan3).run(connectContext, null));

        Assert.assertTrue(accessManager.checkCloudPriv(new UserIdentity("testUser3", "%"), computeGroup1,
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));
        Assert.assertFalse(accessManager.checkGlobalPriv(new UserIdentity("testUser3", "%"), PrivPredicate.USAGE));

        // 3. revoke usage_priv on cluster '*' from role 'role1'
        String revokeCgRoleSql = "revoke usage_priv on cluster '*' from role 'role1'";
        LogicalPlan revokeCgRolePlan = nereidsParser.parseSingle(revokeCgRoleSql);
        Assertions.assertTrue(revokeCgRolePlan instanceof RevokeResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((RevokeResourcePrivilegeCommand) revokeCgRolePlan).run(connectContext, null));

        // also revoke from user with this role
        Assert.assertFalse(accessManager.checkResourcePriv(userIdentity, computeGroup1, PrivPredicate.USAGE));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 4. drop user and role
        String dropUserSql2 = "DROP USER testUser3";
        LogicalPlan dropplan3 = nereidsParser.parseSingle(dropUserSql2);
        Assertions.assertTrue(dropplan3 instanceof DropUserCommand);
        Assertions.assertDoesNotThrow(() -> ((DropUserCommand) dropplan3).run(connectContext, null));

        String dropRoleSql2 = "DROP Role role1";
        LogicalPlan dropRolePlan2 = nereidsParser.parseSingle(dropRoleSql2);
        Assertions.assertTrue(dropRolePlan2 instanceof DropRoleCommand);
        Assertions.assertDoesNotThrow(() -> ((DropRoleCommand) dropRolePlan2).run(connectContext, null));


        // ------ error case ------
        String createUserSql4 = "CREATE USER testUser4";
        LogicalPlan createUserPlan4 = nereidsParser.parseSingle(createUserSql4);
        Assertions.assertTrue(createUserPlan4 instanceof CreateUserCommand);
        Assertions.assertDoesNotThrow(() -> ((CreateUserCommand) createUserPlan4).run(connectContext, null));

        // 1. grant db table priv to cluster
        String errSql4 = "grant SELECT_PRIV on cluster 'test' to 'testUser4'";
        LogicalPlan errPlan4 = nereidsParser.parseSingle(errSql4);
        Assertions.assertTrue(errPlan4 instanceof GrantResourcePrivilegeCommand);
        Assertions.assertThrowsExactly(AnalysisException.class, () -> ((GrantResourcePrivilegeCommand) errPlan4).run(connectContext, null));

        // 2. grant cluster priv to db table
        String errSql5 = "grant usage_priv on db1.* to 'testUser4'";
        LogicalPlan errPlan5 = nereidsParser.parseSingle(errSql5);
        Assertions.assertTrue(errPlan5 instanceof GrantTablePrivilegeCommand);
        Assertions.assertThrowsExactly(AnalysisException.class, () -> ((GrantTablePrivilegeCommand) errPlan5).run(connectContext, null));

        // 3. grant cluster prov to role on db.table
        String errSql6 = "grant usage_priv on db1.* to ROLE 'test_role'";
        LogicalPlan errPlan6 = nereidsParser.parseSingle(errSql6);
        Assertions.assertTrue(errPlan6 instanceof GrantTablePrivilegeCommand);
        Assertions.assertThrowsExactly(AnalysisException.class, () -> ((GrantTablePrivilegeCommand) errPlan6).run(connectContext, null));

        // 4.drop user
        String dropUserSql5 = "DROP USER testUser4";
        LogicalPlan dropplan5 = nereidsParser.parseSingle(dropUserSql5);
        Assertions.assertTrue(dropplan5 instanceof DropUserCommand);
        Assertions.assertDoesNotThrow(() -> ((DropUserCommand) dropplan5).run(connectContext, null));
    }

    @Test
    public void testVirtualComputeGroup() throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        String createUserSql1 = "CREATE USER testUser";
        LogicalPlan createUserPlan1 = nereidsParser.parseSingle(createUserSql1);
        Assertions.assertTrue(createUserPlan1 instanceof CreateUserCommand);
        Assertions.assertDoesNotThrow(() -> ((CreateUserCommand) createUserPlan1).run(connectContext, null));
        // -------------------- case 1 -------------------------
        // grant usage_priv on cluster 'vcg' to 'testUser'@'%'
        String grantVcgUser = "grant usage_priv on cluster 'vcg' to 'testUser'@'%'";
        LogicalPlan grantAnyPlan1 = nereidsParser.parseSingle(grantVcgUser);
        Assertions.assertTrue(grantAnyPlan1 instanceof GrantResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((GrantResourcePrivilegeCommand) grantAnyPlan1).run(connectContext, null));
        Assert.assertTrue(accessManager.checkCloudPriv(new UserIdentity("testUser", "%"), "vcg",
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));
        // create vcg, sub cg(cg1, cg2), add to systemInfoService
        ComputeGroup vcg  = new ComputeGroup("vcg_id", "vcg", ComputeGroup.ComputeTypeEnum.VIRTUAL);
        vcg.setSubComputeGroups(Lists.newArrayList("cg2", "cg1"));
        systemInfoService.addComputeGroup("vcg_id", vcg);
        ComputeGroup.Policy policy = new ComputeGroup.Policy();
        policy.setActiveComputeGroup("cg1");
        policy.setStandbyComputeGroup("cg2");
        vcg.setPolicy(policy);

        Assert.assertTrue(accessManager.checkCloudPriv(new UserIdentity("testUser", "%"), "vcg",
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));

        // testUser has vcg, but not have cg1,cg2, he can use cg1,cg2
        Assert.assertTrue(accessManager.checkCloudPriv(new UserIdentity("testUser", "%"), "cg1",
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));
        Assert.assertTrue(accessManager.checkCloudPriv(new UserIdentity("testUser", "%"), "cg2",
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));
        ShowGrantsCommand sg = new ShowGrantsCommand(new UserIdentity("testUser", "%"), false);
        ShowResultSet showResultSet = sg.doRun(connectContext, null);
        // cluster field
        Assert.assertEquals("vcg: Cluster_usage_priv", showResultSet.getResultRows().get(0).get(10));
        // compute group field
        Assert.assertEquals("vcg: Cluster_usage_priv", showResultSet.getResultRows().get(0).get(14));

        // -------------------- case 2 -------------------------
        // grant usage_priv on cluster 'cg1' to 'testUser'@'%'
        String grantVcgUser2 = "grant usage_priv on cluster 'cg1' to 'testUser'@'%'";
        LogicalPlan grantAnyPlan2 = nereidsParser.parseSingle(grantVcgUser2);
        Assertions.assertTrue(grantAnyPlan2 instanceof GrantResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((GrantResourcePrivilegeCommand) grantAnyPlan2).run(connectContext, null));
        Assert.assertTrue(accessManager.checkCloudPriv(new UserIdentity("testUser", "%"), "cg1",
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));

        // testUser can use cg1, because he has vcg,cg1 auth
        Assert.assertTrue(accessManager.checkCloudPriv(new UserIdentity("testUser", "%"), "cg1",
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));
        showResultSet = sg.doRun(connectContext, null);
        // cluster field
        Assert.assertEquals("cg1: Cluster_usage_priv; vcg: Cluster_usage_priv",
                showResultSet.getResultRows().get(0).get(10));
        // compute group field
        Assert.assertEquals("cg1: Cluster_usage_priv; vcg: Cluster_usage_priv",
                showResultSet.getResultRows().get(0).get(14));

        // revoke cg1 from test user
        String revokeCgSql1 = "revoke usage_priv on cluster 'cg1' from 'testUser'@'%'";
        LogicalPlan revokeplan1 = nereidsParser.parseSingle(revokeCgSql1);
        Assertions.assertTrue(revokeplan1 instanceof RevokeResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((RevokeResourcePrivilegeCommand) revokeplan1).run(connectContext, null));

        // testUser can use cg1, because he has vcg auth
        Assert.assertTrue(accessManager.checkCloudPriv(new UserIdentity("testUser", "%"), "cg1",
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));
        showResultSet = sg.doRun(connectContext, null);
        // cluster field
        Assert.assertEquals("vcg: Cluster_usage_priv",
                showResultSet.getResultRows().get(0).get(10));
        // compute group field
        Assert.assertEquals("vcg: Cluster_usage_priv",
                showResultSet.getResultRows().get(0).get(14));

        // grant cg2 to user
        String grantVcgUser3 = "grant usage_priv on cluster 'cg2' to 'testUser'@'%'";
        LogicalPlan grantAnyPlan3 = nereidsParser.parseSingle(grantVcgUser3);
        Assertions.assertTrue(grantAnyPlan3 instanceof GrantResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((GrantResourcePrivilegeCommand) grantAnyPlan3).run(connectContext, null));
        Assert.assertTrue(accessManager.checkCloudPriv(new UserIdentity("testUser", "%"), "cg1",
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));

        // revoke vcg from test user
        String revokeCgSql2 = "revoke usage_priv on cluster 'vcg' from 'testUser'";
        LogicalPlan revokeplan2 = nereidsParser.parseSingle(revokeCgSql2);
        Assertions.assertTrue(revokeplan2 instanceof RevokeResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((RevokeResourcePrivilegeCommand) revokeplan2).run(connectContext, null));

        // currently, user has cg2 auth, not have vcg auth
        Assert.assertFalse(accessManager.checkCloudPriv(new UserIdentity("testUser", "%"), "vcg",
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));

        // testUser has cg2, but not have vcg, he can use cg2, can't use cg1, vcg
        Assert.assertFalse(accessManager.checkCloudPriv(new UserIdentity("testUser", "%"), "cg1",
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));
        Assert.assertTrue(accessManager.checkCloudPriv(new UserIdentity("testUser", "%"), "cg2",
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));
        showResultSet = sg.doRun(connectContext, null);
        // cluster field
        Assert.assertEquals("cg2: Cluster_usage_priv",
                showResultSet.getResultRows().get(0).get(10));
        // compute group field
        Assert.assertEquals("cg2: Cluster_usage_priv",
                showResultSet.getResultRows().get(0).get(14));
        // revoke cg2 from user

        String revokeCgSql3 = "revoke usage_priv on cluster 'cg2' from 'testUser'";
        LogicalPlan revokeplan3 = nereidsParser.parseSingle(revokeCgSql3);
        Assertions.assertTrue(revokeplan3 instanceof RevokeResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((RevokeResourcePrivilegeCommand) revokeplan3).run(connectContext, null));

        // revoke vcg from user, he can't use it
        String revokeCgSql4 = "revoke usage_priv on cluster 'vcg' from 'testUser'";
        LogicalPlan revokeplan4 = nereidsParser.parseSingle(revokeCgSql4);
        Assertions.assertTrue(revokeplan4 instanceof RevokeResourcePrivilegeCommand);
        Assertions.assertDoesNotThrow(() -> ((RevokeResourcePrivilegeCommand) revokeplan4).run(connectContext, null));

        Assert.assertFalse(accessManager.checkCloudPriv(new UserIdentity("testUser", "%"), "vcg",
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));

        // testUser after revoke vcg, not have cg1,cg2, it should can use, vcg,cg1,cg2
        Assert.assertFalse(accessManager.checkCloudPriv(new UserIdentity("testUser", "%"), "cg1",
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));
        Assert.assertFalse(accessManager.checkCloudPriv(new UserIdentity("testUser", "%"), "cg2",
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));
        showResultSet = sg.doRun(connectContext, null);
        // cluster field
        Assert.assertEquals("\\N", showResultSet.getResultRows().get(0).get(10));
        // compute group field
        Assert.assertEquals("\\N", showResultSet.getResultRows().get(0).get(14));

        // drop user
        String dropUserSql5 = "DROP USER testUser";
        LogicalPlan dropplan5 = nereidsParser.parseSingle(dropUserSql5);
        Assertions.assertTrue(dropplan5 instanceof DropUserCommand);
        Assertions.assertDoesNotThrow(() -> ((DropUserCommand) dropplan5).run(connectContext, null));
    }
}
