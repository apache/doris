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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.CreateRoleStmt;
import org.apache.doris.analysis.CreateUserStmt;
import org.apache.doris.analysis.DropRoleStmt;
import org.apache.doris.analysis.DropUserStmt;
import org.apache.doris.analysis.GrantStmt;
import org.apache.doris.analysis.ResourcePattern;
import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.RevokeStmt;
import org.apache.doris.analysis.ShowGrantsStmt;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AccessPrivilege;
import org.apache.doris.catalog.AccessPrivilegeWithCols;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.catalog.ComputeGroup;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.PrivInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.ShowExecutor;
import org.apache.doris.qe.ShowResultSet;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class CloudAuthTest {

    private Auth auth;
    private AccessControllerManager accessManager;
    @Mocked
    public CloudEnv env;
    @Mocked
    private Analyzer analyzer;
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

                analyzer.getDefaultCatalog();
                minTimes = 0;
                result = InternalCatalog.INTERNAL_CATALOG_NAME;

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

                // systemInfoService.addComputeGroup(anyString, (ComputeGroup) any);
                // minTimes = 0;
            }
        };

    }

    private void dropUser(UserIdentity userIdentity) throws UserException {
        DropUserStmt dropUserStmt = new DropUserStmt(userIdentity);
        dropUserStmt.analyze(analyzer);
        auth.dropUser(dropUserStmt);
    }

    public ShowResultSet testShowGrants(UserIdentity userIdent) throws AnalysisException {
        ShowGrantsStmt stmt = new ShowGrantsStmt(userIdent, false);
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        return executor.execute();
    }

    @Test
    public void testComputeGroup() throws UserException {
        UserIdentity userIdentity = new UserIdentity("testUser", "%");
        String role = "role0";
        String computeGroup1 = "cg1";
        ResourcePattern resourcePattern = new ResourcePattern(computeGroup1, ResourceTypeEnum.CLUSTER);
        String anyResource = "%";
        ResourcePattern anyResourcePattern = new ResourcePattern(anyResource, ResourceTypeEnum.CLUSTER);
        List<AccessPrivilegeWithCols> usagePrivileges = Lists
                .newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.USAGE_PRIV));
        UserDesc userDesc = new UserDesc(userIdentity, "12345", true);

        // ------ grant|revoke cluster to|from user ------
        // 1. create user with no role
        CreateUserStmt createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
            auth.createUser(createUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // 2. grant usage_priv on cluster 'cg1' to 'testUser'@'%'
        GrantStmt grantStmt = new GrantStmt(userIdentity, null, resourcePattern, usagePrivileges,
                ResourceTypeEnum.CLUSTER);
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(accessManager.checkCloudPriv(userIdentity, computeGroup1,
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 3. revoke usage_priv on cluster 'cg1' from 'testUser'@'%'
        RevokeStmt revokeStmt = new RevokeStmt(userIdentity, null, resourcePattern, usagePrivileges,
                ResourceTypeEnum.CLUSTER);
        try {
            revokeStmt.analyze(analyzer);
            auth.revoke(revokeStmt);
        } catch (UserException e) {
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
            grantStmt = new GrantStmt(userIdentity, null, resourcePattern, notAllowedPrivileges,
                ResourceTypeEnum.GENERAL);
            try {
                grantStmt.analyze(analyzer);
                Assert.fail(String.format("Can not grant/revoke %s to/from any other users or roles",
                        Privilege.notBelongToWorkloadGroupPrivileges[i]));
            } catch (UserException e) {
                e.printStackTrace();
            }
        }
        // 4. drop user
        DropUserStmt dropUserStmt = new DropUserStmt(userIdentity);
        try {
            dropUserStmt.analyze(analyzer);
            auth.dropUser(dropUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // ------ grant|revoke resource to|from role ------
        // 1. create role
        CreateRoleStmt roleStmt = new CreateRoleStmt(role);
        try {
            roleStmt.analyze(analyzer);
            auth.createRole(roleStmt);
        } catch (UserException e1) {
            e1.printStackTrace();
            Assert.fail();
        }
        // grant usage_priv on cluster 'cg1' to role 'role0'
        grantStmt = new GrantStmt(null, role, resourcePattern, usagePrivileges, ResourceTypeEnum.CLUSTER);
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        // 2. create user with role
        createUserStmt = new CreateUserStmt(false, userDesc, role);
        try {
            createUserStmt.analyze(analyzer);
            auth.createUser(createUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(accessManager.checkCloudPriv(userIdentity, computeGroup1,
                 PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 3. revoke usage_priv on cluster 'cg1' from role 'role0'
        revokeStmt = new RevokeStmt(null, role, resourcePattern, usagePrivileges, ResourceTypeEnum.CLUSTER);
        try {
            revokeStmt.analyze(analyzer);
            auth.revoke(revokeStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        // also revoke from user with this role
        Assert.assertFalse(accessManager.checkResourcePriv(userIdentity, computeGroup1, PrivPredicate.USAGE));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 4. drop user and role
        dropUserStmt = new DropUserStmt(userIdentity);
        try {
            dropUserStmt.analyze(analyzer);
            auth.dropUser(dropUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        DropRoleStmt dropRoleStmt = new DropRoleStmt(role);
        try {
            dropRoleStmt.analyze(analyzer);
            auth.dropRole(dropRoleStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // ------ grant|revoke any compute group to|from user ------
        // 1. create user with no role
        createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
            auth.createUser(createUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // 2. grant usage_priv on cluster '*' to 'testUser'@'%'
        grantStmt = new GrantStmt(userIdentity, null, anyResourcePattern, usagePrivileges, ResourceTypeEnum.CLUSTER);
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(accessManager.checkCloudPriv(userIdentity, computeGroup1,
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));
        // anyResource not belong to global auth
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.SHOW_RESOURCES));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.SHOW));

        // 3. revoke usage_priv on cluster '*' from 'testUser'@'%'
        revokeStmt = new RevokeStmt(userIdentity, null, anyResourcePattern, usagePrivileges, ResourceTypeEnum.CLUSTER);
        try {
            revokeStmt.analyze(analyzer);
            auth.revoke(revokeStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertFalse(accessManager.checkCloudPriv(userIdentity, computeGroup1,
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.SHOW_RESOURCES));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.SHOW));

        // 4. drop user
        dropUserStmt = new DropUserStmt(userIdentity);
        try {
            dropUserStmt.analyze(analyzer);
            auth.dropUser(dropUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // ------ grant|revoke any cluster to|from role ------
        // 1. create role
        roleStmt = new CreateRoleStmt(role);
        try {
            roleStmt.analyze(analyzer);
            auth.createRole(roleStmt);
        } catch (UserException e1) {
            e1.printStackTrace();
            Assert.fail();
        }
        // grant usage_priv on cluster '*' to role 'role0'
        grantStmt = new GrantStmt(null, role, anyResourcePattern, usagePrivileges, ResourceTypeEnum.CLUSTER);
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        // 2. create user with role
        createUserStmt = new CreateUserStmt(false, userDesc, role);
        try {
            createUserStmt.analyze(analyzer);
            auth.createUser(createUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertTrue(accessManager.checkCloudPriv(userIdentity, computeGroup1,
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 3. revoke usage_priv on cluster '*' from role 'role0'
        revokeStmt = new RevokeStmt(null, role, anyResourcePattern, usagePrivileges, ResourceTypeEnum.CLUSTER);
        try {
            revokeStmt.analyze(analyzer);
            auth.revoke(revokeStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        // also revoke from user with this role
        Assert.assertFalse(accessManager.checkResourcePriv(userIdentity, computeGroup1, PrivPredicate.USAGE));
        Assert.assertFalse(accessManager.checkGlobalPriv(userIdentity, PrivPredicate.USAGE));

        // 4. drop user and role
        dropUserStmt = new DropUserStmt(userIdentity);
        try {
            dropUserStmt.analyze(analyzer);
            auth.dropUser(dropUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        dropRoleStmt = new DropRoleStmt(role);
        try {
            dropRoleStmt.analyze(analyzer);
            auth.dropRole(dropRoleStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // ------ error case ------
        boolean hasException = false;
        createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
            auth.createUser(createUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // 1. grant db table priv to cluster
        List<AccessPrivilegeWithCols> privileges = Lists
                .newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.SELECT_PRIV));
        grantStmt = new GrantStmt(userIdentity, null, resourcePattern, privileges, ResourceTypeEnum.GENERAL);
        hasException = false;
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e) {
            e.printStackTrace();
            hasException = true;
        }
        Assert.assertTrue(hasException);

        // 2. grant cluster priv to db table
        TablePattern tablePattern = new TablePattern("db1", "*");
        GrantStmt grantStmt2 = new GrantStmt(userIdentity, null, tablePattern, usagePrivileges);
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "Can not grant/revoke Usage_priv to/from any other users or roles",
                () -> grantStmt2.analyze(analyzer));

        // 3. grant cluster prov to role on db.table
        tablePattern = new TablePattern("db1", "*");
        GrantStmt grantStmt3 = new GrantStmt(userIdentity, "test_role", tablePattern, usagePrivileges);
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "Can not grant/revoke Usage_priv to/from any other users or roles",
                () -> grantStmt3.analyze(analyzer));

        // 4.drop user
        dropUser(userIdentity);
    }

    @Test
    public void testVirtualComputeGroup() throws UserException {
        UserIdentity userIdentity = new UserIdentity("testUser", "%");
        // String role = "role0";
        String computeGroup1 = "cg1";
        ResourcePattern resourcePattern1 = new ResourcePattern(computeGroup1, ResourceTypeEnum.CLUSTER);
        String computeGroup2 = "cg2";
        ResourcePattern resourcePattern2 = new ResourcePattern(computeGroup2, ResourceTypeEnum.CLUSTER);
        String virtualComputeGroup = "vcg";
        ResourcePattern resourcePatternVcg = new ResourcePattern(virtualComputeGroup, ResourceTypeEnum.CLUSTER);

        List<AccessPrivilegeWithCols> usagePrivileges = Lists
                .newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.USAGE_PRIV));

        UserDesc userDesc = new UserDesc(userIdentity, "12345", true);

        // create user with no role
        CreateUserStmt createUserStmt = new CreateUserStmt(false, userDesc, null);
        try {
            createUserStmt.analyze(analyzer);
            auth.createUser(createUserStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // -------------------- case 1 -------------------------
        // grant usage_priv on cluster 'vcg' to 'testUser'@'%'
        GrantStmt grantStmt = new GrantStmt(userIdentity, null, resourcePatternVcg, usagePrivileges,
                ResourceTypeEnum.CLUSTER);
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        // create vcg, sub cg(cg1, cg2), add to systemInfoService
        ComputeGroup vcg  = new ComputeGroup("vcg_id", virtualComputeGroup, ComputeGroup.ComputeTypeEnum.VIRTUAL);
        vcg.setSubComputeGroups(Lists.newArrayList(computeGroup2, computeGroup1));
        systemInfoService.addComputeGroup(virtualComputeGroup, vcg);
        ComputeGroup.Policy policy = new ComputeGroup.Policy();
        policy.setActiveComputeGroup(computeGroup1);
        policy.setStandbyComputeGroup(computeGroup2);
        vcg.setPolicy(policy);

        Assert.assertTrue(accessManager.checkCloudPriv(userIdentity, virtualComputeGroup,
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));

        // testUser has vcg, but not have cg1,cg2, he can use cg1,cg2
        Assert.assertTrue(accessManager.checkCloudPriv(userIdentity, computeGroup1,
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));
        Assert.assertTrue(accessManager.checkCloudPriv(userIdentity, computeGroup2,
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));
        ShowResultSet showResultSet = testShowGrants(userIdentity);
        // cluster field
        Assert.assertEquals("vcg: Cluster_usage_priv", showResultSet.getResultRows().get(0).get(10));
        // compute group field
        Assert.assertEquals("vcg: Cluster_usage_priv", showResultSet.getResultRows().get(0).get(14));

        // -------------------- case 2 -------------------------
        // grant usage_priv on cluster 'cg1' to 'testUser'@'%'
        grantStmt = new GrantStmt(userIdentity, null, resourcePattern1, usagePrivileges,
            ResourceTypeEnum.CLUSTER);
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        // testUser can use cg1, because he has vcg,cg1 auth
        Assert.assertTrue(accessManager.checkCloudPriv(userIdentity, computeGroup1,
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));
        showResultSet = testShowGrants(userIdentity);
        // cluster field
        Assert.assertEquals("cg1: Cluster_usage_priv; vcg: Cluster_usage_priv",
                showResultSet.getResultRows().get(0).get(10));
        // compute group field
        Assert.assertEquals("cg1: Cluster_usage_priv; vcg: Cluster_usage_priv",
                showResultSet.getResultRows().get(0).get(14));

        // revoke cg1 from test user
        RevokeStmt revokeStmt = new RevokeStmt(userIdentity, null, resourcePattern1, usagePrivileges,
                ResourceTypeEnum.CLUSTER);
        try {
            revokeStmt.analyze(analyzer);
            auth.revoke(revokeStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        // testUser can use cg1, because he has vcg auth
        Assert.assertTrue(accessManager.checkCloudPriv(userIdentity, computeGroup1,
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));
        showResultSet = testShowGrants(userIdentity);
        // cluster field
        Assert.assertEquals("vcg: Cluster_usage_priv",
                showResultSet.getResultRows().get(0).get(10));
        // compute group field
        Assert.assertEquals("vcg: Cluster_usage_priv",
                showResultSet.getResultRows().get(0).get(14));

        // grant cg2 to user
        grantStmt = new GrantStmt(userIdentity, null, resourcePattern2, usagePrivileges,
                ResourceTypeEnum.CLUSTER);
        try {
            grantStmt.analyze(analyzer);
            auth.grant(grantStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // revoke vcg from test user
        revokeStmt = new RevokeStmt(userIdentity, null, resourcePatternVcg, usagePrivileges,
                ResourceTypeEnum.CLUSTER);
        try {
            revokeStmt.analyze(analyzer);
            auth.revoke(revokeStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // currently, user has cg2 auth, not have vcg auth
        Assert.assertFalse(accessManager.checkCloudPriv(userIdentity, virtualComputeGroup,
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));

        // testUser has cg2, but not have vcg, he can use cg2, can't use cg1, vcg
        Assert.assertFalse(accessManager.checkCloudPriv(userIdentity, computeGroup1,
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));
        Assert.assertTrue(accessManager.checkCloudPriv(userIdentity, computeGroup2,
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));
        showResultSet = testShowGrants(userIdentity);
        // cluster field
        Assert.assertEquals("cg2: Cluster_usage_priv",
                showResultSet.getResultRows().get(0).get(10));
        // compute group field
        Assert.assertEquals("cg2: Cluster_usage_priv",
                showResultSet.getResultRows().get(0).get(14));
        // revoke cg2 from user
        revokeStmt = new RevokeStmt(userIdentity, null, resourcePattern2, usagePrivileges,
                ResourceTypeEnum.CLUSTER);
        try {
            revokeStmt.analyze(analyzer);
            auth.revoke(revokeStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // revoke vcg from user, he can't use it
        revokeStmt = new RevokeStmt(userIdentity, null, resourcePatternVcg, usagePrivileges,
                ResourceTypeEnum.CLUSTER);
        try {
            revokeStmt.analyze(analyzer);
            auth.revoke(revokeStmt);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
        Assert.assertFalse(accessManager.checkCloudPriv(userIdentity, virtualComputeGroup,
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));

        // testUser after revoke vcg, not have cg1,cg2, it should can use, vcg,cg1,cg2
        Assert.assertFalse(accessManager.checkCloudPriv(userIdentity, computeGroup1,
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));
        Assert.assertFalse(accessManager.checkCloudPriv(userIdentity, computeGroup2,
                PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER));
        showResultSet = testShowGrants(userIdentity);
        // cluster field
        Assert.assertEquals("\\N", showResultSet.getResultRows().get(0).get(10));
        // compute group field
        Assert.assertEquals("\\N", showResultSet.getResultRows().get(0).get(14));

        // drop user
        dropUser(userIdentity);
    }
}
