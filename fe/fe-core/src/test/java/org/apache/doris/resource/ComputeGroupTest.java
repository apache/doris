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

package org.apache.doris.resource;

import org.apache.doris.analysis.SetUserPropertyStmt;
import org.apache.doris.analysis.UserDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.RandomIdentifierGenerator;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.FederationBackendPolicy;
import org.apache.doris.load.loadv2.BrokerLoadJob;
import org.apache.doris.load.routineload.KafkaRoutineLoadJob;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.load.routineload.RoutineLoadManager;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.CreateUserCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateUserInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.computegroup.AllBackendComputeGroup;
import org.apache.doris.resource.computegroup.CloudComputeGroup;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.resource.computegroup.ComputeGroupMgr;
import org.apache.doris.resource.computegroup.MergedComputeGroup;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

public class ComputeGroupTest {
    private static final Logger LOG = LogManager.getLogger(ComputeGroupTest.class);

    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    private static String runningDirBase = "fe";
    private static ConnectContext connectContext;

    private Auth auth;
    @Mocked
    public Env env;
    @Mocked
    AccessControllerManager accessManager;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @Before
    public void setUp() throws MetaNotFoundException {
        auth = new Auth();
        accessManager = new AccessControllerManager(auth);

        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAuth();
                minTimes = 0;
                result = auth;

                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;

                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.GRANT);
                minTimes = 0;
                result = true;

                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.OPERATOR);
                minTimes = 0;
                result = true;

                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.CREATE);
                minTimes = 0;
                result = true;
            }
        };
    }

    // @AfterClass
    // public static void tearDown() {
    //     UtFrameUtils.cleanDorisFeDir(runningDirBase);
    // }

    private static void setProperty(String sql) throws Exception {
        SetUserPropertyStmt setUserPropertyStmt = (SetUserPropertyStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Env.getCurrentEnv().getAuth().updateUserProperty(setUserPropertyStmt);
    }

    @Test
    public void testGetSetResourceTagFromAuth() throws Exception {
        new Expectations() {
            {
                Env.getCurrentEnv().getComputeGroupMgr();
                minTimes = 0;
                result = new ComputeGroupMgr(null);
            }
        };

            // 1 get no user
            {
                String invalidUser = RandomIdentifierGenerator.generateRandomIdentifier(8);
                ComputeGroup cg1 = auth.getComputeGroup(invalidUser);
                Assert.assertTrue(cg1 == ComputeGroup.INVALID_COMPUTE_GROUP);
            }
            // 2 get a non-admin user without resource tag
            {
                String nonAdminUserStr = "non_admin_user";
                UserIdentity nonAdminUser = new UserIdentity(nonAdminUserStr, "%");
                UserDesc nonAdminUserDesc = new UserDesc(nonAdminUser, "12345", true);

                CreateUserCommand createUserCommand = new CreateUserCommand(new CreateUserInfo(nonAdminUserDesc));
                createUserCommand.getInfo().validate();
                auth.createUser(createUserCommand.getInfo());

                ComputeGroup cg = auth.getComputeGroup(nonAdminUserStr);
                Assert.assertTrue(cg instanceof MergedComputeGroup);
                Assert.assertTrue(((MergedComputeGroup) cg).getName().contains(Tag.VALUE_DEFAULT_TAG));

                // 2.1 get a non-admin user with resource tag
                String setPropStr = "set property for '" + nonAdminUserStr + "' 'resource_tags.location' = 'test_rg1';";
                ExceptionChecker.expectThrowsNoException(() -> setProperty(setPropStr));
                ComputeGroup cg2 = auth.getComputeGroup(nonAdminUserStr);
                Assert.assertTrue(cg2 instanceof MergedComputeGroup);
                Assert.assertTrue(((MergedComputeGroup) cg2).getName().contains("test_rg1"));

                // 2.2 get a non-admin user with multi-resource tag
                String setPropStr2 = "set property for '" + nonAdminUserStr
                        + "' 'resource_tags.location' = 'test_rg1,test_rg2';";
                ExceptionChecker.expectThrowsNoException(() -> setProperty(setPropStr2));
                ComputeGroup cg3 = auth.getComputeGroup(nonAdminUserStr);
                Assert.assertTrue(cg3 instanceof MergedComputeGroup);
                String cgName3 = ((MergedComputeGroup) cg3).getName();
                Assert.assertTrue(cgName3.contains("test_rg1"));
                Assert.assertTrue(cgName3.contains("test_rg2"));

                // 2.3 get a non-admin user with empty tag
                String setPropStr3 = "set property for '" + nonAdminUserStr
                        + "' 'resource_tags.location' = '';";
                ExceptionChecker.expectThrowsNoException(() -> setProperty(setPropStr3));
                ComputeGroup cg4 = auth.getComputeGroup(nonAdminUserStr);
                Assert.assertTrue(cg4 instanceof MergedComputeGroup);
                String cgName4 = ((MergedComputeGroup) cg4).getName();
                Assert.assertTrue(cgName4.contains("default"));
            }

            // 4 get an admin user without resource tag
            {
                ComputeGroup cg1 = auth.getComputeGroup("root");
                Assert.assertTrue(cg1 instanceof AllBackendComputeGroup);

                // 4.1 get an admin user with a resource tag
                String setPropStr = "set property for 'root' 'resource_tags.location' = 'test_rg2';";
                ExceptionChecker.expectThrowsNoException(() -> setProperty(setPropStr));
                ComputeGroup cg2 = auth.getComputeGroup("root");
                Assert.assertTrue(cg2 instanceof MergedComputeGroup);
                Assert.assertTrue(((MergedComputeGroup) cg2).getName().contains("test_rg2"));


                // 4.2 get an admin user with an empty resource tag
                String setPropStr2 = "set property for 'root' 'resource_tags.location' = '';";
                ExceptionChecker.expectThrowsNoException(() -> setProperty(setPropStr2));
                ComputeGroup cg3 = auth.getComputeGroup("root");
                Assert.assertTrue(cg3 instanceof AllBackendComputeGroup);
            }
    }

    @Test
    public void testComputeGroup() {
            // test invalid compute group
            {
                try {
                    ComputeGroup.INVALID_COMPUTE_GROUP.getBackendList();
                } catch (Exception e) {
                    Assert.assertTrue(e.getMessage().contains("invalid compute group can not be used"));
                }

                try {
                    ComputeGroup.INVALID_COMPUTE_GROUP.containsBackend("");
                } catch (Exception e) {
                    Assert.assertTrue(e.getMessage().contains("invalid compute group can not be used"));
                }

                String invalidCgToString = ComputeGroup.INVALID_COMPUTE_GROUP.toString();
                String expectedCgString = String.format("%s id=%s, name=%s",
                        ComputeGroup.INVALID_COMPUTE_GROUP.getClass().getSimpleName(),
                        ComputeGroup.INVALID_COMPUTE_GROUP_NAME, ComputeGroup.INVALID_COMPUTE_GROUP_NAME);
                Assert.assertTrue(expectedCgString.equals(invalidCgToString));
            }

            // test Compute group
            {
                String cgId = "test_cg_id";
                String cgName = "test_cg_1";
                ComputeGroup cg1 = new ComputeGroup(cgId, cgName, null);
                Assert.assertTrue(cgId.equals(cg1.getId()));
                Assert.assertTrue(cgName.equals(cg1.getName()));
                String cg1ToString = String.format("%s id=%s, name=%s", ComputeGroup.class.getSimpleName(), cgId, cgName);
                Assert.assertTrue(cg1ToString.equals(cg1.toString()));
                Assert.assertTrue(cg1.containsBackend(cgName));
                Assert.assertFalse(cg1.containsBackend("123"));
            }

            // test Cloud Compute group
            {
                String cgId = "test_cloud_cg_id";
                String cgName = "test_cloud_cg_name";
                ComputeGroup cg1 = new CloudComputeGroup(cgId, cgName, null);
                Assert.assertTrue(cgId.equals(cg1.getId()));
                Assert.assertTrue(cgName.equals(cg1.getName()));
                String cg1ToString = String.format("%s id=%s, name=%s", CloudComputeGroup.class.getSimpleName(), cgId,
                        cgName);
                Assert.assertTrue(cg1ToString.equals(cg1.toString()));
                Assert.assertTrue(cg1.containsBackend(cgName));
                Assert.assertFalse(cg1.containsBackend("123"));
            }

            // test MergedComputeGroup
            {
                Set<String> emptyTags = Sets.newHashSet();
                String beTag = "merged_cg_name";
                String mergedEmptyName = String.join(",", emptyTags);
                ComputeGroup emptyMergedCg = new MergedComputeGroup(
                        mergedEmptyName, emptyTags, null);

                String mergedCgToString = String.format("%s name=%s ", MergedComputeGroup.class.getSimpleName(), "");
                Assert.assertTrue(mergedCgToString.equals(emptyMergedCg.toString()));
                Assert.assertFalse(emptyMergedCg.containsBackend(beTag));

                Set<String> tags = Sets.newHashSet();
                tags.add(beTag);
                String mergedName = String.join(",", tags);
                ComputeGroup notEmptyMergedCg = new MergedComputeGroup(mergedName, tags, null);
                String mergedCgToString2 = String.format("%s name=%s ", MergedComputeGroup.class.getSimpleName(), mergedName);
                Assert.assertTrue(mergedCgToString2.equals(notEmptyMergedCg.toString()));
                Assert.assertTrue(notEmptyMergedCg.containsBackend(beTag));
            }

            // test AllBackendComputeGroup
            {
                AllBackendComputeGroup allBeCg = new AllBackendComputeGroup(null);
                try {
                    allBeCg.getName();
                } catch (Exception e) {
                    Assert.assertTrue(e.getMessage().contains("AllBackendComputeGroup not implements getName"));
                }

                try {
                    allBeCg.getId();
                } catch (Exception e) {
                    Assert.assertTrue(e.getMessage().contains("AllBackendComputeGroup not implements getId"));
                }

                Assert.assertTrue(allBeCg.getClass().getSimpleName().equals(allBeCg.toString()));
            }

            // test equals
            {
                String cgName1 = "cg_name1";
                String cgId1 = "cg_id1";
                ComputeGroup cg1 = new ComputeGroup(cgId1, cgName1, null);

                String cgName11 = "cg_name11";
                ComputeGroup cg11 = new ComputeGroup(cgId1, cgName11, null);

                String cgName2 = "cg_name2";
                String cgId2 = "cg_id2";
                ComputeGroup cg2 = new ComputeGroup(cgId2, cgName2, null);

                ComputeGroup cg3 = new ComputeGroup(cgId1, cgName1, null);

                Assert.assertFalse(cg1.equals(cg2));
                Assert.assertFalse(cg2.equals(cg1));

                Assert.assertFalse(cg1.equals(cg3));
                Assert.assertFalse(cg3.equals(cg1));

                Assert.assertFalse(cg1.equals(cg11));
                Assert.assertFalse(cg11.equals(cg1));

                Assert.assertFalse(cg1.equals(null));

                CloudComputeGroup cloudCg1 = new CloudComputeGroup(cgId1, cgName1, null);
                CloudComputeGroup cloudCg2 = new CloudComputeGroup(cgId2, cgName2, null);
                Assert.assertFalse(cloudCg1.equals(cloudCg2));
                Assert.assertFalse(cloudCg2.equals(cloudCg1));

                AllBackendComputeGroup allBecg1 = new AllBackendComputeGroup(null);
                AllBackendComputeGroup allBecg2 = new AllBackendComputeGroup(null);
                Assert.assertFalse(allBecg1.equals(allBecg2));
                Assert.assertFalse(allBecg2.equals(allBecg1));

                MergedComputeGroup mergedCg1 = new MergedComputeGroup("", null, null);
                MergedComputeGroup mergedCg2 = new MergedComputeGroup("", null, null);
                Assert.assertFalse(mergedCg1.equals(mergedCg2));
                Assert.assertFalse(mergedCg2.equals(mergedCg1));

                // ComputeGroup vs others
                Assert.assertTrue(cg1.equals(cg1));
                Assert.assertFalse(cg1.equals(cloudCg1));
                Assert.assertFalse(cg1.equals(allBecg1));
                Assert.assertFalse(cg1.equals(mergedCg1));

                // CloudComputeGroup vs others
                Assert.assertTrue(cloudCg1.equals(cloudCg1));
                Assert.assertFalse(cloudCg1.equals(cg1));
                Assert.assertFalse(cloudCg1.equals(allBecg1));
                Assert.assertFalse(cloudCg1.equals(mergedCg1));

                // AllBackendComputeGroup vs others
                Assert.assertTrue(allBecg1.equals(allBecg1));
                Assert.assertFalse(allBecg1.equals(cg1));
                Assert.assertFalse(allBecg1.equals(cloudCg1));
                Assert.assertFalse(allBecg1.equals(mergedCg1));

                // MergedComputeGroup vs others
                Assert.assertTrue(mergedCg1.equals(mergedCg1));
                Assert.assertFalse(mergedCg1.equals(cg1));
                Assert.assertFalse(mergedCg1.equals(allBecg1));
                Assert.assertFalse(mergedCg1.equals(cloudCg1));
            }
    }

    @Test
    public void testComputeGroupMgr() throws Exception {
        SystemInfoService systemInfoService = new SystemInfoService();
        ComputeGroupMgr cgmgr = new ComputeGroupMgr(systemInfoService);

        Tag beTag1 = Tag.create(Tag.TYPE_LOCATION, "be_tag1");
        Tag beTag2 = Tag.create(Tag.TYPE_LOCATION, "be_tag2");
        Tag beTag3 = Tag.create(Tag.TYPE_LOCATION, "be_tag3");

            {
                Backend be1 = new Backend(10001, "192.168.1.1", 9050);
                be1.setTagMap(beTag1.toMap());
                be1.setAlive(true);

                Backend be2 = new Backend(10002, "192.168.1.2", 9050);
                be2.setTagMap(beTag1.toMap());
                be2.setAlive(true);

                systemInfoService.addBackend(be1);
                systemInfoService.addBackend(be2);
            }

            {
                Backend be1 = new Backend(10003, "192.168.1.3", 9050);
                be1.setTagMap(beTag2.toMap());
                be1.setAlive(true);

                Backend be2 = new Backend(10004, "192.168.1.4", 9050);
                be2.setTagMap(beTag2.toMap());
                be2.setAlive(true);
                systemInfoService.addBackend(be1);
                systemInfoService.addBackend(be2);
            }

            {
                Backend be3 = new Backend(10005, "192.168.1.5", 9050);
                be3.setTagMap(beTag3.toMap());
                be3.setAlive(true);
                systemInfoService.addBackend(be3);
            }

        ComputeGroup cg1 = cgmgr.getComputeGroupByName(beTag1.value);
        Assert.assertTrue(cg1.getBackendList().size() == 2);
        ComputeGroup cg2 = cgmgr.getComputeGroupByName("abc");
        Assert.assertTrue(cg2.getBackendList().size() == 0);

        Set<Tag> tagSet1 = Sets.newHashSet(beTag1, beTag2);
        Assert.assertTrue(cgmgr.getComputeGroup(tagSet1).getBackendList().size() == 4);

        Tag beTag4 = Tag.create(Tag.TYPE_LOCATION, "abc");
        Set<Tag> tagset2 = Sets.newHashSet(beTag4);
        Assert.assertTrue(cgmgr.getComputeGroup(tagset2).getBackendList().size() == 0);

        Set<Tag> emptyTagSet = Sets.newHashSet();
        Assert.assertTrue(cgmgr.getComputeGroup(emptyTagSet).getBackendList().size() == 0);

        Assert.assertTrue(cgmgr.getAllBackendComputeGroup().getBackendList().size() == 5);

    }

    @Test
    public void testConnectContextToFederationBackendPolicy() throws UserException, IOException {
        SystemInfoService systemInfoService = new SystemInfoService();

        Tag beTag1 = Tag.create(Tag.TYPE_LOCATION, "be_tag1");
        Tag beTag2 = Tag.DEFAULT_BACKEND_TAG;
        Backend defaultBe = null;
        Backend tag1Be = null;

            {
                tag1Be = new Backend(10001, "192.168.1.1", 9050);
                tag1Be.setTagMap(beTag1.toMap());
                tag1Be.setAlive(true);

                defaultBe = new Backend(10002, "192.168.1.2", 9050);
                defaultBe.setTagMap(beTag2.toMap());
                defaultBe.setAlive(true);

                systemInfoService.addBackend(tag1Be);
                systemInfoService.addBackend(defaultBe);
            }


        ComputeGroupMgr cgmgr = new ComputeGroupMgr(systemInfoService);
        new Expectations() {
            {
                Env.getCurrentEnv().getComputeGroupMgr();
                minTimes = 0;
                result = cgmgr;
            }
        };
        BeSelectionPolicy beSelPolicy = new BeSelectionPolicy.Builder().build();


            // 1 when connectctx is null, return default tag be.
            {
                ConnectContext.remove();
                FederationBackendPolicy fbPolicy = new FederationBackendPolicy();
                fbPolicy.init(beSelPolicy);
                Assert.assertTrue(fbPolicy.getBackends().size() == 1);
                Assert.assertTrue(fbPolicy.getBackends().contains(defaultBe));
            }

            // 2 get compute group from connect ctx
            {
                ConnectContext.remove();
                ConnectContext context = new ConnectContext();
                context.setThreadLocalInfo();
                context.setComputeGroup(cgmgr.getComputeGroupByName(beTag1.value));
                FederationBackendPolicy fbPolicy = new FederationBackendPolicy();
                fbPolicy.init(beSelPolicy);
                Assert.assertTrue(fbPolicy.getBackends().size() == 1);
                Assert.assertTrue(fbPolicy.getBackends().contains(tag1Be));
            }

            // 3 test set invalid compute group
            {
                ConnectContext.remove();
                ConnectContext context = new ConnectContext();
                context.setThreadLocalInfo();
                context.setComputeGroup(ComputeGroup.INVALID_COMPUTE_GROUP);
                FederationBackendPolicy fbPolicy = new FederationBackendPolicy();
                try {
                    fbPolicy.init(beSelPolicy);
                } catch (UserException e) {
                    Assert.assertTrue(e.getMessage().contains(ComputeGroup.INVALID_COMPUTE_GROUP_ERR_MSG));
                }
            }

            // 4 test get compute group from user property
            {
                ConnectContext.remove();
                UtFrameUtils.createDefaultCtx();
                String setPropStr = "set property for 'root' 'resource_tags.location' = '" + beTag1.value + "';";
                ExceptionChecker.expectThrowsNoException(() -> setProperty(setPropStr));
                FederationBackendPolicy fbPolicy = new FederationBackendPolicy();
                fbPolicy.init(beSelPolicy);
                Assert.assertTrue(fbPolicy.getBackends().size() == 1);
                Assert.assertTrue(fbPolicy.getBackends().contains(tag1Be));
            }
    }

    @Test
    public void testBrokerLoadToConnectContext() throws UserException, IOException {
        new Expectations() {
            {
                Env.getCurrentEnv().getComputeGroupMgr();
                minTimes = 0;
                result = new ComputeGroupMgr(null);
            }
        };

            // test user is empty string
            {
                ConnectContext.remove();
                UserIdentity emptyUser = new UserIdentity("", "%");
                emptyUser.setIsAnalyzed();
                BrokerLoadJob brokerLoadJob =
                        new BrokerLoadJob(1, null, null, null, emptyUser);
                brokerLoadJob.setComputeGroup();
                Assert.assertTrue(ConnectContext.get().getComputeGroupSafely() instanceof AllBackendComputeGroup);
            }

            // test invalid user
            {
                ConnectContext.remove();
                UserIdentity emptyUser = new UserIdentity(RandomIdentifierGenerator.generateRandomIdentifier(8), "%");
                emptyUser.setIsAnalyzed();
                BrokerLoadJob brokerLoadJob =
                        new BrokerLoadJob(1, null, null, null, emptyUser);
                brokerLoadJob.setComputeGroup();
                Assert.assertTrue(ConnectContext.get().getComputeGroupSafely() instanceof AllBackendComputeGroup);
            }

            // test get cg from user property
            {

                UtFrameUtils.createDefaultCtx();
                String nonAdminUserStr = "non_admin_user";
                UserIdentity nonAdminUser = new UserIdentity(nonAdminUserStr, "%");
                UserDesc nonAdminUserDesc = new UserDesc(nonAdminUser, "12345", true);

                CreateUserCommand createUserCommand = new CreateUserCommand(new CreateUserInfo(nonAdminUserDesc));
                createUserCommand.getInfo().validate();
                auth.createUser(createUserCommand.getInfo());

                String tagName = "tag_rg_1";
                String setPropStr = "set property for '" + nonAdminUserStr + "' 'resource_tags.location' = '" + tagName + "';";
                ExceptionChecker.expectThrowsNoException(() -> setProperty(setPropStr));

                BrokerLoadJob brokerLoadJob =
                        new BrokerLoadJob(1, null, null, null, nonAdminUser);
                brokerLoadJob.setComputeGroup();
                ComputeGroup cg = ConnectContext.get().getComputeGroupSafely();
                Assert.assertTrue(cg instanceof MergedComputeGroup);
                Assert.assertTrue(((MergedComputeGroup) cg).getName().contains(tagName));

            }
    }

    @Test
    public void testRoutineLoadToConnectContext() throws Exception {
        new Expectations() {
            {
                Env.getCurrentEnv().getComputeGroupMgr();
                minTimes = 0;
                result = new ComputeGroupMgr(null);

                Env.getCurrentSystemInfo();
                minTimes = 0;
                result = new SystemInfoService();
            }
        };



            // 1 ctx's user is empty, return all backend
            {
                ConnectContext ctx = UtFrameUtils.createDefaultCtx();
                RoutineLoadJob job = new KafkaRoutineLoadJob();
                job.setComputeGroup();
                Assert.assertTrue(ctx.getComputeGroupSafely() instanceof AllBackendComputeGroup);
            }


            // 2 set an invalid user, get an invalid compute group, then return all backends
            {
                ConnectContext.get().setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("xxxx", "%"));
                RoutineLoadJob job = new KafkaRoutineLoadJob();
                job.setComputeGroup();
                Assert.assertTrue(ConnectContext.get().getComputeGroupSafely() instanceof AllBackendComputeGroup);
            }

            // 3 get a valid compute group
            {
                ConnectContext.get().setCurrentUserIdentity(UserIdentity.ROOT);
                String setPropStr = "set property for 'root' 'resource_tags.location' = 'tag_rg_1';";
                ExceptionChecker.expectThrowsNoException(() -> setProperty(setPropStr));
                RoutineLoadJob job = new KafkaRoutineLoadJob();
                job.setComputeGroup();
                ComputeGroup cg = ConnectContext.get().getComputeGroupSafely();
                Assert.assertTrue(cg instanceof MergedComputeGroup);
                Assert.assertTrue(((MergedComputeGroup) cg).getName().contains("tag_rg_1"));
            }

            // 4 get a null job
            {
                RoutineLoadManager routineLoadManager = new RoutineLoadManager();
                try {
                    routineLoadManager.getAvailableBackendIdsForUt(1);
                } catch (LoadException e) {
                    Assert.assertTrue(e.getMessage().contains("does not exist"));
                }
            }
    }
}
