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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SuCommandTest extends TestWithFeService {
    private static final UserIdentity TARGET_USER =
            UserIdentity.createAnalyzedUserIdentWithIp("mry", "selectdb.com");

    @Override
    protected void runBeforeAll() throws Exception {
        Env.getCurrentEnv().getAuth().createRole("role1", true, null);
        Env.getCurrentEnv().getAuth().createRole("role2", true, null);
    }

    @Test
    public void testSuWithRoles() throws Exception {
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        connectContext.clearCurrentRoles();

        SuCommand cmd = new SuCommand(TARGET_USER, Lists.newArrayList("role1"));
        cmd.run(connectContext, null);

        Assertions.assertEquals(TARGET_USER, connectContext.getCurrentUserIdentity());
        Assertions.assertNotNull(connectContext.getCurrentRoles());
        Assertions.assertEquals(1, connectContext.getCurrentRoles().size());
        Assertions.assertTrue(connectContext.getCurrentRoles().contains("role1"));
    }

    @Test
    public void testSuWithRolesUsingCommaSyntax() throws Exception {
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        connectContext.clearCurrentRoles();

        SuCommand cmd = new SuCommand(TARGET_USER, Lists.newArrayList("role1"));
        cmd.run(connectContext, null);

        Assertions.assertEquals(TARGET_USER, connectContext.getCurrentUserIdentity());
        Assertions.assertNotNull(connectContext.getCurrentRoles());
        Assertions.assertEquals(1, connectContext.getCurrentRoles().size());
        Assertions.assertTrue(connectContext.getCurrentRoles().contains("role1"));
    }

    @Test
    public void testSuWithMultipleRolesRequireCommaSyntax() throws Exception {
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        connectContext.clearCurrentRoles();

        SuCommand cmd = new SuCommand(TARGET_USER, Lists.newArrayList("role1", "role2"));
        cmd.run(connectContext, null);

        Assertions.assertEquals(TARGET_USER, connectContext.getCurrentUserIdentity());
        Assertions.assertNotNull(connectContext.getCurrentRoles());
        Assertions.assertEquals(2, connectContext.getCurrentRoles().size());
        Assertions.assertTrue(connectContext.getCurrentRoles().contains("role1"));
        Assertions.assertTrue(connectContext.getCurrentRoles().contains("role2"));
    }

    @Test
    public void testSuWithoutRoles() throws Exception {
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        connectContext.clearCurrentRoles();

        SuCommand cmd = new SuCommand(TARGET_USER, Lists.newArrayList());
        cmd.run(connectContext, null);

        Assertions.assertEquals(TARGET_USER, connectContext.getCurrentUserIdentity());
        Assertions.assertNotNull(connectContext.getCurrentRoles());
        Assertions.assertTrue(connectContext.getCurrentRoles().isEmpty());
    }

    @Test
    public void testSuNonRootRejected() throws Exception {
        ConnectContext ctx = TestWithFeService.createCtx(
                UserIdentity.createAnalyzedUserIdentWithIp("u1", "%"), "127.0.0.1");
        SuCommand cmd = new SuCommand(TARGET_USER, Lists.newArrayList("role1"));
        Assertions.assertThrows(AnalysisException.class, () -> cmd.run(ctx, null));
    }

    @Test
    public void testSuRoleNotExist() {
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        connectContext.clearCurrentRoles();
        SuCommand cmd = new SuCommand(TARGET_USER, Lists.newArrayList("no_such_role"));
        Assertions.assertDoesNotThrow(() -> cmd.run(connectContext, null));
        Assertions.assertEquals(TARGET_USER, connectContext.getCurrentUserIdentity());
    }

    @Test
    public void testSuAfterSwitchUserRejected() throws Exception {
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        connectContext.clearCurrentRoles();

        SuCommand first = new SuCommand(TARGET_USER, Lists.newArrayList("role1"));
        first.run(connectContext, null);
        Assertions.assertEquals(TARGET_USER, connectContext.getCurrentUserIdentity());

        SuCommand second = new SuCommand(
                UserIdentity.createAnalyzedUserIdentWithIp("another", "selectdb.com"), Lists.newArrayList("role1"));
        Assertions.assertThrows(AnalysisException.class, () -> second.run(connectContext, null));
    }

    @Test
    public void testSuAfterSwitchRootRejected() throws Exception {
        connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
        connectContext.clearCurrentRoles();

        SuCommand first = new SuCommand(UserIdentity.ROOT, Lists.newArrayList("role1"));
        first.run(connectContext, null);
        Assertions.assertEquals(UserIdentity.ROOT, connectContext.getCurrentUserIdentity());

        SuCommand second = new SuCommand(TARGET_USER, Lists.newArrayList("role1"));
        Assertions.assertThrows(AnalysisException.class, () -> second.run(connectContext, null));
    }
}
