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
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AccessControllerManagerCurrentRolesFallbackTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createRole("role1");
        grantPriv("GRANT SELECT_PRIV ON internal.test.* TO ROLE 'role1';");
        addUser("u1", true);
    }

    @Test
    public void testUserIdentityFallbackUsesThreadContextWhenUserMatches() {
        AccessControllerManager accessManager = Env.getCurrentEnv().getAccessManager();
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("u1", "%");

        connectContext.setCurrentUserIdentity(user);
        connectContext.setCurrentRoles(Sets.newHashSet("role1"));
        connectContext.setThreadLocalInfo();

        Assertions.assertTrue(accessManager.checkDbPriv(
                PrivilegeContext.of(user, connectContext.getCurrentRoles()), InternalCatalog.INTERNAL_CATALOG_NAME,
                "test", PrivPredicate.SELECT));
    }

    @Test
    public void testUserIdentityFallbackIgnoredWhenUserNotMatch() {
        AccessControllerManager accessManager = Env.getCurrentEnv().getAccessManager();
        UserIdentity ctxUser = UserIdentity.createAnalyzedUserIdentWithIp("u1", "%");
        UserIdentity anotherUser = UserIdentity.createAnalyzedUserIdentWithIp("u2", "%");

        connectContext.setCurrentUserIdentity(ctxUser);
        connectContext.setCurrentRoles(Sets.newHashSet("role1"));
        connectContext.setThreadLocalInfo();

        Assertions.assertFalse(accessManager.checkDbPriv(
                PrivilegeContext.of(anotherUser), InternalCatalog.INTERNAL_CATALOG_NAME,
                "test", PrivPredicate.SELECT));
    }

    @Test
    public void testExplicitCurrentRolesWorksWithoutThreadContext() {
        AccessControllerManager accessManager = Env.getCurrentEnv().getAccessManager();
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("u1", "%");

        ConnectContext.remove();

        Assertions.assertTrue(accessManager.checkDbPriv(PrivilegeContext.of(user, Sets.newHashSet("role1")),
                InternalCatalog.INTERNAL_CATALOG_NAME, "test", PrivPredicate.SELECT));

        connectContext.setThreadLocalInfo();
    }
}
