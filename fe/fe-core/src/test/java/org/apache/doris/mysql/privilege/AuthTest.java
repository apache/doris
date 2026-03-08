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

import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AccessPrivilege;
import org.apache.doris.catalog.AccessPrivilegeWithCols;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.authenticate.ldap.LdapManager;
import org.apache.doris.nereids.trees.plans.commands.GrantRoleCommand;
import org.apache.doris.nereids.trees.plans.commands.GrantTablePrivilegeCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateUserInfo;
import org.apache.doris.utframe.TestWithFeService;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

public class AuthTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
    }

    @Test
    public void testMergeRolePriv() throws Exception {
        UserIdentity user = createUser("u1");
        createRole("role1");
        createRole("role2");
        grantTablePrivilegeToRole("role1", AccessPrivilege.LOAD_PRIV);
        grantTablePrivilegeToRole("role2", AccessPrivilege.GRANT_PRIV);
        grantRoles(user, "role1", "role2");

        boolean hasPriv = Env.getCurrentEnv().getAuth()
                .checkDbPriv(PrivilegeContext.of(user),
                        InternalCatalog.INTERNAL_CATALOG_NAME, "test",
                        PrivPredicate.of(PrivBitSet.of(Privilege.GRANT_PRIV, Privilege.LOAD_PRIV), Operator.AND));
        Assert.assertTrue(hasPriv);
    }

    @Test
    public void testGetRoleNamesByUserWithLdap() throws Exception {
        UserIdentity user = createUser("u2");
        createRole("role3");
        createRole("role4");
        grantRoles(user, "role3", "role4");

        Set<String> roleNames = Env.getCurrentEnv().getAuth()
                .getRoleNamesByUserWithLdap(user, true);
        Assert.assertEquals(3, roleNames.size());
        roleNames = Env.getCurrentEnv().getAuth().getRoleNamesByUserWithLdap(user, false);
        Assert.assertEquals(2, roleNames.size());
    }

    @Test
    public void testGetRolesByUserWithCurrentRolesOverride() throws Exception {
        UserIdentity user = createUser("u3");
        createRole("role5");
        createRole("role6");
        grantRoles(user, "role5");

        Set<Role> roles = Env.getCurrentEnv().getAuth()
                .getRolesByUserWithLdap(user, Collections.singleton("role6"));
        Assert.assertEquals(1, roles.size());
        Assert.assertTrue(roles.stream().anyMatch(role -> role != null && "role6".equals(role.getRoleName())));
    }

    @Test
    public void testGetRolesByUserWithEmptyCurrentRolesAddsLdap(@Mocked LdapManager ldapManager) throws Exception {
        UserIdentity user = createUser("u4");
        createRole("role_local");
        createRole("role_ldap");
        grantRoles(user, "role_local");

        new Expectations() {
            {
                ldapManager.getUserRoles(anyString);
                minTimes = 0;
                result = Collections.singleton(Env.getCurrentEnv().getAuth().getRoleByName("role_ldap"));
            }
        };

        String previousAuthType = Config.authentication_type;
        try {
            Config.authentication_type = "ldap";
            Set<Role> roles = Env.getCurrentEnv().getAuth()
                    .getRolesByUserWithLdap(user, Collections.emptySet());
            Assert.assertTrue(roles.stream().anyMatch(role -> role != null && "role_ldap".equals(role.getRoleName())));
            Assert.assertFalse(roles.stream().anyMatch(role -> role != null && "role_local".equals(role.getRoleName())));
        } finally {
            Config.authentication_type = previousAuthType;
        }
    }

    @Test
    public void testGetRolesByUserWithLdapErrorFallback(@Mocked LdapManager ldapManager) throws Exception {
        UserIdentity user = createUser("u5");
        createRole("role_override");

        new Expectations() {
            {
                ldapManager.getUserRoles(anyString);
                minTimes = 0;
                result = new RuntimeException("ldap config invalid");
            }
        };

        String previousAuthType = Config.authentication_type;
        try {
            Config.authentication_type = "ldap";
            Set<Role> roles = Env.getCurrentEnv().getAuth()
                    .getRolesByUserWithLdap(user, Collections.singleton("role_override"));
            Assert.assertEquals(1, roles.size());
            Assert.assertTrue(roles.stream().anyMatch(role -> role != null
                    && "role_override".equals(role.getRoleName())));
        } finally {
            Config.authentication_type = previousAuthType;
        }
    }

    private UserIdentity createUser(String userName) throws Exception {
        UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp(userName, "%");
        CreateUserInfo createUserInfo = new CreateUserInfo(new UserDesc(userIdentity));
        Env.getCurrentEnv().getAuth().createUser(createUserInfo);
        return userIdentity;
    }

    @Override
    protected void createRole(String roleName) throws Exception {
        Env.getCurrentEnv().getAuth().createRole(roleName, true, null);
    }

    private void grantRoles(UserIdentity userIdentity, String... roles) throws Exception {
        GrantRoleCommand command = new GrantRoleCommand(userIdentity, Arrays.asList(roles));
        command.validate();
        Env.getCurrentEnv().getAuth().grantRoleCommand(command);
    }

    private void grantTablePrivilegeToRole(String roleName, AccessPrivilege privilege) throws Exception {
        TablePattern tablePattern = new TablePattern(InternalCatalog.INTERNAL_CATALOG_NAME, "test", "*");
        tablePattern.analyze();
        GrantTablePrivilegeCommand command = new GrantTablePrivilegeCommand(
                Collections.singletonList(new AccessPrivilegeWithCols(privilege)),
                tablePattern,
                Optional.empty(),
                Optional.of(roleName));
        command.validate();
        Env.getCurrentEnv().getAuth().grantTablePrivilegeCommand(command);
    }
}
