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

package org.apache.doris.authentication.handler;

import org.apache.doris.authentication.AuthenticationPluginType;
import org.apache.doris.authentication.AuthenticationProfile;
import org.apache.doris.authentication.Identity;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class RoleMapperTest {

    @Test
    public void testRoleMapping() {
        AuthenticationProfile profile = AuthenticationProfile.builder()
                .name("ldap_profile")
                .pluginType(AuthenticationPluginType.LDAP)
                .mapRole("dev", "role_dev")
                .mapRole("ops", "role_ops")
                .build();

        Set<String> externalGroups = new HashSet<>(Arrays.asList("dev", "unknown"));
        Identity identity = Identity.builder()
                .username("alice")
                .authenticatorName("ldap")
                .authenticatorType(AuthenticationPluginType.LDAP)
                .externalGroups(externalGroups)
                .build();

        RoleMapper mapper = new RoleMapper();
        Set<String> roles = mapper.mapRoles(identity, profile);

        Assert.assertEquals(1, roles.size());
        Assert.assertTrue(roles.contains("role_dev"));
    }
}
