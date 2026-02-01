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

import org.apache.doris.authentication.AuthenticationBinding;
import org.apache.doris.authentication.AuthenticationPluginType;
import org.apache.doris.authentication.AuthenticationProfile;
import org.apache.doris.authentication.AuthenticationRequest;
import org.apache.doris.authentication.CredentialType;
import org.apache.doris.authentication.spi.AuthenticationException;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class BindingResolverTest {

    @Test
    public void testUserBindingPrecedenceOverRequested() throws AuthenticationException {
        ProfileRegistry profileRegistry = new ProfileRegistry();
        profileRegistry.register(AuthenticationProfile.builder()
                .name("user_profile")
                .pluginType(AuthenticationPluginType.PASSWORD)
                .enabled(true)
                .priority(1)
                .build());
        profileRegistry.register(AuthenticationProfile.builder()
                .name("requested_profile")
                .pluginType(AuthenticationPluginType.LDAP)
                .enabled(true)
                .priority(10)
                .build());

        BindingRegistry bindingRegistry = new BindingRegistry();
        bindingRegistry.putUserBinding("alice",
                new AuthenticationBinding(AuthenticationBinding.BindingType.USER, "alice", "user_profile", 1, false));

        BindingResolver resolver = new BindingResolver(profileRegistry, bindingRegistry);
        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("alice")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .requestedProfile("requested_profile")
                .build();

        List<AuthenticationProfile> candidates = resolver.resolveCandidates("alice", request);
        Assert.assertFalse(candidates.isEmpty());
        Assert.assertEquals("user_profile", candidates.get(0).getName());
        if (candidates.size() > 1) {
            Assert.assertEquals("requested_profile", candidates.get(1).getName());
        }
    }

    @Test(expected = AuthenticationException.class)
    public void testRequestedProfileRequiredWhenNoUserBinding() throws AuthenticationException {
        ProfileRegistry profileRegistry = new ProfileRegistry();
        BindingRegistry bindingRegistry = new BindingRegistry();
        BindingResolver resolver = new BindingResolver(profileRegistry, bindingRegistry);

        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("bob")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .requestedProfile("missing_profile")
                .build();

        resolver.resolveCandidates("bob", request);
    }

    @Test
    public void testRequestedProfileUsedWhenNoUserBinding() throws AuthenticationException {
        ProfileRegistry profileRegistry = new ProfileRegistry();
        profileRegistry.register(AuthenticationProfile.builder()
                .name("requested_profile")
                .pluginType(AuthenticationPluginType.LDAP)
                .enabled(true)
                .priority(10)
                .build());

        BindingRegistry bindingRegistry = new BindingRegistry();
        BindingResolver resolver = new BindingResolver(profileRegistry, bindingRegistry);

        AuthenticationRequest request = AuthenticationRequest.builder()
                .username("charlie")
                .credentialType(CredentialType.CLEAR_TEXT_PASSWORD)
                .requestedProfile("requested_profile")
                .build();

        List<AuthenticationProfile> candidates = resolver.resolveCandidates("charlie", request);
        Assert.assertEquals(1, candidates.size());
        Assert.assertEquals("requested_profile", candidates.get(0).getName());
    }
}
