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
import org.apache.doris.authorization.AccessContext;
import org.apache.doris.authorization.AccessDeniedException;
import org.apache.doris.authorization.AccessRequirement;
import org.apache.doris.authorization.AccessRequirements;
import org.apache.doris.authorization.AuthorizedResource;
import org.apache.doris.authorization.AuthorizedSubject;
import org.apache.doris.authorization.ResourceKind;
import org.apache.doris.authorization.spi.AuthorizationPlugin;
import org.apache.doris.common.jmockit.Deencapsulation;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Set;

/**
 * What the engine answers an authorization source that asks it something.
 */
public class EngineAuthorizationContextTest {

    private static final AuthorizedSubject SUBJECT = AuthorizedSubject.of("user1", "%");

    /** An authorization source that records what it was asked and grants everything. */
    private static final class Recording implements AuthorizationPlugin {
        private ResourceKind askedAbout;
        private AccessRequirement asked;

        @Override
        public String name() {
            return "recording";
        }

        @Override
        public void checkPrivilege(AuthorizedSubject subject, AuthorizedResource resource,
                AccessRequirement requirement, AccessContext context) throws AccessDeniedException {
            askedAbout = resource.getKind();
            asked = requirement;
        }
    }

    /**
     * Roles come without the per-user default role: it is how the built-in model stores a user's own grants,
     * not a role an administrator writes a policy against.
     */
    @Test
    public void testRolesAreReadWithoutTheInternalDefaultRole() {
        Auth auth = Mockito.mock(Auth.class);
        Set<String> roles = ImmutableSet.of("analyst");
        Mockito.when(auth.getRolesByUser(Mockito.any(UserIdentity.class), Mockito.eq(false))).thenReturn(roles);

        EngineAuthorizationContext context = new EngineAuthorizationContext(
                new AccessControllerManager(auth), auth);
        context.servedBy(new Recording());

        Assertions.assertEquals(roles, context.rolesOf(SUBJECT));
        Mockito.verify(auth).getRolesByUser(
                UserIdentity.createAnalyzedUserIdentWithIp("user1", "%"), false);
    }

    @Test
    public void testAsksWhoeverGovernsInstanceScope() {
        Auth auth = Mockito.mock(Auth.class);
        AccessControllerManager manager = new AccessControllerManager(auth);
        Recording authority = new Recording();
        Deencapsulation.setField(manager, "defaultAccessController", authority);

        EngineAuthorizationContext context = new EngineAuthorizationContext(manager, auth);
        context.servedBy(new Recording());

        Assertions.assertTrue(context.grantedByGlobalScopeAuthority(SUBJECT, AccessRequirements.SELECT));
        Assertions.assertEquals(ResourceKind.GLOBAL, authority.askedAbout);
        Assertions.assertEquals(AccessRequirements.SELECT, authority.asked);
    }

    /**
     * A source that governs instance scope itself is told no rather than being routed back into itself: it is
     * about to answer that very question, and answering it twice evaluates the same policies twice.
     */
    @Test
    public void testTheAuthorityIsNotAskedToDeferToItself() {
        Auth auth = Mockito.mock(Auth.class);
        AccessControllerManager manager = new AccessControllerManager(auth);
        Recording authority = new Recording();
        Deencapsulation.setField(manager, "defaultAccessController", authority);

        EngineAuthorizationContext context = new EngineAuthorizationContext(manager, auth);
        context.servedBy(authority);

        Assertions.assertFalse(context.grantedByGlobalScopeAuthority(SUBJECT, AccessRequirements.SELECT));
        Assertions.assertNull(authority.askedAbout);
    }

    /**
     * The authority may well be a controller written against the older interface - that is what
     * {@code access_controller_type} has always named - so deferring to it has to reach through the adapter
     * and arrive as the global check that interface offers.
     */
    @Test
    public void testDeferenceReachesAnAuthorityWrittenAgainstTheOlderInterface() {
        Auth auth = Mockito.mock(Auth.class);
        AccessControllerManager manager = new AccessControllerManager(auth);
        CatalogAccessController controller = Mockito.mock(CatalogAccessController.class);
        Mockito.when(controller.checkGlobalPriv(
                UserIdentity.createAnalyzedUserIdentWithIp("user1", "%"), PrivPredicate.SELECT))
                .thenReturn(true);
        Deencapsulation.setField(manager, "defaultAccessController",
                new LegacyAccessControllerPlugin("authority", controller,
                        (subject, requirement) -> false));

        EngineAuthorizationContext context = new EngineAuthorizationContext(manager, auth);
        context.servedBy(new Recording());

        Assertions.assertTrue(context.grantedByGlobalScopeAuthority(SUBJECT, AccessRequirements.SELECT));
    }

    @Test
    public void testAskingBeforeTheEngineKnowsWhichSourceItIsFails() {
        Auth auth = Mockito.mock(Auth.class);
        EngineAuthorizationContext context = new EngineAuthorizationContext(
                new AccessControllerManager(auth), auth);

        Assertions.assertThrows(IllegalStateException.class,
                () -> context.grantedByGlobalScopeAuthority(SUBJECT, AccessRequirements.SELECT));
    }
}
