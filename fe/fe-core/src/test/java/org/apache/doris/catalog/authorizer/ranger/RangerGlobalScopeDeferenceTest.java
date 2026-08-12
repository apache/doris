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

package org.apache.doris.catalog.authorizer.ranger;

import org.apache.doris.authorization.AccessContext;
import org.apache.doris.authorization.AccessDeniedException;
import org.apache.doris.authorization.AccessRequirement;
import org.apache.doris.authorization.AccessRequirements;
import org.apache.doris.authorization.AuthorizedResource;
import org.apache.doris.authorization.AuthorizedSubject;
import org.apache.doris.authorization.ResourceKind;
import org.apache.doris.authorization.spi.AuthorizationPlugin;
import org.apache.doris.catalog.authorizer.ranger.doris.RangerDorisAccessController;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.EngineAuthorizationContext;
import org.apache.doris.mysql.privilege.StubRangerPolicyEngine;

import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A catalog bound to Ranger still lets through whoever holds the privilege at instance scope.
 *
 * <p>Instance scope is not something a Ranger service knows about; it belongs to the source named by
 * {@code access_controller_type}. Honouring it is why a cluster administrator can still reach a Ranger-governed
 * catalog after the engine stopped establishing privileges on a source's behalf. It is the source's own choice,
 * so it is tested on the source, it is configurable, and a source that declines it stays a legal source.
 */
public class RangerGlobalScopeDeferenceTest {
    private static final AuthorizedSubject ADMIN = AuthorizedSubject.of("admin_user", "%");
    private static final AuthorizedSubject RANGER_USER =
            AuthorizedSubject.of(StubRangerPolicyEngine.ALLOWED_USER, "%");
    private static final AuthorizedResource.Table ALLOWED_TABLE = AuthorizedResource.table("ctl",
            StubRangerPolicyEngine.ALLOWED_DB, StubRangerPolicyEngine.ALLOWED_TABLE);

    /** Counts what actually reached the policy engine, so "answered without asking Ranger" is observable. */
    private static final class CountingPolicyEngine extends StubRangerPolicyEngine {
        private final AtomicInteger requests = new AtomicInteger();

        @Override
        public RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
            requests.incrementAndGet();
            return super.isAccessAllowed(request);
        }
    }

    /**
     * Stands for whatever {@code access_controller_type} installs: it grants everything to one account and
     * nothing to anyone else, and remembers what it was asked about.
     */
    private static final class Authority implements AuthorizationPlugin {
        private final AuthorizedSubject allowed;
        private ResourceKind askedAbout;

        private Authority(AuthorizedSubject allowed) {
            this.allowed = allowed;
        }

        @Override
        public String name() {
            return "authority";
        }

        @Override
        public void checkPrivilege(AuthorizedSubject subject, AuthorizedResource resource,
                AccessRequirement requirement, AccessContext context) throws AccessDeniedException {
            askedAbout = resource.getKind();
            if (!subject.equals(allowed)) {
                throw AccessDeniedException.of(subject, resource, requirement, name());
            }
        }
    }

    private final CountingPolicyEngine engine = new CountingPolicyEngine();

    @Test
    public void testGlobalScopeAuthorityGrantsWithoutConsultingRanger() throws Exception {
        Authority authority = new Authority(ADMIN);
        RangerDorisAccessController controller = rangerDeferringTo(authority, Collections.emptyMap());

        controller.checkPrivilege(ADMIN, ALLOWED_TABLE, AccessRequirements.SELECT, AccessContext.NONE);

        // Not merely an optimisation: the Ranger policy set denies this user everywhere, so had the request
        // reached the engine the answer would have been the opposite one.
        Assert.assertEquals(0, engine.requests.get());
        // And what was deferred is the privilege at instance scope, not the table.
        Assert.assertEquals(ResourceKind.GLOBAL, authority.askedAbout);
    }

    @Test
    public void testRangerDecidesWhenTheAuthorityGrantsNothingGlobally() throws Exception {
        RangerDorisAccessController controller = rangerDeferringTo(new Authority(ADMIN),
                Collections.emptyMap());

        controller.checkPrivilege(RANGER_USER, ALLOWED_TABLE, AccessRequirements.SELECT, AccessContext.NONE);
        Assert.assertThrows(AccessDeniedException.class,
                () -> controller.checkPrivilege(RANGER_USER,
                        AuthorizedResource.table("ctl", StubRangerPolicyEngine.ALLOWED_DB, "other_tbl"),
                        AccessRequirements.SELECT, AccessContext.NONE));
    }

    /**
     * A source configured not to defer answers out of its own policies alone, so an administrator of the
     * instance has no access to what it governs beyond what Ranger grants.
     */
    @Test
    public void testConfiguredNotToDeferRefusesTheGlobalScopeAuthority() throws Exception {
        Authority authority = new Authority(ADMIN);
        RangerDorisAccessController controller = rangerDeferringTo(authority, Collections.singletonMap(
                RangerAccessController.DEFER_TO_GLOBAL_SCOPE_AUTHORITY, "false"));

        Assert.assertThrows(AccessDeniedException.class,
                () -> controller.checkPrivilege(ADMIN, ALLOWED_TABLE, AccessRequirements.SELECT,
                        AccessContext.NONE));
        Assert.assertNull("the authority must not even be asked", authority.askedAbout);
        Assert.assertNotEquals("Ranger has to be the one deciding", 0, engine.requests.get());
    }

    @Test
    public void testUnreadableDeferenceSettingIsRejected() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> rangerDeferringTo(new Authority(ADMIN), Collections.singletonMap(
                        RangerAccessController.DEFER_TO_GLOBAL_SCOPE_AUTHORITY, "no")));
    }

    /**
     * With Ranger installed for the instance, the source is itself the authority and its own global check
     * already answers the question - asking through the engine would evaluate the same Ranger policies a
     * second time on every database, table and column check.
     */
    @Test
    public void testBeingTheAuthorityCostsNoExtraPolicyEvaluation() throws Exception {
        int asCatalogSource = requestsWhile(new Authority(ADMIN));
        int asAuthority = requestsWhile(null);

        Assert.assertEquals(asCatalogSource, asAuthority);
    }

    private int requestsWhile(Authority authority) throws Exception {
        engine.requests.set(0);
        RangerDorisAccessController controller = rangerDeferringTo(authority, Collections.emptyMap());
        try {
            controller.checkPrivilege(RANGER_USER, AuthorizedResource.database("ctl", "other_db"),
                    AccessRequirements.SELECT, AccessContext.NONE);
        } catch (AccessDeniedException expected) {
            // The point is the number of evaluations, not the verdict.
        }
        return engine.requests.get();
    }

    /**
     * Stands a Ranger source up the way the engine does: with a context of the engine's own, on an FE whose
     * {@code access_controller_type} resolves to {@code authority} - or to the Ranger source itself when
     * there is no other authority.
     */
    private RangerDorisAccessController rangerDeferringTo(AuthorizationPlugin authority,
            Map<String, String> properties) {
        AccessControllerManager manager = new AccessControllerManager(new Auth());
        EngineAuthorizationContext context = new EngineAuthorizationContext(manager, manager.getAuth());
        RangerDorisAccessController controller = new RangerDorisAccessController(engine, properties, context);
        context.servedBy(controller);
        Deencapsulation.setField(manager, "defaultAccessController",
                authority == null ? controller : authority);
        return controller;
    }
}
