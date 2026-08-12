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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.authorizer.ranger.doris.RangerDorisAccessController;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.CatalogAccessController;
import org.apache.doris.mysql.privilege.LegacyAccessControllerPlugin;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.StubRangerPolicyEngine;

import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

/**
 * A catalog bound to Ranger still lets through whoever holds the privilege at global scope.
 *
 * <p>Global scope is not something a Ranger service knows about; it belongs to the controller named by
 * {@code access_controller_type}. Honouring it is why a cluster administrator can still reach a Ranger-governed
 * catalog after the engine stopped establishing privileges on a plugin's behalf. It is the plugin's own choice,
 * so it is tested on the plugin, and a plugin that declines it stays a legal plugin.
 */
public class RangerGlobalScopeDeferenceTest {
    private static final UserIdentity ADMIN = UserIdentity.createAnalyzedUserIdentWithIp("admin_user", "%");
    private static final UserIdentity RANGER_USER =
            UserIdentity.createAnalyzedUserIdentWithIp(StubRangerPolicyEngine.ALLOWED_USER, "%");

    /** Counts what actually reached the policy engine, so "answered without asking Ranger" is observable. */
    private static final class CountingPolicyEngine extends StubRangerPolicyEngine {
        private final AtomicInteger requests = new AtomicInteger();

        @Override
        public RangerAccessResult isAccessAllowed(RangerAccessRequest request) {
            requests.incrementAndGet();
            return super.isAccessAllowed(request);
        }
    }

    private final CountingPolicyEngine engine = new CountingPolicyEngine();
    private final RangerDorisAccessController controller = new RangerDorisAccessController(engine);

    @Test
    public void testGlobalScopeAuthorityGrantsWithoutConsultingRanger() {
        CatalogAccessController authority = Mockito.mock(CatalogAccessController.class);
        Mockito.when(authority.checkGlobalPriv(ADMIN, PrivPredicate.SELECT)).thenReturn(true);

        boolean allowed = withGlobalScopeAuthority(authority,
                () -> controller.checkTblPriv(ADMIN, "ctl", StubRangerPolicyEngine.ALLOWED_DB,
                        StubRangerPolicyEngine.ALLOWED_TABLE, PrivPredicate.SELECT));

        Assert.assertTrue(allowed);
        // Not merely an optimisation: the Ranger policy set denies this user everywhere, so had the request
        // reached the engine the answer would have been the opposite one.
        Assert.assertEquals(0, engine.requests.get());
    }

    @Test
    public void testRangerDecidesWhenTheAuthorityGrantsNothingGlobally() {
        CatalogAccessController authority = Mockito.mock(CatalogAccessController.class);

        Assert.assertTrue(withGlobalScopeAuthority(authority,
                () -> controller.checkTblPriv(RANGER_USER, "ctl", StubRangerPolicyEngine.ALLOWED_DB,
                        StubRangerPolicyEngine.ALLOWED_TABLE, PrivPredicate.SELECT)));
        Assert.assertFalse(withGlobalScopeAuthority(authority,
                () -> controller.checkTblPriv(RANGER_USER, "ctl", StubRangerPolicyEngine.ALLOWED_DB,
                        "other_tbl", PrivPredicate.SELECT)));
    }

    /**
     * With Ranger installed globally, the controller is itself the authority and its own global check already
     * answers the question - asking through the manager would evaluate the same Ranger policies a second time
     * on every database, table and column check.
     */
    @Test
    public void testBeingTheAuthorityCostsNoExtraPolicyEvaluation() {
        int asPlugin = requestsWhile(Mockito.mock(CatalogAccessController.class),
                () -> controller.checkDbPriv(RANGER_USER, "ctl", "other_db", PrivPredicate.SELECT));
        int asAuthority = requestsWhile(controller,
                () -> controller.checkDbPriv(RANGER_USER, "ctl", "other_db", PrivPredicate.SELECT));

        Assert.assertEquals(asPlugin, asAuthority);
    }

    private int requestsWhile(CatalogAccessController authority, BooleanSupplier check) {
        engine.requests.set(0);
        withGlobalScopeAuthority(authority, check);
        return engine.requests.get();
    }

    /**
     * Runs {@code check} against an FE whose {@code access_controller_type} resolves to {@code authority}.
     *
     * <p>Installed the way the engine installs one written against the older interface - behind the adapter -
     * because that is what makes "is this authority me?" a question about the controller rather than about
     * the object the manager happens to hold.
     */
    private boolean withGlobalScopeAuthority(CatalogAccessController authority, BooleanSupplier check) {
        AccessControllerManager manager = new AccessControllerManager(new Auth());
        Deencapsulation.setField(manager, "defaultAccessController",
                new LegacyAccessControllerPlugin("authority", authority));
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getAccessManager()).thenReturn(manager);
            return check.getAsBoolean();
        }
    }
}
