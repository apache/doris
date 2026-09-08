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

package org.apache.doris.qe;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/**
 * A session-narrowed (SU) session forwarding a statement to the master: the origin FE carries the
 * session's active role subset in TMasterOpRequest (is_su_user + current_roles) and the master
 * installs it on the proxy session before executing, so the statement is authorized against exactly
 * that set and never the target's full role union.
 */
public class ForwardedSessionNarrowingTest extends TestWithFeService {

    private static final String CTL = InternalCatalog.INTERNAL_CATALOG_NAME;

    @Override
    protected void runBeforeAll() throws Exception {
        // GRANT validates database existence on the internal catalog
        createDatabase("test");
        createDatabase("perso");
    }

    private UserIdentity ident(String name) {
        return UserIdentity.createAnalyzedUserIdentWithIp(name, "%");
    }

    private boolean canSelectDb(UserIdentity user, String db) {
        return Env.getCurrentEnv().getAccessManager().checkDbPriv(user, CTL, db, PrivPredicate.SELECT);
    }

    private static TMasterOpRequest forwardRequestOf(ConnectContext ctx) throws AnalysisException {
        return new ForwardParamsProbe(ctx).build();
    }

    /** Exposes the request the origin FE would send for this session. */
    private static class ForwardParamsProbe extends FEOpExecutor {
        private ForwardParamsProbe(ConnectContext ctx) {
            super(new TNetworkAddress("127.0.0.1", 9020), new OriginStatement("show frontends", 0), ctx, true);
        }

        private TMasterOpRequest build() throws AnalysisException {
            return buildStmtForwardParams();
        }
    }

    @Test
    public void testOriginCarriesNarrowingOnlyWhenSwitched() throws Exception {
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(ident("alice"));
        ctx.setRemoteIP("127.0.0.1");

        TMasterOpRequest plain = forwardRequestOf(ctx);
        Assert.assertFalse(plain.isSetIsSuUser());
        Assert.assertFalse(plain.isSetCurrentRoles());

        ctx.setSessionRoleOverride(Sets.newHashSet("tenant_a", "tenant_a_scoped"));
        TMasterOpRequest narrowed = forwardRequestOf(ctx);
        Assert.assertTrue(narrowed.isSetIsSuUser() && narrowed.isIsSuUser());
        Assert.assertEquals(Sets.newHashSet("tenant_a", "tenant_a_scoped"), narrowed.getCurrentRoles());
        // the identity the master rebuilds is still the session's (switched-to) identity
        Assert.assertEquals(ctx.getQualifiedUser(), narrowed.getUser());
    }

    @Test
    public void testMasterInstallsCarriedNarrowingOnProxySession() throws Exception {
        addUser("bob", true);
        createRole("tenant_b");
        grantPriv("GRANT SELECT_PRIV ON internal.test.* TO ROLE 'tenant_b';");
        grantRole("GRANT 'tenant_b' TO 'bob'@'%'");
        // a personal (default-role) grant the narrowing must drop on the master as well
        grantPriv("GRANT SELECT_PRIV ON internal.perso.* TO 'bob'@'%';");
        UserIdentity bob = ident("bob");

        TMasterOpRequest request = new TMasterOpRequest();
        request.setIsSuUser(true);
        request.setCurrentRoles(Sets.newHashSet("tenant_b"));

        ConnectContext proxy = new ConnectContext();
        proxy.setCurrentUserIdentity(bob);
        ConnectProcessor.applyForwardedSessionNarrowing(proxy, request);
        Assert.assertEquals(Collections.singleton("tenant_b"), proxy.getSessionRoleOverride());

        proxy.setThreadLocalInfo();
        try {
            Assert.assertFalse(canSelectDb(bob, "perso")); // personal grant dropped
            Assert.assertTrue(canSelectDb(bob, "test"));   // carried role kept
        } finally {
            connectContext.setThreadLocalInfo();
        }
        // the same identity checked outside the proxy session sees its full union
        Assert.assertTrue(canSelectDb(bob, "perso"));
    }

    @Test
    public void testMasterFailsClosedWhenSwitchedRequestCarriesNoRoles() {
        TMasterOpRequest request = new TMasterOpRequest();
        request.setIsSuUser(true);
        ConnectContext proxy = new ConnectContext();
        ConnectProcessor.applyForwardedSessionNarrowing(proxy, request);
        Assert.assertNotNull(proxy.getSessionRoleOverride());
        Assert.assertTrue(proxy.getSessionRoleOverride().isEmpty());
    }

    @Test
    public void testUnswitchedRequestLeavesProxySessionUnnarrowed() {
        ConnectContext proxy = new ConnectContext();
        ConnectProcessor.applyForwardedSessionNarrowing(proxy, new TMasterOpRequest());
        Assert.assertNull(proxy.getSessionRoleOverride());

        // a role list without the switched flag is not a narrowing
        TMasterOpRequest rolesOnly = new TMasterOpRequest();
        rolesOnly.setCurrentRoles(Sets.newHashSet("tenant_c"));
        ConnectProcessor.applyForwardedSessionNarrowing(proxy, rolesOnly);
        Assert.assertNull(proxy.getSessionRoleOverride());
    }

    @Test
    public void testMasterItselfAlwaysRunsTheSameBuild() {
        // the single-FE test environment is its own master: nothing to forward, nothing to refuse
        Assert.assertTrue(Env.getCurrentEnv().isMaster());
        Assert.assertTrue(Env.getCurrentEnv().masterRunsSameBuild());
    }
}
