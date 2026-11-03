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
import org.apache.doris.common.AuthorizationException;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.mysql.MysqlCapability;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.HbPackage;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.FrontendHbResponse;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
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
        return new ForwardParamsProbe(ctx, selfAddress()).build();
    }

    /** The single test FE, as an FE address: the one receiver that is known to run this build. */
    private static TNetworkAddress selfAddress() {
        return new TNetworkAddress(Env.getCurrentEnv().getSelfNode().getHost(), Config.rpc_port);
    }

    /** Exposes the request the origin FE would send for this session to the FE at the address. */
    private static class ForwardParamsProbe extends FEOpExecutor {
        private ForwardParamsProbe(ConnectContext ctx, TNetworkAddress receiver) {
            super(receiver, new OriginStatement("show frontends", 0), ctx, true);
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
        // the forward request carries the client's negotiated capabilities; a bare test context has none
        ctx.setCapability(MysqlCapability.DEFAULT_CAPABILITY);

        TMasterOpRequest plain = forwardRequestOf(ctx);
        Assertions.assertFalse(plain.isSetIsSuUser());
        Assertions.assertFalse(plain.isSetCurrentRoles());
        Assertions.assertFalse(plain.isSetAuthenticatedUserIdent());

        ctx.setAuthenticatedIdentity(ident("svc"));
        ctx.setSessionRoleOverride(Sets.newHashSet("tenant_a", "tenant_a_scoped"));
        TMasterOpRequest narrowed = forwardRequestOf(ctx);
        Assertions.assertTrue(narrowed.isSetIsSuUser() && narrowed.isIsSuUser());
        Assertions.assertEquals(Sets.newHashSet("tenant_a", "tenant_a_scoped"), narrowed.getCurrentRoles());
        // the identity the master rebuilds is still the session's (switched-to) identity ...
        Assertions.assertEquals(ctx.getQualifiedUser(), narrowed.getUser());
        // ... and the account that authenticated the connection rides along for the master's audit row
        Assertions.assertEquals(ident("svc").toThrift(), narrowed.getAuthenticatedUserIdent());
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
        request.setAuthenticatedUserIdent(ident("svc").toThrift());

        ConnectContext proxy = new ConnectContext();
        proxy.setCurrentUserIdentity(bob);
        ConnectProcessor.applyForwardedSessionNarrowing(proxy, request);
        Assertions.assertEquals(Collections.singleton("tenant_b"), proxy.getSessionRoleOverride());
        // the master's audit row names the switcher, the effective identity stays the target
        Assertions.assertEquals("svc", proxy.getAuthenticatedIdentity().getQualifiedUser());
        Assertions.assertEquals("bob", proxy.getCurrentUserIdentity().getQualifiedUser());

        proxy.setThreadLocalInfo();
        try {
            Assertions.assertFalse(canSelectDb(bob, "perso")); // personal grant dropped
            Assertions.assertTrue(canSelectDb(bob, "test"));   // carried role kept
        } finally {
            connectContext.setThreadLocalInfo();
        }
        // the same identity checked outside the proxy session sees its full union
        Assertions.assertTrue(canSelectDb(bob, "perso"));
    }

    /**
     * A switched request that names no role subset (or an empty one) is refused, never narrowed to
     * nothing: an installed override is never empty, so "absent = not narrowed" is the one meaning of
     * current_roles on every side of the wire.
     */
    @Test
    public void testMasterRefusesASwitchedRequestWithoutRoles() {
        TMasterOpRequest noRoles = new TMasterOpRequest();
        noRoles.setIsSuUser(true);
        ConnectContext proxy = new ConnectContext();
        Assertions.assertThrows(AuthorizationException.class,
                () -> ConnectProcessor.applyForwardedSessionNarrowing(proxy, noRoles));
        Assertions.assertFalse(proxy.isSessionRoleNarrowed());

        TMasterOpRequest emptyRoles = new TMasterOpRequest();
        emptyRoles.setIsSuUser(true);
        emptyRoles.setCurrentRoles(Sets.newHashSet());
        Assertions.assertThrows(AuthorizationException.class,
                () -> ConnectProcessor.applyForwardedSessionNarrowing(proxy, emptyRoles));
        Assertions.assertFalse(proxy.isSessionRoleNarrowed());
    }

    @Test
    public void testUnswitchedRequestLeavesProxySessionUnnarrowed() throws Exception {
        ConnectContext proxy = new ConnectContext();
        ConnectProcessor.applyForwardedSessionNarrowing(proxy, new TMasterOpRequest());
        Assertions.assertNull(proxy.getSessionRoleOverride());
        Assertions.assertNull(proxy.getAuthenticatedIdentity());

        // a role list without the switched flag is not a narrowing
        TMasterOpRequest rolesOnly = new TMasterOpRequest();
        rolesOnly.setCurrentRoles(Sets.newHashSet("tenant_c"));
        ConnectProcessor.applyForwardedSessionNarrowing(proxy, rolesOnly);
        Assertions.assertNull(proxy.getSessionRoleOverride());
    }

    @Test
    public void testMasterItselfAlwaysRunsTheSameBuild() {
        // the single-FE test environment is its own master: nothing to forward, nothing to refuse
        Assertions.assertTrue(Env.getCurrentEnv().isMaster());
        Assertions.assertTrue(Env.getCurrentEnv().masterRunsSameBuild());
        TNetworkAddress self = selfAddress();
        Assertions.assertTrue(Env.getCurrentEnv().frontendRunsSameBuild(self.getHostname(), self.getPort()));
        // an FE this FE does not know counts as a different build
        Assertions.assertFalse(Env.getCurrentEnv().frontendRunsSameBuild("203.0.113.9", 9020));
    }

    /**
     * The same-build gate sits at the single place that sets is_su_user, so every SQL forward of a
     * narrowed session -- to the master, or for KILL QUERY to any other FE -- fails closed unless the
     * receiving FE is known to run this build. A plain session forwards anywhere.
     */
    @Test
    public void testNarrowedForwardToAnFeOfUnknownBuildIsRefused() throws Exception {
        TNetworkAddress unknown = new TNetworkAddress("203.0.113.9", 9020);
        ConnectContext ctx = new ConnectContext();
        ctx.setCurrentUserIdentity(ident("alice"));
        ctx.setRemoteIP("127.0.0.1");
        ctx.setCapability(MysqlCapability.DEFAULT_CAPABILITY);
        Assertions.assertFalse(new ForwardParamsProbe(ctx, unknown).build().isSetIsSuUser());

        ctx.setAuthenticatedIdentity(ident("svc"));
        ctx.setSessionRoleOverride(Collections.singleton("tenant_a"));
        AnalysisException refused = Assertions.assertThrows(AnalysisException.class,
                () -> new ForwardParamsProbe(ctx, unknown).build());
        Assertions.assertTrue(refused.getMessage().contains("different build"), refused.getMessage());
        // the FE known to run this build still takes the narrowed forward
        Assertions.assertTrue(forwardRequestOf(ctx).isIsSuUser());
    }

    /**
     * A non-master FE learns the master's build only from the HbPackage journal the master writes
     * after every heartbeat round: the version must survive the Gson round trip of that journal and
     * land on the replayed Frontend, or masterRunsSameBuild() is false off-master forever.
     */
    @Test
    public void testHeartbeatJournalCarriesTheBuildToReplayingFrontends() {
        String build = Frontend.localBuildVersion();
        FrontendHbResponse hb = new FrontendHbResponse("fe_probe", 9030, 9020, 0, 7L, System.currentTimeMillis(),
                build, 0L, Collections.emptyList(), 1L, "");
        FrontendHbResponse roundTripped = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(hb),
                FrontendHbResponse.class);
        Assertions.assertEquals(build, roundTripped.getVersion());

        HbPackage hbPackage = new HbPackage();
        hbPackage.addHbResponse(hb);
        HbPackage replayed = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(hbPackage), HbPackage.class);
        Assertions.assertEquals(1, replayed.getHbResults().size());
        FrontendHbResponse fromJournal = (FrontendHbResponse) replayed.getHbResults().get(0);

        // what a follower's HeartbeatMgr.replayHearbeat does with the journaled response
        Frontend fe = new Frontend(FrontendNodeType.FOLLOWER, "fe_probe", "203.0.113.9", 9010);
        Assertions.assertTrue(fe.handleHbResponse(fromJournal, true));
        Assertions.assertTrue(fe.isAlive());
        Assertions.assertEquals(build, fe.getVersion());
        Assertions.assertEquals(9020, fe.getRpcPort());
    }
}
