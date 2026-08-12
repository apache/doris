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
import org.apache.doris.authorization.DataMaskSpec;
import org.apache.doris.authorization.RowFilterSpec;
import org.apache.doris.authorization.spi.AuthorizationPlugin;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.authorizer.ranger.doris.RangerDorisAccessController;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.test.TestExternalCatalog.TestCatalogProvider;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.GrantResourcePrivilegeCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.resource.workloadgroup.WorkloadGroupMgr;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

/**
 * Records, in one golden file, every decision {@link AccessControllerManager} makes over the matrix
 * (resource kind x action x built-in/Ranger x privilege level of the caller).
 *
 * <p><b>Why this exists.</b> It was recorded ahead of two reworks that were meant to change structure and
 * nothing else - dropping the {@code hasGlobal} argument from the controller interface, and collapsing the
 * manager into pure routing with no cross-plugin OR - because "nothing else" is only checkable against a
 * recording made beforehand. Both have since landed against it unchanged. The rest of the plugin work moves
 * the same decisions around further, so the rule stands: after a structural change, {@code git diff} on the
 * baseline must be empty.
 *
 * <p><b>What is real and what is faked.</b> The built-in half is entirely real - a real FE, real users, real
 * {@code GRANT} statements, a real row policy - because a baseline built on hand-poked internal state would
 * freeze this test's assumptions rather than the product's behaviour. The Ranger half runs the production
 * {@link org.apache.doris.catalog.authorizer.ranger.doris.RangerDorisAccessController} over the deterministic
 * {@link StubRangerPolicyEngine}; only the policy engine is a stub.
 *
 * <p><b>Reading a row.</b> {@code <default controller> | <scope> | <user> | <probe> | <verdict>}. For the
 * check probes the verdict lists exactly the actions that were allowed, so a single flipped cell shows up as
 * one action name appearing or disappearing on one line. {@code -} means "nothing allowed" / "no policy".
 * Scope {@code -} marks the system-level resources, which are always answered by the default controller.
 *
 * <p><b>Deliberately frozen oddities.</b> Some rows record behaviour that looks wrong and is kept anyway,
 * because Phase 0's contract is structure-only:
 * <ul>
 *   <li>{@code node_user} holds only global NODE_PRIV, and {@link Auth} refuses NODE privileges at
 *       catalog/database/table level - yet the built-in controller's global check lets OPERATOR through
 *       there;</li>
 *   <li>{@code admin_user} is denied by Ranger everywhere, yet passes on the Ranger-governed catalog, because
 *       the Ranger controller defers to whichever controller owns global scope;</li>
 *   <li>a built-in per-table GRANT on a Ranger-governed catalog is ignored ({@code local_user} on
 *       {@code ext_ranger}), because that deference exists only at the global level;</li>
 *   <li>the workload group named {@code normal} is allowed unconditionally, in both implementations;</li>
 *   <li>with {@code skip_catalog_priv_check} on, a catalog bound to an external plugin answers SELECT/SHOW
 *       with a flat yes.</li>
 * </ul>
 *
 * <p><b>Regenerating.</b> Run this test; on mismatch it writes the full current matrix next to the build
 * output and names the path. Copy it over {@code src/test/resources/access-control-behavior-baseline.txt}
 * only together with a justification for every changed line - each one is a user-visible privilege change.
 */
public class AccessControlBehaviorBaselineTest extends TestWithFeService {
    private static final String BASELINE_RESOURCE = "/access-control-behavior-baseline.txt";
    private static final Path ACTUAL_DUMP = Paths.get("target", "access-control-behavior-baseline.actual.txt");

    private static final String DB = StubRangerPolicyEngine.ALLOWED_DB;
    private static final String TBL = StubRangerPolicyEngine.ALLOWED_TABLE;
    /** Ranger grants this column to {@code ranger_user}; the built-in grants are table wide. */
    private static final String COL_ALLOWED = StubRangerPolicyEngine.ALLOWED_COLUMN;
    private static final String COL_DENIED = "pcol2";

    private static final String CTL_PLAIN = "ext_plain";
    private static final String CTL_RANGER = "ext_ranger";
    private static final List<String> CATALOGS =
            ImmutableList.of(InternalCatalog.INTERNAL_CATALOG_NAME, CTL_PLAIN, CTL_RANGER);

    private static final String ADMIN_USER = "admin_user";
    private static final String NODE_USER = "node_user";
    private static final String LOCAL_USER = "local_user";
    private static final String RANGER_USER = StubRangerPolicyEngine.ALLOWED_USER;
    private static final String NOBODY = "nobody";
    private static final List<String> USERS =
            ImmutableList.of(ADMIN_USER, NODE_USER, LOCAL_USER, RANGER_USER, NOBODY);

    /** Every {@link PrivPredicate} constant, by name. A new constant lands here on its own and turns red. */
    private static final Map<String, PrivPredicate> ACTIONS = allPrivPredicates();

    /** Metadata for the two external catalogs: only the names matter, nothing reads a row. */
    public static class BaselineCatalogProvider implements TestCatalogProvider {
        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return ImmutableMap.of(DB, ImmutableMap.of(TBL, ImmutableList.of(
                    new Column(COL_ALLOWED, PrimitiveType.INT),
                    new Column(COL_DENIED, PrimitiveType.INT))));
        }
    }

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;

        createDatabase(DB);
        useDatabase(DB);
        createTable("create table " + TBL + " (" + COL_ALLOWED + " int, " + COL_DENIED + " int)"
                + " distributed by hash(" + COL_ALLOWED + ") buckets 1"
                + " properties(\"replication_num\" = \"1\");");

        String provider = BaselineCatalogProvider.class.getName();
        createCatalog("create catalog " + CTL_PLAIN + " properties("
                + "\"type\"=\"test\", \"catalog_provider.class\"=\"" + provider + "\")");
        createCatalog("create catalog " + CTL_RANGER + " properties("
                + "\"type\"=\"test\", \"catalog_provider.class\"=\"" + provider + "\","
                + "\"" + CatalogMgr.ACCESS_CONTROLLER_CLASS_PROP + "\"=\""
                + StubRangerAccessControllerFactory.class.getName() + "\")");

        for (String user : USERS) {
            addUser(user, true);
        }
        grantPriv("GRANT ADMIN_PRIV ON *.*.* TO '" + ADMIN_USER + "'@'%'");
        grantPriv("GRANT NODE_PRIV ON *.*.* TO '" + NODE_USER + "'@'%'");
        // The same table-level grant on all three catalogs: on the Ranger-governed one it is ignored today,
        // and that asymmetry is one of the things the baseline is here to hold still.
        for (String catalog : CATALOGS) {
            grantPriv("GRANT SELECT_PRIV ON " + catalog + "." + DB + "." + TBL
                    + " TO '" + LOCAL_USER + "'@'%'");
        }
        grantResourcePriv("GRANT USAGE_PRIV ON RESOURCE '" + StubRangerPolicyEngine.ALLOWED_RESOURCE
                + "' TO '" + LOCAL_USER + "'@'%'");
        grantResourcePriv("GRANT USAGE_PRIV ON WORKLOAD GROUP '"
                + StubRangerPolicyEngine.ALLOWED_WORKLOAD_GROUP + "' TO '" + LOCAL_USER + "'@'%'");

        createPolicy("CREATE ROW POLICY builtin_row_policy ON " + DB + "." + TBL
                + " AS RESTRICTIVE TO " + LOCAL_USER + " USING (" + COL_ALLOWED + " = 1)");
    }

    @Test
    public void accessDecisionsMatchRecordedBaseline() throws Exception {
        List<String> actual = renderSnapshot();
        List<String> expected = readBaseline();

        if (!actual.equals(expected)) {
            Files.createDirectories(ACTUAL_DUMP.getParent());
            Files.write(ACTUAL_DUMP, actual, StandardCharsets.UTF_8);
        }
        Assertions.assertEquals(expected.size(), actual.size(),
                "The access-control behaviour matrix changed shape.\n" + regenerationHint());
        for (int i = 0; i < expected.size(); i++) {
            Assertions.assertEquals(expected.get(i), actual.get(i),
                    "Access-control behaviour changed at line " + (i + 1) + ".\n" + regenerationHint());
        }
    }

    private String regenerationHint() {
        return "Every differing line is a privilege decision that flipped for some user - it is a behaviour"
                + " change, not a test artifact. The current matrix was written to " + ACTUAL_DUMP.toAbsolutePath()
                + "; only copy it over src/test/resources" + BASELINE_RESOURCE + " once each changed line is"
                + " explained and intended.";
    }

    // ------------------------------------------------------------------------------------------------
    // rendering
    // ------------------------------------------------------------------------------------------------

    private List<String> renderSnapshot() {
        List<String> lines = new ArrayList<>();
        lines.add("ACTIONS: " + String.join(",", ACTIONS.keySet()));
        lines.add("USERS: " + String.join(",", USERS));
        lines.add("");
        AccessControllerManager manager = Env.getCurrentEnv().getAccessManager();
        AuthorizationPlugin builtin = Deencapsulation.getField(manager, "defaultAccessController");

        renderWithDefaultController(lines, manager, "builtin", builtin);
        renderWithDefaultController(lines, manager, "ranger", rangerInstalledForTheInstance(manager));
        return lines;
    }

    /** A Ranger source standing where {@code access_controller_type} puts one, built as the engine builds it. */
    private AuthorizationPlugin rangerInstalledForTheInstance(AccessControllerManager manager) {
        EngineAuthorizationContext context = new EngineAuthorizationContext(manager, manager.getAuth());
        RangerDorisAccessController ranger =
                new RangerDorisAccessController(new StubRangerPolicyEngine(), context);
        context.servedBy(ranger);
        return ranger;
    }

    /**
     * Renders the whole matrix under one default controller, i.e. under one value of
     * {@code fe.conf: access_controller_type}.
     */
    private void renderWithDefaultController(List<String> lines, AccessControllerManager manager,
            String label, AuthorizationPlugin defaultController) {
        AuthorizationPlugin original = Deencapsulation.getField(manager, "defaultAccessController");
        Map<String, Object> routes = Deencapsulation.getField(manager, "ctlToCtlAccessController");
        Map<String, Object> savedRoutes = new HashMap<>(routes);
        try {
            Deencapsulation.setField(manager, "defaultAccessController", defaultController);
            // Catalogs that fall back to the default keep a cached reference to the previous one; drop those
            // so they resolve against the controller under test. The bound catalog keeps its own plugin.
            routes.keySet().removeIf(ctl -> !CTL_RANGER.equals(ctl));

            renderSystemScope(lines, manager, label);
            for (String catalog : CATALOGS) {
                renderCatalogScope(lines, manager, label, catalog);
            }
        } finally {
            Deencapsulation.setField(manager, "defaultAccessController", original);
            routes.clear();
            routes.putAll(savedRoutes);
        }
    }

    /** Global / resource / workload group / storage vault / compute group: always the default controller. */
    private void renderSystemScope(List<String> lines, AccessControllerManager manager, String label) {
        addRows(lines, label, "-", "global",
                (user, action) -> outcome(manager.checkGlobalPriv(user, action)));
        addRows(lines, label, "-", "resource:res1",
                (user, action) -> outcome(manager.checkResourcePriv(user, "res1", action)));
        addRows(lines, label, "-", "workloadgrp:wg1",
                (user, action) -> outcome(manager.checkWorkloadGroupPriv(user, "wg1", action)));
        addRows(lines, label, "-", "workloadgrp:default",
                (user, action) -> outcome(
                        manager.checkWorkloadGroupPriv(user, WorkloadGroupMgr.DEFAULT_GROUP_NAME, action)));
        addRows(lines, label, "-", "vault:sv1",
                (user, action) -> outcome(manager.checkStorageVaultPriv(user, "sv1", action)));
        // checkCloudPriv is deliberately absent. Its built-in implementation reaches
        // Role.checkCloudVirtualComputeGroup, which casts the system info service to the cloud subclass
        // unconditionally, so on a non-cloud FE it throws for every caller that does not already hold a
        // global privilege. Recording that would freeze a crash, not a decision.
    }

    private void renderCatalogScope(List<String> lines, AccessControllerManager manager, String label,
            String catalog) {
        addRows(lines, label, catalog, "catalog",
                (user, action) -> outcome(manager.checkCtlPriv(user, catalog, action)));
        addRows(lines, label, catalog, "catalog:skipchk",
                (user, action) -> outcome(
                        withSkipCatalogPrivCheck(() -> manager.checkCtlPriv(user, catalog, action))));
        addRows(lines, label, catalog, "database",
                (user, action) -> outcome(manager.checkDbPriv(user, catalog, DB, action)));
        addRows(lines, label, catalog, "table",
                (user, action) -> outcome(manager.checkTblPriv(user, catalog, DB, TBL, action)));
        addRows(lines, label, catalog, "column:" + COL_ALLOWED,
                (user, action) -> checkColumn(manager, user, catalog, COL_ALLOWED, action));
        addRows(lines, label, catalog, "column:" + COL_DENIED,
                (user, action) -> checkColumn(manager, user, catalog, COL_DENIED, action));

        for (String user : USERS) {
            List<RowFilterSpec> filters = manager.evalRowFilterPolicies(identity(user), catalog, DB, TBL);
            lines.add(row(label, catalog, user, "rowfilter", filters.isEmpty() ? "-"
                    : filters.stream()
                            .map(spec -> "[" + spec.getPolicyIdent() + " | " + spec.getFilterSql()
                                    + " | " + spec.getMergeType() + "]")
                            .collect(Collectors.joining(","))));
        }
        for (String column : ImmutableList.of(COL_ALLOWED, COL_DENIED)) {
            for (String user : USERS) {
                Optional<DataMaskSpec> mask =
                        manager.evalDataMaskPolicy(identity(user), catalog, DB, TBL, column);
                lines.add(row(label, catalog, user, "mask:" + column,
                        mask.map(spec -> spec.getPolicyIdent() + " | " + spec.getMaskSql()).orElse("-")));
            }
        }
    }

    private void addRows(List<String> lines, String label, String scope, String probe, Probe decision) {
        for (String user : USERS) {
            lines.add(row(label, scope, user, probe, verdict(identity(user), decision)));
        }
    }

    /**
     * {@code <allowed actions>}, plus {@code / unsupported=<actions>} when the check refused to answer at all.
     * The third outcome is not decoration: a column check only accepts privileges that carry SELECT or LOAD,
     * so whether a caller gets a verdict or an IllegalStateException today depends on whether the manager's
     * global short circuit answered before the built-in column check was ever reached.
     */
    private String verdict(UserIdentity user, Probe probe) {
        List<String> allowed = new ArrayList<>();
        List<String> unsupported = new ArrayList<>();
        for (Map.Entry<String, PrivPredicate> action : ACTIONS.entrySet()) {
            switch (probe.evaluate(user, action.getValue())) {
                case ALLOW:
                    allowed.add(action.getKey());
                    break;
                case UNSUPPORTED:
                    unsupported.add(action.getKey());
                    break;
                default:
                    break;
            }
        }
        String verdict = allowed.isEmpty() ? "-" : String.join(",", allowed);
        return unsupported.isEmpty() ? verdict : verdict + " / unsupported=" + String.join(",", unsupported);
    }

    private static String row(String label, String scope, String user, String probe, String verdict) {
        return String.format("%-7s | %-10s | %-11s | %-17s | %s", label, scope, user, probe, verdict);
    }

    private enum Outcome { ALLOW, DENY, UNSUPPORTED }

    private interface Probe {
        Outcome evaluate(UserIdentity user, PrivPredicate action);
    }

    private static Outcome outcome(boolean allowed) {
        return allowed ? Outcome.ALLOW : Outcome.DENY;
    }

    private Outcome checkColumn(AccessControllerManager manager, UserIdentity user, String catalog, String column,
            PrivPredicate action) {
        try {
            manager.checkColumnsPriv(user, catalog, DB, TBL, ImmutableSet.of(column), action);
            return Outcome.ALLOW;
        } catch (UserException e) {
            return Outcome.DENY;
        } catch (IllegalStateException e) {
            // Auth.checkColPriv rejects any predicate that carries neither SELECT nor LOAD.
            return Outcome.UNSUPPORTED;
        }
    }

    private boolean withSkipCatalogPrivCheck(BooleanSupplier body) {
        boolean original = Config.skip_catalog_priv_check;
        Config.skip_catalog_priv_check = true;
        try {
            return body.getAsBoolean();
        } finally {
            Config.skip_catalog_priv_check = original;
        }
    }

    private static UserIdentity identity(String user) {
        return UserIdentity.createAnalyzedUserIdentWithIp(user, "%");
    }

    private static Map<String, PrivPredicate> allPrivPredicates() {
        Map<String, PrivPredicate> sorted = new TreeMap<>();
        for (Field field : PrivPredicate.class.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers()) && field.getType() == PrivPredicate.class) {
                try {
                    sorted.put(field.getName(), (PrivPredicate) field.get(null));
                } catch (IllegalAccessException e) {
                    throw new IllegalStateException("cannot read PrivPredicate." + field.getName(), e);
                }
            }
        }
        return new LinkedHashMap<>(sorted);
    }

    /** {@code GRANT ... ON RESOURCE/WORKLOAD GROUP}, which the shared helper does not run. */
    private void grantResourcePriv(String sql) throws Exception {
        LogicalPlan parsed = new NereidsParser().parseSingle(sql);
        ((GrantResourcePrivilegeCommand) parsed).run(connectContext, new StmtExecutor(connectContext, sql));
    }

    private List<String> readBaseline() throws IOException {
        List<String> lines = new ArrayList<>();
        try (InputStream in = getClass().getResourceAsStream(BASELINE_RESOURCE)) {
            Assertions.assertNotNull(in, "missing baseline resource " + BASELINE_RESOURCE);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }
        return lines;
    }
}
