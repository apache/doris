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

package org.apache.doris.authorizationexample;

import org.apache.doris.authorization.spi.AuthorizationPlugin;
import org.apache.doris.authorization.spi.AuthorizationPluginFactory;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.extension.loader.ApiVersionGate;
import org.apache.doris.extension.loader.PluginRegistry;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.PluginJarWriter;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * An authorization source shipped as a jar, installed the way a third party installs one, really decides
 * what a running FE allows.
 *
 * <p>Everything before this test stops one step short of that. The loader tests prove a plugin directory is
 * swept and a version gate applied; the wiring test proves the source named in {@code fe.conf} is the one
 * the manager reports. None of them puts a statement through the engine, so none of them would notice if a
 * plugin were installed and then never asked, or asked and then overruled.
 *
 * <p>So the whole chain runs here: a jar written into {@code authorization_plugins_dir}, discovered at
 * startup, admitted on the plugin API version it declares, loaded through its own classloader, built by its
 * factory - and then three things are checked that are only observable from SQL:
 *
 * <ul>
 *   <li>an account the built-in model granted nothing can read a table, because the plugin allows it;</li>
 *   <li>an account the built-in model granted {@code SELECT} on that same table cannot, because the plugin
 *       does not - the source governing a resource is the whole answer for it;</li>
 *   <li>the row filter the plugin returns is planned over the table, for the account it applies to and not
 *       for the one it does not;</li>
 *   <li>the column mask the plugin returns is projected over the table, the same way and with the same
 *       control.</li>
 * </ul>
 *
 * <p>The second of those records today's behaviour deliberately: {@code GRANT} still succeeds under an
 * external source and is then ignored. Making it an error is later work, and this test is where that change
 * will announce itself.
 *
 * <p>What is not covered here, honestly: the properties an instance-wide source is configured with.
 * {@link ExampleAuthorizationPlugin} runs on its defaults, and it does so because this test points
 * {@code authorization_config_file_path} at a file that does not exist - the run does export
 * {@code DORIS_HOME} (see {@code run-fe-ut.sh}), so leaving the path at its default would read whatever
 * {@code <repo-root>/conf/authorization.conf} happens to hold on the machine running the tests. The
 * per-catalog property channel is a catalog property and is exercised by the Ranger tests.
 */
public class AuthorizationPluginFromDirectoryTest extends TestWithFeService {

    private static final String DB = "example_db";
    private static final String TBL = "sales";
    private static final String QUALIFIED_TBL = InternalCatalog.INTERNAL_CATALOG_NAME + "." + DB + "." + TBL;

    /** Holds the role the example source grants reading to, and nothing the built-in model ever granted. */
    private static final String READER = "example_reader_user";
    /** Holds the role the example source lets read without a row filter; the control for the filter case. */
    private static final String AUDITOR = "example_auditor_user";
    /** Holds a real built-in SELECT grant on the table, and no role the example source has heard of. */
    private static final String GRANTED_BY_DORIS = "granted_by_doris_user";

    private Path pluginRoot;
    private String originalPluginsDir;
    private String originalControllerType;
    private String originalConfigFilePath;

    /**
     * Runs before the FE exists, which is the only moment this is configurable: the manager reading
     * {@code access_controller_type} is built once, while the FE starts.
     */
    @Override
    protected void beforeCreatingConnectContext() throws Exception {
        ApiVersionGate gate = ApiVersionGate.forFamily("authorization", AuthorizationPluginFactory.class);
        pluginRoot = Files.createTempDirectory("authorization-plugin-e2e");
        PluginJarWriter.writePluginRoot(pluginRoot, ExampleAuthorizationPluginFactory.NAME,
                ExampleAuthorizationPluginFactory.class, AuthorizationPluginFactory.class,
                gate.getManifestAttribute(), gate.getExpectedVersion(),
                Collections.singletonList(ExampleAuthorizationPlugin.class));

        originalPluginsDir = Config.authorization_plugins_dir;
        originalControllerType = Config.access_controller_type;
        originalConfigFilePath = Config.authorization_config_file_path;
        Config.authorization_plugins_dir = pluginRoot.toString();
        Config.access_controller_type = ExampleAuthorizationPluginFactory.NAME;
        // Named so it cannot exist: the source has to run on its defaults for the assertions below to mean
        // what they say, and the default path is a real file an FE checkout may well have.
        Config.authorization_config_file_path = "/conf/authorization-absent-in-this-test.conf";
    }

    /**
     * Everything here runs as {@code root}, which the example source lets through because root holds the
     * Doris role it treats as its administrator. That is not a detail of the test: a source installed for
     * the whole instance answers for administration too, so one without an admin rule of its own would make
     * the FE unadministrable from its first statement.
     */
    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase(DB);
        useDatabase(DB);
        // Region, not region: the source writes its policies against "region" and keys its answer by the
        // lower-cased name, while the slot names a query carries come from this schema. Declared in one
        // case throughout, both Locale.ROOT foldings could be deleted and every case here would still
        // pass - which is the whole of what they are for.
        createTable("create table " + TBL + " (id int, Region varchar(8))"
                + " distributed by hash(id) buckets 1 properties(\"replication_num\" = \"1\");");

        createRole(ExampleAuthorizationPlugin.DEFAULT_READER_ROLE);
        addUser(READER, true);
        grantRole("GRANT '" + ExampleAuthorizationPlugin.DEFAULT_READER_ROLE + "' TO '" + READER + "'@'%'");

        createRole(ExampleAuthorizationPlugin.DEFAULT_AUDITOR_ROLE);
        addUser(AUDITOR, true);
        grantRole("GRANT '" + ExampleAuthorizationPlugin.DEFAULT_AUDITOR_ROLE + "' TO '" + AUDITOR + "'@'%'");

        addUser(GRANTED_BY_DORIS, true);
        grantPriv("GRANT SELECT_PRIV ON " + QUALIFIED_TBL + " TO '" + GRANTED_BY_DORIS + "'@'%'");
    }

    @Override
    protected void runBeforeEach() throws Exception {
        // Each test names the account it is about; start from root so a leftover identity cannot make one
        // of them pass for the wrong reason.
        useUser("root");
    }

    @Override
    protected void runAfterAll() throws Exception {
        Config.authorization_plugins_dir = originalPluginsDir;
        Config.access_controller_type = originalControllerType;
        Config.authorization_config_file_path = originalConfigFilePath;
        // loadPlugins writes rows into a process-wide registry backing information_schema.extensions;
        // leaving them there would make other classes' assertions depend on execution order.
        PluginRegistry.getInstance().clearForTest();
    }

    @Test
    public void theSourceGoverningThisInstanceIsTheOneFromThePluginDirectory() {
        AuthorizationPlugin governing = Env.getCurrentEnv().getAccessManager()
                .getAccessControllerOrDefault(InternalCatalog.INTERNAL_CATALOG_NAME);

        Assertions.assertEquals(ExampleAuthorizationPluginFactory.NAME, governing.name());
        // Without this, every assertion below would hold just as well with the example loaded off the test
        // class path - and then nothing here would be about installing a plugin at all.
        Assertions.assertNotSame(getClass().getClassLoader(), governing.getClass().getClassLoader(),
                "the example was loaded through the FE's own classloader, so it did not come from the jar");
    }

    @Test
    public void anAccountTheBuiltInModelGrantedNothingReadsWhatThePluginAllows() throws Exception {
        useUser(READER);

        // No GRANT was ever issued to this account. If anything but the plugin had a say, this would fail.
        Plan plan = rewrite("select id, region from " + QUALIFIED_TBL);

        Assertions.assertNotNull(plan);
    }

    @Test
    public void builtInGrantsDoNotCountOnceThePluginGoverns() throws Exception {
        useUser(GRANTED_BY_DORIS);

        AnalysisException refused = Assertions.assertThrows(AnalysisException.class,
                () -> rewrite("select id, region from " + QUALIFIED_TBL),
                "a built-in GRANT was honoured while an external source governs the table; that source's"
                        + " answer is supposed to be the whole answer");

        // Naming the source is the difference between an operator finding the policy that refused and
        // hunting through a privilege model that has nothing to do with the decision.
        Assertions.assertTrue(refused.getMessage().contains(ExampleAuthorizationPluginFactory.NAME),
                "the refusal does not say which source refused: " + refused.getMessage());
    }

    @Test
    public void theRowFilterTheSourceReturnsIsPlannedOverTheTable() throws Exception {
        useUser(READER);
        List<Expression> readerFilters = filterConjunctsOf(rewrite("select id, region from " + QUALIFIED_TBL));

        Assertions.assertTrue(readerFilters.stream().anyMatch(this::isTheExampleRowFilter),
                "the row filter the source returned never reached the plan: " + readerFilters);

        // The same query by an account the source imposes no filter on. Without this the assertion above
        // would also pass if every plan carried the predicate regardless of who asked.
        //
        // Deliberately not root: the planner exempts the literal root and admin accounts from every row filter
        // and column mask before any source is asked (LogicalCheckPolicy#findPolicy), so with root here this
        // would hold for any implementation whatsoever - including one that returns the same filter to
        // everybody. The auditor account is one the source itself answers about, and answers differently.
        useUser(AUDITOR);
        List<Expression> auditorFilters = filterConjunctsOf(rewrite("select id, region from " + QUALIFIED_TBL));

        Assertions.assertTrue(auditorFilters.stream().noneMatch(this::isTheExampleRowFilter),
                "an account the source returns no filter for was filtered anyway: " + auditorFilters);
    }

    /**
     * The column mask the source returns is planned as a projection over the table.
     *
     * <p>Masks reach the planner by a different route from row filters - a batch call keyed by column name,
     * folded with {@code Locale.ROOT} on both sides - and nothing else exercises that route end to end: every
     * other masking test either stubs the manager out or stops short of the planner, so the whole of it could
     * be removed and stay green. The column is named in mixed case on purpose, since matching it is what the
     * case folding is for.
     *
     * <p>With the same negative control the row filter case has: an account this source masks nothing for
     * must read the column unchanged, or an implementation masking everything for everybody would pass too.
     */
    @Test
    public void theColumnMaskTheSourceReturnsIsProjectedOverTheTable() throws Exception {
        useUser(READER);
        Plan masked = rewrite("select id, REGION from " + QUALIFIED_TBL);

        Assertions.assertTrue(projectionsOf(masked).stream().anyMatch(this::isTheExampleColumnMask),
                "the column mask the source returned never reached the plan: " + projectionsOf(masked));

        useUser(AUDITOR);
        Plan unmasked = rewrite("select id, REGION from " + QUALIFIED_TBL);

        Assertions.assertTrue(projectionsOf(unmasked).stream().noneMatch(this::isTheExampleColumnMask),
                "an account the source masks nothing for was masked anyway: " + projectionsOf(unmasked));
    }

    private Plan rewrite(String sql) {
        return PlanChecker.from(connectContext).parse(sql).analyze().rewrite().getPlan();
    }

    private List<NamedExpression> projectionsOf(Plan plan) {
        List<NamedExpression> projections = new ArrayList<>();
        for (Object node : plan.<Plan>collectToList(LogicalProject.class::isInstance)) {
            projections.addAll(((LogicalProject<?>) node).getProjects());
        }
        return projections;
    }

    /**
     * Matches the masked form of {@code region} - an alias standing in for the column, carrying the mask's
     * own marker text.
     *
     * <p>Matched on the marker rather than on {@code concat(...)}: the row filter this same source imposes
     * pins {@code region} to one value, so the rewrite folds the mask expression to a literal. That the
     * expression was folded is beside the point - what this case is about is that the column reaching the
     * plan is the source's rewriting of it and not the column itself.
     */
    private boolean isTheExampleColumnMask(NamedExpression projection) {
        return projection instanceof Alias
                && "region".equalsIgnoreCase(projection.getName())
                && projection.child(0).toSql().contains("***");
    }

    private List<Expression> filterConjunctsOf(Plan plan) {
        List<Expression> conjuncts = new ArrayList<>();
        for (Object node : plan.<Plan>collectToList(LogicalFilter.class::isInstance)) {
            conjuncts.addAll(((LogicalFilter<?>) node).getConjuncts());
        }
        return conjuncts;
    }

    /** Matches {@code region = 'EU'}, whatever the planner has renamed or re-typed around it. */
    private boolean isTheExampleRowFilter(Expression conjunct) {
        if (!(conjunct instanceof EqualTo)) {
            return false;
        }
        EqualTo equalTo = (EqualTo) conjunct;
        // Case-insensitively: the policy text names the column in lower case and the schema declares it
        // in mixed case, and a bound slot renders as the schema spells it.
        return equalTo.left().toSql().toLowerCase(Locale.ROOT).contains("region")
                && equalTo.right() instanceof Literal
                && "EU".equals(((Literal) equalTo.right()).getStringValue());
    }
}
