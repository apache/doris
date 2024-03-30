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

package org.apache.doris.nereids.privileges;

import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AuthorizationException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.test.TestExternalCatalog.TestCatalogProvider;
import org.apache.doris.mysql.privilege.AccessControllerFactory;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.CatalogAccessController;
import org.apache.doris.mysql.privilege.DataMaskPolicy;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.RowFilterPolicy;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.pattern.GeneratedMemoPatterns;
import org.apache.doris.nereids.rules.RulePromise;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Concat;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.policy.FilterType;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import mockit.Expectations;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TestCheckPrivileges extends TestWithFeService implements GeneratedMemoPatterns {
    private static final Map<String, Map<String, List<Column>>> CATALOG_META = ImmutableMap.of(
            "test_db", ImmutableMap.of(
                    "test_tbl1", ImmutableList.of(
                            new Column("id", PrimitiveType.INT),
                            new Column("name", PrimitiveType.VARCHAR)
                    ),
                    "test_tbl2", ImmutableList.of(
                            new Column("id", PrimitiveType.INT),
                            new Column("name", PrimitiveType.VARCHAR)
                    ),
                    "test_tbl3", ImmutableList.of(
                            new Column("id", PrimitiveType.INT),
                            new Column("name", PrimitiveType.VARCHAR)
                    ),
                    "test_tbl4", ImmutableList.of(
                            new Column("id", PrimitiveType.INT),
                            new Column("name", PrimitiveType.VARCHAR)
                    )
            )
    );

    @Test
    public void testPrivilegesAndPolicies() throws Exception {
        FeConstants.runningUnitTest = true;
        String catalogProvider
                = "org.apache.doris.nereids.privileges.TestCheckPrivileges$CustomCatalogProvider";
        String accessControllerFactory
                = "org.apache.doris.nereids.privileges.TestCheckPrivileges$CustomAccessControllerFactory";

        String catalog = "custom_catalog";
        String db = "test_db";
        createCatalog("create catalog " + catalog + " properties("
                + " \"type\"=\"test\","
                + " \"catalog_provider.class\"=\"" + catalogProvider + "\","
                + " \"" + CatalogMgr.ACCESS_CONTROLLER_CLASS_PROP + "\"=\"" + accessControllerFactory + "\""
                + ")");

        createDatabase("internal_db");
        String internalDb = "internal_db";
        String table1 = "test_tbl1";
        String table2 = "test_tbl2";
        String table3 = "test_tbl3";
        String table4 = "test_tbl4";

        String view1 = "query_tbl2_view1";
        createView("create view " + internalDb + "."
                + view1 + " as select * from custom_catalog.test_db." + table2);
        String view2 = "query_tbl2_view2";
        createView("create view " + internalDb + "."
                + view2 + " as select * from custom_catalog.test_db." + table2);
        String view3 = "query_tbl2_view3";
        createView("create view " + internalDb + "."
                + view3 + " as select * from custom_catalog.test_db." + table3);
        String view4 = "query_tbl2_view4";
        createView("create view " + internalDb + "."
                + view4 + " as select * from " + internalDb + "." + view3);

        String user = "test_nereids_privilege_user";
        addUser(user, true);
        useUser(user);

        List<MakeTablePrivileges> privileges = ImmutableList.of(
                // base table privileges
                MakePrivileges.table(catalog, db, table1).allowSelectTable(user),
                MakePrivileges.table(catalog, db, table2).allowSelectColumns(user, ImmutableSet.of("id")),

                // view privileges
                MakePrivileges.table("internal", internalDb, view1).allowSelectTable(user),
                MakePrivileges.table("internal", internalDb, view2)
                        .allowSelectColumns(user, ImmutableSet.of("name")),

                MakePrivileges.table("internal", internalDb, view4)
                        .allowSelectColumns(user, ImmutableSet.of("id")),

                // data masking and row policy
                MakePrivileges.table(catalog, db, table4).allowSelectTable(user)
                        .addRowPolicy(user, "id = 1")
                        .addDataMasking(user, "id", "concat(id, '_****_', id)")
        );

        AccessControllerManager accessManager = Env.getCurrentEnv().getAccessManager();
        CatalogAccessController catalogAccessController = accessManager.getAccessControllerOrDefault(catalog);
        new Expectations(accessManager) {
            {
                accessManager.getAccessControllerOrDefault("internal");
                minTimes = 0;
                result = catalogAccessController;
            }
        };

        withPrivileges(privileges, () -> {
                // test base table
                {
                    // has table privilege
                    query("select * from custom_catalog.test_db.test_tbl1");

                    // has id column privilege
                    query("select id from custom_catalog.test_db.test_tbl2");

                    // no name column privilege, throw exception:
                    //
                    // Permission denied: user ['test_nereids_privilege_user'@'%'] does not have privilege for
                    // [priv predicate: OR, Admin_priv Select_priv ] command on
                    // [custom_catalog].[test_db].[test_tbl2].[name]
                    Assertions.assertThrows(AnalysisException.class, () ->
                            query("select * from custom_catalog.test_db.test_tbl2")
                    );

                    // no table privilege
                    Assertions.assertThrows(AnalysisException.class, () ->
                            query("select * from custom_catalog.test_db.test_tbl3")
                    );
                }

                // test view
                {
                    // has view privilege
                    query("select * from " + internalDb + "." + view1);

                    // has view name privilege
                    query("select name from " + internalDb + "." + view2);

                    // no id column privilege
                    Assertions.assertThrows(AnalysisException.class, () ->
                            query("select id from " + internalDb + "." + view2)
                    );

                    // no view privilege
                    Assertions.assertThrows(AnalysisException.class, () ->
                            query("select * from " + internalDb + "." + view3)
                    );

                    // has id column privilege
                    query("select id from " + internalDb + "." + view4);

                    // no name column privilege
                    Assertions.assertThrows(AnalysisException.class, () ->
                            query("select name from " + internalDb + "." + view4)
                    );
                }

                // test row policy with data masking
                {
                    Function<NamedExpression, Boolean> checkId = (NamedExpression ne) -> {
                        if (!(ne instanceof Alias) || !ne.getName().equals("id")) {
                            return false;
                        }
                        return ne.child(0) instanceof Concat;
                    };
                    PlanChecker.from(connectContext)
                            .parse("select id,"
                                    + "  test_tbl4.id,"
                                    + "  test_db.test_tbl4.id, "
                                    + "  custom_catalog.test_db.test_tbl4.id, "
                                    + "  * "
                                    + "from custom_catalog.test_db.test_tbl4")
                            .analyze()
                            .rewrite()
                            .matches(logicalProject(
                                    logicalFilter(
                                        logicalTestScan()
                                    ).when(f -> {
                                        EqualTo predicate = (EqualTo) f.getPredicate();
                                        return predicate.left() instanceof Slot
                                                && predicate.right().equals(new IntegerLiteral((byte) 1));
                                    })
                            ).when(p -> {
                                List<NamedExpression> projects = p.getProjects();
                                if (!checkId.apply(projects.get(0)) || !checkId.apply(projects.get(1))
                                        || !checkId.apply(projects.get(2)) || !checkId.apply(projects.get(3))
                                        || !checkId.apply(projects.get(4))) {
                                    return false;
                                }
                                return projects.get(5) instanceof Slot && projects.get(5).getName().equals("name");
                            }));

                    PlanChecker.from(connectContext)
                            .parse("select id, t.id, *"
                                    + "from custom_catalog.test_db.test_tbl4 t")
                            .analyze()
                            .rewrite()
                            .matches(logicalProject(
                                    logicalFilter(
                                            logicalTestScan()
                                    ).when(f -> {
                                        EqualTo predicate = (EqualTo) f.getPredicate();
                                        return predicate.left() instanceof Slot
                                                && predicate.right().equals(new IntegerLiteral((byte) 1));
                                    })
                            ).when(p -> {
                                List<NamedExpression> projects = p.getProjects();
                                if (!checkId.apply(projects.get(0)) || !checkId.apply(projects.get(1))
                                        || !checkId.apply(projects.get(2))) {
                                    return false;
                                }
                                return projects.get(3) instanceof Slot && projects.get(3).getName().equals("name");
                            }));
                }
        });
    }

    private void query(String sql) {
        PlanChecker.from(connectContext)
                .parse(sql)
                .analyze()
                .rewrite();
    }

    private void withPrivileges(List<MakeTablePrivileges> privileges, Runnable task) {
        List<TablePrivilege> tablePrivileges = Lists.newArrayList();
        List<ColumnPrivilege> columnPrivileges = Lists.newArrayList();
        List<CustomRowPolicy> rowPolicies = Lists.newArrayList();
        List<CustomDataMaskingPolicy> dataMaskingPolicies = Lists.newArrayList();

        for (MakeTablePrivileges privilege : privileges) {
            tablePrivileges.addAll(privilege.tablePrivileges);
            columnPrivileges.addAll(privilege.columnPrivileges);
            rowPolicies.addAll(privilege.rowPolicies);
            dataMaskingPolicies.addAll(privilege.dataMaskingPolicies);
        }

        SimpleCatalogAccessController.tablePrivileges.set(tablePrivileges);
        SimpleCatalogAccessController.columnPrivileges.set(columnPrivileges);
        SimpleCatalogAccessController.rowPolicies.set(rowPolicies);
        SimpleCatalogAccessController.dataMaskings.set(dataMaskingPolicies);

        try {
            task.run();
        } finally {
            SimpleCatalogAccessController.rowPolicies.remove();
            SimpleCatalogAccessController.dataMaskings.remove();
            SimpleCatalogAccessController.tablePrivileges.remove();
            SimpleCatalogAccessController.columnPrivileges.remove();
        }
    }

    @Override
    public RulePromise defaultPromise() {
        return RulePromise.REWRITE;
    }

    public static class CustomCatalogProvider implements TestCatalogProvider {

        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return CATALOG_META;
        }
    }

    public static class CustomAccessControllerFactory implements AccessControllerFactory {
        @Override
        public CatalogAccessController createAccessController(Map<String, String> prop) {
            return new SimpleCatalogAccessController();
        }
    }

    public static class SimpleCatalogAccessController implements CatalogAccessController {
        private static ThreadLocal<List<TablePrivilege>> tablePrivileges = new ThreadLocal<>();
        private static ThreadLocal<List<ColumnPrivilege>> columnPrivileges = new ThreadLocal<>();
        private static ThreadLocal<List<CustomRowPolicy>> rowPolicies = new ThreadLocal<>();
        private static ThreadLocal<List<CustomDataMaskingPolicy>> dataMaskings = new ThreadLocal<>();

        @Override
        public boolean checkGlobalPriv(UserIdentity currentUser, PrivPredicate wanted) {
            return true;
        }

        @Override
        public boolean checkCtlPriv(UserIdentity currentUser, String ctl, PrivPredicate wanted) {
            return true;
        }

        @Override
        public boolean checkDbPriv(UserIdentity currentUser, String ctl, String db, PrivPredicate wanted) {
            return true;
        }

        @Override
        public boolean checkTblPriv(UserIdentity currentUser, String ctl, String db, String tbl, PrivPredicate wanted) {
            List<TablePrivilege> tablePrivileges = SimpleCatalogAccessController.tablePrivileges.get();
            if (!CollectionUtils.isEmpty(tablePrivileges)
                    && tablePrivileges.stream().anyMatch(p -> p.checkTblPriv(currentUser, ctl, db, tbl))) {
                return true;
            }
            List<ColumnPrivilege> columnPrivileges = SimpleCatalogAccessController.columnPrivileges.get();
            if (!CollectionUtils.isEmpty(columnPrivileges)
                    && columnPrivileges.stream().anyMatch(p -> p.checkTblPriv(currentUser, ctl, db, tbl))) {
                return true;
            }
            return false;
        }

        @Override
        public boolean checkResourcePriv(UserIdentity currentUser, String resourceName, PrivPredicate wanted) {
            return true;
        }

        @Override
        public boolean checkWorkloadGroupPriv(UserIdentity currentUser, String workloadGroupName,
                PrivPredicate wanted) {
            return true;
        }

        @Override
        public void checkColsPriv(UserIdentity currentUser, String ctl, String db, String tbl, Set<String> cols,
                PrivPredicate wanted) throws AuthorizationException {
            List<TablePrivilege> tablePrivileges = SimpleCatalogAccessController.tablePrivileges.get();
            if (!CollectionUtils.isEmpty(tablePrivileges)
                    && tablePrivileges.stream().anyMatch(p -> p.checkTblPriv(currentUser, ctl, db, tbl))) {
                return;
            }

            List<ColumnPrivilege> columnPrivileges = SimpleCatalogAccessController.columnPrivileges.get();
            if (CollectionUtils.isEmpty(columnPrivileges)) {
                String format = "Permission denied: user [%s] does not have privilege "
                        + "for [%s] command on [%s].[%s].[%s].[%s]";
                throw new AuthorizationException(String.format(
                        format,
                        currentUser, wanted, ctl, db, tbl, cols.iterator().next()));
            }

            for (String col : cols) {
                boolean hasPrivilege = columnPrivileges.stream()
                        .anyMatch(t -> t.checkColsPriv(currentUser, ctl, db, tbl, col));
                if (!hasPrivilege) {
                    String format = "Permission denied: user [%s] does not have privilege "
                            + "for [%s] command on [%s].[%s].[%s].[%s]";
                    throw new AuthorizationException(String.format(
                            format,
                            currentUser, wanted, ctl, db, tbl, col));
                }
            }
        }

        @Override
        public boolean checkCloudPriv(UserIdentity currentUser, String resourceName, PrivPredicate wanted,
                ResourceTypeEnum type) {
            return true;
        }

        @Override
        public Optional<DataMaskPolicy> evalDataMaskPolicy(UserIdentity currentUser, String ctl, String db, String tbl,
                String col) {
            List<CustomDataMaskingPolicy> dataMaskingPolicies = dataMaskings.get();
            if (dataMaskingPolicies == null) {
                return Optional.empty();
            }

            for (CustomDataMaskingPolicy dataMaskingPolicy : dataMaskingPolicies) {
                if (dataMaskingPolicy.column.equalsIgnoreCase(col)) {
                    return Optional.of(dataMaskingPolicy);
                }
            }
            return Optional.empty();
        }

        @Override
        public List<? extends RowFilterPolicy> evalRowFilterPolicies(UserIdentity currentUser, String ctl, String db,
                String tbl) {
            List<CustomRowPolicy> customRowPolicies = rowPolicies.get();
            if (customRowPolicies == null) {
                return ImmutableList.of();
            }
            NereidsParser nereidsParser = new NereidsParser();
            return customRowPolicies.stream()
                    .map(p -> new RowFilterPolicy() {
                        @Override
                        public Expression getFilterExpression() {
                            return nereidsParser.parseExpression(p.filter);
                        }

                        @Override
                        public String getPolicyIdent() {
                            return "custom policy: " + p.filter;
                        }
                    })
                    .collect(Collectors.toList());
        }
    }

    private static class MakePrivileges {
        public static MakeTablePrivileges table(String catalog, String db, String table) {
            return new MakeTablePrivileges(catalog, db, table);
        }
    }

    private static class MakeTablePrivileges {
        private String catalog;
        private String db;
        private String table;

        private List<TablePrivilege> tablePrivileges;
        private List<ColumnPrivilege> columnPrivileges;
        private List<CustomRowPolicy> rowPolicies;
        private List<CustomDataMaskingPolicy> dataMaskingPolicies;

        public MakeTablePrivileges(String catalog, String db, String table) {
            this.catalog = catalog;
            this.db = db;
            this.table = table;
            this.tablePrivileges = Lists.newArrayList();
            this.columnPrivileges = Lists.newArrayList();
            this.rowPolicies = Lists.newArrayList();
            this.dataMaskingPolicies = Lists.newArrayList();
        }

        public MakeTablePrivileges allowSelectTable(String user) {
            tablePrivileges.add(new TablePrivilege(catalog, db, table, user));
            return this;
        }

        public MakeTablePrivileges allowSelectColumns(String user, Set<String> allowColumns) {
            columnPrivileges.add(new ColumnPrivilege(catalog, db, table, user, allowColumns));
            return this;
        }

        public MakeTablePrivileges addRowPolicy(String user, String filter) {
            rowPolicies.add(new CustomRowPolicy(user, filter));
            return this;
        }

        public MakeTablePrivileges addDataMasking(String user, String column, String project) {
            dataMaskingPolicies.add(new CustomDataMaskingPolicy(user, column, project));
            return this;
        }
    }

    private static class TablePrivilege {
        private final String catalog;
        private final String db;
        private final String table;
        private final String user;

        public TablePrivilege(String catalog, String db, String table, String user) {
            this.catalog = catalog;
            this.db = db;
            this.table = table;
            this.user = user;
        }

        public boolean checkTblPriv(UserIdentity currentUser, String ctl, String db, String tbl) {
            return isSameTable(ctl, db, tbl) && StringUtils.equals(this.user, currentUser.getUser());
        }

        public boolean isSameTable(String catalog, String db, String tbl) {
            return StringUtils.equals(this.catalog, catalog)
                    && StringUtils.equals(this.db, db)
                    && StringUtils.equals(this.table, tbl);
        }
    }

    private static class ColumnPrivilege {
        private final String catalog;
        private final String db;
        private final String table;
        private final String user;
        private final Set<String> allowColumns;

        public ColumnPrivilege(String catalog, String db, String table, String user, Set<String> allowColumns) {
            this.catalog = catalog;
            this.db = db;
            this.table = table;
            this.user = user;
            this.allowColumns = allowColumns;
        }

        public boolean checkTblPriv(UserIdentity currentUser, String ctl, String db, String tbl) {
            return isSameTable(ctl, db, tbl) && StringUtils.equals(this.user, currentUser.getUser());
        }

        public boolean checkColsPriv(UserIdentity currentUser, String ctl, String db, String tbl, String col) {
            return isSameTable(ctl, db, tbl)
                    && StringUtils.equals(this.user, currentUser.getUser()) && allowColumns.contains(col);
        }

        public boolean isSameTable(String catalog, String db, String tbl) {
            return StringUtils.equals(this.catalog, catalog)
                    && StringUtils.equals(this.db, db)
                    && StringUtils.equals(this.table, tbl);
        }
    }

    private static class CustomRowPolicy implements RowFilterPolicy {
        private final String user;
        private final String filter;

        public CustomRowPolicy(String user, String filter) {
            this.user = user;
            this.filter = filter;
        }

        public String getUser() {
            return user;
        }

        @Override
        public Expression getFilterExpression() {
            return new NereidsParser().parseExpression(filter);
        }

        @Override
        public String getPolicyIdent() {
            return "custom policy: " + filter;
        }

        @Override
        public FilterType getFilterType() {
            return FilterType.PERMISSIVE;
        }
    }

    private static class CustomDataMaskingPolicy implements DataMaskPolicy {
        private final String user;
        private final String column;
        private final String project;

        public CustomDataMaskingPolicy(String user, String name, String project) {
            this.user = user;
            this.column = name;
            this.project = project;
        }

        public String getUser() {
            return user;
        }

        @Override
        public String getMaskTypeDef() {
            return project;
        }

        @Override
        public String getPolicyIdent() {
            return "custom policy: " + project;
        }
    }
}
