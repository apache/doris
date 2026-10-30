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

package org.apache.doris.nereids;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authorization.DataMaskSpec;
import org.apache.doris.authorization.RowFilterSpec;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.InternalAuthorizationPlugin;
import org.apache.doris.nereids.rules.analysis.UserAuthentication;
import org.apache.doris.policy.PolicyMgr;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Security dependencies of a reusable prepared point-query plan. */
public class SecurityDependencyContext {
    private final UserIdentity planningUserIdentity;
    private final Set<String> planningAuthenticatedRoles;
    private final Env planningEnv;
    private final boolean authorizationChecksEnabled;
    private final List<CheckedPrivilege> checkedPrivileges = new ArrayList<>();
    private boolean hasEffectiveRowPolicy;
    private boolean hasDataMask;
    private boolean complete;

    /** Create an incomplete context for tests and callers without a connection. */
    public SecurityDependencyContext() {
        this(null, ImmutableSet.of(), null, false);
    }

    /** Capture the authorization subject before analysis starts. */
    public SecurityDependencyContext(ConnectContext connectContext) {
        this(connectContext == null ? null : connectContext.getCurrentUserIdentity(),
                authenticatedRoles(connectContext),
                connectContext == null ? null : connectContext.getEnv(),
                usesAuthorizationChecks(connectContext));
    }

    private SecurityDependencyContext(UserIdentity planningUserIdentity, Set<String> planningAuthenticatedRoles,
            Env planningEnv, boolean authorizationChecksEnabled) {
        this.planningUserIdentity = planningUserIdentity;
        this.planningAuthenticatedRoles = planningAuthenticatedRoles;
        this.planningEnv = planningEnv;
        this.authorizationChecksEnabled = authorizationChecksEnabled;
        this.complete = authorizationChecksEnabled;
    }

    /** Record the exact SELECT check which must be repeated before direct reuse. */
    public synchronized void addCheckedPrivilege(TableIf table, Set<String> usedColumns) {
        if (table == null) {
            complete = false;
            return;
        }
        DatabaseIf<?> database = table.getDatabase();
        CatalogIf<?> catalog = database == null ? null : database.getCatalog();
        if (catalog == null
                || !(table instanceof OlapTable)
                || !InternalCatalog.INTERNAL_CATALOG_NAME.equals(catalog.getName())) {
            complete = false;
            return;
        }
        checkedPrivileges.add(new CheckedPrivilege(table, database, catalog, catalog.getName(),
                database.getFullName(), table.getName(),
                usedColumns == null ? ImmutableSet.of() : ImmutableSet.copyOf(usedColumns)));
    }

    /** Record mask presence so a masked plan is never reused without policy analysis. */
    public synchronized void addDataMask(
            String catalog, String database, String table, String column, Optional<DataMaskSpec> mask) {
        hasDataMask |= mask.isPresent();
    }

    public synchronized boolean hasDataMask() {
        return hasDataMask;
    }

    /** Record only whether external policy analysis produced a row filter; definitions are not retained. */
    public synchronized void addRowPolicies(List<RowFilterSpec> policies) {
        hasEffectiveRowPolicy |= policies != null && !policies.isEmpty();
    }

    public synchronized boolean hasEffectiveRowPolicy() {
        return hasEffectiveRowPolicy;
    }

    /** Freeze the completed dependency set before storing it with the prepared plan. */
    public synchronized SecurityDependencyContext snapshotForShortCircuit() {
        SecurityDependencyContext snapshot = new SecurityDependencyContext(
                planningUserIdentity, planningAuthenticatedRoles, planningEnv, authorizationChecksEnabled);
        snapshot.checkedPrivileges.addAll(checkedPrivileges);
        snapshot.hasEffectiveRowPolicy = hasEffectiveRowPolicy;
        snapshot.hasDataMask = hasDataMask;
        snapshot.complete = complete && !checkedPrivileges.isEmpty()
                && !hasEffectiveRowPolicy && !hasDataMask
                && checkedPrivileges.stream().allMatch(CheckedPrivilege::matchesNamespace);
        return snapshot;
    }

    /**
     * Recheck the small set of facts needed to bypass planning. Returning false only rejects direct reuse; normal
     * planning then performs the authoritative check and reports its standard error.
     */
    public boolean isValid(ConnectContext connectContext) {
        if (!complete || !authorizationChecksEnabled || connectContext == null
                || planningEnv == null || planningEnv != connectContext.getEnv()
                || !Objects.equals(planningUserIdentity, connectContext.getCurrentUserIdentity())
                || !planningAuthenticatedRoles.equals(authenticatedRoles(connectContext))
                || !usesAuthorizationChecks(connectContext)
                || !usesBuiltInAuthorization(planningEnv)) {
            return false;
        }
        try {
            PolicyMgr policyMgr = planningEnv.getPolicyMgr();
            if (policyMgr == null) {
                return false;
            }
            for (CheckedPrivilege checkedPrivilege : checkedPrivileges) {
                if (!checkedPrivilege.matchesNamespace()) {
                    return false;
                }
                if (policyMgr.hasRowPolicy(checkedPrivilege.catalog, checkedPrivilege.database,
                        checkedPrivilege.tableName)) {
                    return false;
                }
                UserAuthentication.checkPermission(
                        checkedPrivilege.table, connectContext, checkedPrivilege.usedColumns);
                if (!checkedPrivilege.matchesNamespace()) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean usesBuiltInAuthorization(Env env) {
        return env != null && env.getAccessManager() != null
                && env.getAccessManager().getAccessControllerOrDefault(InternalCatalog.INTERNAL_CATALOG_NAME)
                        instanceof InternalAuthorizationPlugin;
    }

    private static boolean usesAuthorizationChecks(ConnectContext connectContext) {
        if (connectContext == null || connectContext.isSkipAuth()) {
            return false;
        }
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        return sessionVariable != null && !sessionVariable.isPlayNereidsDump();
    }

    private static Set<String> authenticatedRoles(ConnectContext connectContext) {
        if (connectContext == null) {
            return ImmutableSet.of();
        }
        Set<String> roles = connectContext.getAuthenticatedRoles();
        return roles == null || roles.isEmpty() ? ImmutableSet.of() : ImmutableSet.copyOf(roles);
    }

    private static class CheckedPrivilege {
        private final TableIf table;
        private final DatabaseIf<?> databaseObject;
        private final CatalogIf<?> catalogObject;
        private final String catalog;
        private final String database;
        private final String tableName;
        private final Set<String> usedColumns;

        private CheckedPrivilege(TableIf table, DatabaseIf<?> databaseObject, CatalogIf<?> catalogObject,
                String catalog, String database, String tableName, Set<String> usedColumns) {
            this.table = table;
            this.databaseObject = databaseObject;
            this.catalogObject = catalogObject;
            this.catalog = catalog;
            this.database = database;
            this.tableName = tableName;
            this.usedColumns = usedColumns;
        }

        private boolean matchesNamespace() {
            DatabaseIf<?> currentDatabase = table.getDatabase();
            CatalogIf<?> currentCatalog = currentDatabase == null ? null : currentDatabase.getCatalog();
            return currentDatabase == databaseObject
                    && currentCatalog == catalogObject
                    && Objects.equals(database, currentDatabase == null ? null : currentDatabase.getFullName())
                    && Objects.equals(catalog, currentCatalog == null ? null : currentCatalog.getName())
                    && Objects.equals(tableName, table.getName());
        }
    }
}
