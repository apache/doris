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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.InternalAuthorizationPlugin;
import org.apache.doris.nereids.SqlCacheContext.FullColumnName;
import org.apache.doris.nereids.SqlCacheContext.FullTableName;
import org.apache.doris.nereids.rules.analysis.UserAuthentication;
import org.apache.doris.policy.PolicyMgr;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.commons.collections4.CollectionUtils;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Security decisions which an analyzed plan depends on.
 *
 * <p>Unlike {@link SqlCacheContext}, this context exists independently of the SQL result-cache switch. A prepared
 * short-circuit plan can otherwise outlive the privilege and data-policy decisions made while it was analyzed.
 * Callers record both positive and negative policy answers so that adding a policy invalidates a plan which was
 * built before that policy existed.
 */
public class SecurityDependencyContext {
    private static final long UNKNOWN_VERSION = -1;

    private final Env planningEnv;
    private final long authorizationVersion;
    private final long rowPolicyVersion;
    private final boolean versionValidationEligible;
    private final Map<FullTableName, Set<String>> checkedPrivileges = Maps.newLinkedHashMap();
    private final Map<FullTableName, List<RowFilterSpec>> rowPolicies = Maps.newLinkedHashMap();
    private final Map<FullColumnName, Optional<DataMaskSpec>> dataMaskPolicies = Maps.newLinkedHashMap();
    private final Map<FullTableName, Set<String>> dataMaskColumnsByTable = Maps.newLinkedHashMap();
    private boolean useVersionValidation;
    private boolean complete = true;

    /** Create a context which always uses full security revalidation. */
    public SecurityDependencyContext() {
        this(null, UNKNOWN_VERSION, UNKNOWN_VERSION, false);
    }

    /** Create a context and capture the security versions before analysis starts. */
    public SecurityDependencyContext(ConnectContext connectContext) {
        this(connectContext == null ? null : connectContext.getEnv(), usesAuthorizationChecks(connectContext));
    }

    private SecurityDependencyContext(Env env, boolean versionValidationEligible) {
        this(env, currentAuthorizationVersion(env), currentRowPolicyVersion(env), versionValidationEligible);
    }

    private SecurityDependencyContext(Env planningEnv, long authorizationVersion, long rowPolicyVersion,
            boolean versionValidationEligible) {
        this.planningEnv = planningEnv;
        this.authorizationVersion = authorizationVersion;
        this.rowPolicyVersion = rowPolicyVersion;
        this.versionValidationEligible = versionValidationEligible;
    }

    /** Record the columns whose SELECT privilege was checked while the plan was analyzed. */
    public synchronized void addCheckedPrivilege(TableIf table, Set<String> usedColumns) {
        Optional<FullTableName> tableName = qualifiedName(table);
        if (!tableName.isPresent()) {
            complete = false;
            return;
        }
        Set<String> existing = checkedPrivileges.get(tableName.get());
        if (existing == null) {
            checkedPrivileges.put(tableName.get(), ImmutableSet.copyOf(usedColumns));
        } else {
            checkedPrivileges.put(tableName.get(), ImmutableSet.<String>builder()
                    .addAll(existing).addAll(usedColumns).build());
        }
    }

    /** Record the complete row-filter answer, including an empty answer. */
    public synchronized void setRowPolicies(
            String catalog, String database, String table, List<RowFilterSpec> policies) {
        rowPolicies.put(new FullTableName(catalog, database, table), ImmutableList.copyOf(policies));
    }

    /** Record the mask answer for a column, including the absence of a mask. */
    public synchronized void addDataMask(
            String catalog, String database, String table, String column, Optional<DataMaskSpec> mask) {
        String normalizedColumn = column.toLowerCase(Locale.ROOT);
        FullTableName tableName = new FullTableName(catalog, database, table);
        dataMaskPolicies.put(new FullColumnName(catalog, database, table, normalizedColumn), mask);
        dataMaskColumnsByTable.computeIfAbsent(tableName, ignored -> new LinkedHashSet<>()).add(normalizedColumn);
    }

    /** Freeze the decisions used by a completed plan before storing them in a reusable context. */
    public synchronized SecurityDependencyContext snapshot() {
        SecurityDependencyContext snapshot = new SecurityDependencyContext(
                planningEnv, authorizationVersion, rowPolicyVersion, versionValidationEligible);
        snapshot.complete = complete;
        for (Map.Entry<FullTableName, Set<String>> entry : checkedPrivileges.entrySet()) {
            snapshot.checkedPrivileges.put(entry.getKey(), ImmutableSet.copyOf(entry.getValue()));
        }
        for (Map.Entry<FullTableName, List<RowFilterSpec>> entry : rowPolicies.entrySet()) {
            snapshot.rowPolicies.put(entry.getKey(), ImmutableList.copyOf(entry.getValue()));
        }
        snapshot.dataMaskPolicies.putAll(dataMaskPolicies);
        for (Map.Entry<FullTableName, Set<String>> entry : dataMaskColumnsByTable.entrySet()) {
            snapshot.dataMaskColumnsByTable.put(entry.getKey(), ImmutableSet.copyOf(entry.getValue()));
        }
        snapshot.useVersionValidation = snapshot.canUseVersionValidation();
        return snapshot;
    }

    /** Freeze the decisions for a prepared short-circuit plan, failing closed if authorization was not recorded. */
    public synchronized SecurityDependencyContext snapshotForShortCircuit() {
        SecurityDependencyContext snapshot = snapshot();
        if (checkedPrivileges.isEmpty()) {
            snapshot.complete = false;
        }
        return snapshot;
    }

    /**
     * Revalidate every security decision before a cached plan bypasses analysis.
     *
     * <p>A false result does not deny the statement itself. It rejects only the cached plan, after which the normal
     * planning path performs the authoritative checks and returns the usual user-facing error when access was
     * revoked. Authorization-source failures also reject reuse, so this fast path always fails closed.
     */
    public boolean isValid(ConnectContext connectContext) {
        if (!complete || connectContext == null) {
            return false;
        }
        try {
            Env env = connectContext.getEnv();
            if (useVersionValidation) {
                return usesAuthorizationChecks(connectContext) && versionsAreCurrent(env);
            }
            UserIdentity currentUser = connectContext.getCurrentUserIdentity();
            if (currentUser == null) {
                return false;
            }
            for (Map.Entry<FullTableName, Set<String>> entry : checkedPrivileges.entrySet()) {
                TableIf table = findTable(env, entry.getKey());
                if (table == null) {
                    return false;
                }
                UserAuthentication.checkPermission(table, connectContext, entry.getValue());
            }
            for (Map.Entry<FullTableName, List<RowFilterSpec>> entry : rowPolicies.entrySet()) {
                FullTableName table = entry.getKey();
                List<RowFilterSpec> current = env.getAccessManager().evalRowFilterPolicies(
                        currentUser, table.catalog, table.db, table.table);
                if (!CollectionUtils.isEqualCollection(entry.getValue(), current)) {
                    return false;
                }
            }
            return dataMasksAreValid(env, currentUser);
        } catch (UserException | RuntimeException e) {
            return false;
        }
    }

    private boolean canUseVersionValidation() {
        if (!complete || !versionValidationEligible || checkedPrivileges.isEmpty()
                || authorizationVersion == UNKNOWN_VERSION || rowPolicyVersion == UNKNOWN_VERSION) {
            return false;
        }
        return allDependenciesUseInternalCatalog() && usesVersionedBuiltInAuthorization(planningEnv);
    }

    private boolean versionsAreCurrent(Env env) {
        if (env == null || env != planningEnv) {
            return false;
        }
        Auth auth = env.getAuth();
        PolicyMgr policyMgr = env.getPolicyMgr();
        return auth != null && policyMgr != null
                && auth.isAuthorizationVersionReliable()
                && env.getAccessManager().getAccessControllerOrDefault(InternalCatalog.INTERNAL_CATALOG_NAME)
                        instanceof InternalAuthorizationPlugin
                && auth.getAuthorizationVersion() == authorizationVersion
                && policyMgr.getRowPolicyVersion() == rowPolicyVersion;
    }

    private static boolean usesVersionedBuiltInAuthorization(Env env) {
        if (env == null || env.getAuth() == null || env.getPolicyMgr() == null
                || !env.getAuth().isAuthorizationVersionReliable()) {
            return false;
        }
        return env.getAccessManager().getAccessControllerOrDefault(InternalCatalog.INTERNAL_CATALOG_NAME)
                instanceof InternalAuthorizationPlugin;
    }

    private boolean allDependenciesUseInternalCatalog() {
        for (FullTableName table : checkedPrivileges.keySet()) {
            if (!InternalCatalog.INTERNAL_CATALOG_NAME.equals(table.catalog)) {
                return false;
            }
        }
        for (FullTableName table : rowPolicies.keySet()) {
            if (!InternalCatalog.INTERNAL_CATALOG_NAME.equals(table.catalog)) {
                return false;
            }
        }
        for (FullTableName table : dataMaskColumnsByTable.keySet()) {
            if (!InternalCatalog.INTERNAL_CATALOG_NAME.equals(table.catalog)) {
                return false;
            }
        }
        return true;
    }

    private static long currentAuthorizationVersion(Env env) {
        Auth auth = env == null ? null : env.getAuth();
        return auth == null ? UNKNOWN_VERSION : auth.getAuthorizationVersion();
    }

    private static long currentRowPolicyVersion(Env env) {
        PolicyMgr policyMgr = env == null ? null : env.getPolicyMgr();
        return policyMgr == null ? UNKNOWN_VERSION : policyMgr.getRowPolicyVersion();
    }

    private static boolean usesAuthorizationChecks(ConnectContext connectContext) {
        if (connectContext == null || connectContext.isSkipAuth()) {
            return false;
        }
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        return sessionVariable != null && !sessionVariable.isPlayNereidsDump();
    }

    private boolean dataMasksAreValid(Env env, UserIdentity currentUser) {
        for (Map.Entry<FullTableName, Set<String>> entry : dataMaskColumnsByTable.entrySet()) {
            FullTableName table = entry.getKey();
            Map<String, DataMaskSpec> current = env.getAccessManager().evalDataMaskPolicies(
                    currentUser, table.catalog, table.db, table.table, entry.getValue());
            for (String column : entry.getValue()) {
                Optional<DataMaskSpec> currentMask = Optional.ofNullable(
                        current.get(column.toLowerCase(Locale.ROOT)));
                if (!Objects.equals(dataMaskPolicies.get(
                        new FullColumnName(table.catalog, table.db, table.table, column)), currentMask)) {
                    return false;
                }
            }
        }
        return true;
    }

    private Optional<FullTableName> qualifiedName(TableIf table) {
        if (table == null) {
            return Optional.empty();
        }
        DatabaseIf database = table.getDatabase();
        if (database == null || database.getCatalog() == null) {
            return Optional.empty();
        }
        return Optional.of(new FullTableName(
                database.getCatalog().getName(), database.getFullName(), table.getName()));
    }

    private TableIf findTable(Env env, FullTableName fullTableName) {
        CatalogIf<DatabaseIf<TableIf>> catalog = env.getCatalogMgr().getCatalog(fullTableName.catalog);
        if (catalog == null) {
            return null;
        }
        Optional<DatabaseIf<TableIf>> database = catalog.getDb(fullTableName.db);
        if (!database.isPresent()) {
            return null;
        }
        return database.get().getTable(fullTableName.table).orElse(null);
    }
}
