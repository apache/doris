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
import org.apache.doris.nereids.SqlCacheContext.FullColumnName;
import org.apache.doris.nereids.SqlCacheContext.FullTableName;
import org.apache.doris.nereids.rules.analysis.UserAuthentication;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.commons.collections4.CollectionUtils;

import java.util.LinkedHashMap;
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
    private final UserIdentity userIdentity;
    private final Map<FullTableName, Set<String>> checkedPrivileges = Maps.newLinkedHashMap();
    private final Map<FullTableName, List<RowFilterSpec>> rowPolicies = Maps.newLinkedHashMap();
    private final Map<FullColumnName, Optional<DataMaskSpec>> dataMaskPolicies = Maps.newLinkedHashMap();
    private boolean complete;

    /** SecurityDependencyContext */
    public SecurityDependencyContext(UserIdentity userIdentity) {
        this.userIdentity = userIdentity;
        this.complete = userIdentity != null;
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
        dataMaskPolicies.put(new FullColumnName(
                catalog, database, table, column.toLowerCase(Locale.ROOT)), mask);
    }

    /** Freeze the decisions used by a completed plan before storing them in a reusable context. */
    public synchronized SecurityDependencyContext snapshot() {
        SecurityDependencyContext snapshot = new SecurityDependencyContext(userIdentity);
        snapshot.complete = complete;
        for (Map.Entry<FullTableName, Set<String>> entry : checkedPrivileges.entrySet()) {
            snapshot.checkedPrivileges.put(entry.getKey(), ImmutableSet.copyOf(entry.getValue()));
        }
        for (Map.Entry<FullTableName, List<RowFilterSpec>> entry : rowPolicies.entrySet()) {
            snapshot.rowPolicies.put(entry.getKey(), ImmutableList.copyOf(entry.getValue()));
        }
        snapshot.dataMaskPolicies.putAll(dataMaskPolicies);
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
    public synchronized boolean isValid(ConnectContext connectContext) {
        if (!complete || connectContext == null) {
            return false;
        }
        try {
            if (!Objects.equals(userIdentity, connectContext.getCurrentUserIdentity())) {
                return false;
            }
            Env env = connectContext.getEnv();
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
                        userIdentity, table.catalog, table.db, table.table);
                if (!CollectionUtils.isEqualCollection(entry.getValue(), current)) {
                    return false;
                }
            }
            return dataMasksAreValid(env);
        } catch (UserException | RuntimeException e) {
            return false;
        }
    }

    private boolean dataMasksAreValid(Env env) {
        Map<FullTableName, Set<String>> columnsByTable = new LinkedHashMap<>();
        for (FullColumnName column : dataMaskPolicies.keySet()) {
            columnsByTable.computeIfAbsent(new FullTableName(column.catalog, column.db, column.table),
                    table -> new LinkedHashSet<>()).add(column.column);
        }
        for (Map.Entry<FullTableName, Set<String>> entry : columnsByTable.entrySet()) {
            FullTableName table = entry.getKey();
            Map<String, DataMaskSpec> current = env.getAccessManager().evalDataMaskPolicies(
                    userIdentity, table.catalog, table.db, table.table, entry.getValue());
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
