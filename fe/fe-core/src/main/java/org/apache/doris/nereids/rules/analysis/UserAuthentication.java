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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.plugin.PluginDrivenSysExternalTable;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import org.apache.commons.collections4.CollectionUtils;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Check whether a user is permitted to scan specific tables.
 */
public class UserAuthentication {
    /** checkPermission. */
    public static void checkPermission(TableIf table, ConnectContext connectContext, Set<String> columns)
            throws UserException {
        if (table == null) {
            return;
        }
        TableIf authTable = table;
        Set<String> authColumns = columns;
        if (table instanceof PluginDrivenSysExternalTable) {
            // After the SPI cutover a paimon sys-table ($snapshots/$files/...) is a
            // PluginDrivenSysExternalTable; authorize against its source table (mirrors the
            // legacy PaimonSysExternalTable branch above), so a user holding SELECT on db.tbl
            // can query db.tbl$snapshots.
            authTable = ((PluginDrivenSysExternalTable) table).getSourceTable();
            authColumns = Collections.emptySet();
        }
        String tableName = authTable.getName();
        DatabaseIf db = authTable.getDatabase();
        // when table instanceof FunctionGenTable, db will be null
        if (db == null) {
            return;
        }
        String dbName = db.getFullName();

        // Special handling: cluster snapshot related tables in information_schema
        // require privilege based on configuration
        if (isClusterSnapshotTable(dbName, tableName)) {
            if ("admin".equalsIgnoreCase(Config.cluster_snapshot_min_privilege)) {
                // When configured as admin, check ADMIN privilege
                if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(connectContext, PrivPredicate.ADMIN)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                            PrivPredicate.ADMIN.getPrivs().toString());
                }
            } else {
                // Default or configured as root, check if user is root
                UserIdentity currentUser = connectContext.getCurrentUserIdentity();
                if (currentUser == null || !currentUser.isRootUser()) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                            "root privilege");
                }
            }
            return; // privilege check passed, allow access
        }

        CatalogIf catalog = db.getCatalog();
        if (catalog == null) {
            return;
        }
        String ctlName = catalog.getName();
        AccessControllerManager accessManager = connectContext.getEnv().getAccessManager();
        if (CollectionUtils.isEmpty(authColumns)) {
            if (!accessManager.checkTblPriv(connectContext, ctlName, dbName, tableName, PrivPredicate.SELECT)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLE_ACCESS_DENIED_ERROR,
                        PrivPredicate.SELECT.getPrivs().toString(), tableName);
            }
        } else {
            accessManager.checkColumnsPriv(connectContext, ctlName, dbName, tableName, authColumns,
                    PrivPredicate.SELECT);
        }
    }

    /**
     * A visible column {@code table} has now that the user holds SELECT on by a grant on the column itself,
     * if there is one.
     *
     * <p>This is what lets a statement that reads rows of a table and none of its columns run for a user
     * who was granted some of the columns and not the table, as it does in MySQL and PostgreSQL; see
     * {@code CheckPrivileges}. It tells that user nothing new: whoever is found a column here can already
     * read every row of the table through it, and a row policy on the table filters the rows either way.
     *
     * <p>Answered from the built-in privilege model alone; for a table governed by any other authorization
     * source there is never a column, see {@code AccessControllerManager#findColumnWithPriv}. Neither is
     * there for a table whose check is not about its own grants: a sys table, authorized through its source
     * table, and the cluster snapshot tables, which take a privilege on the instance.
     */
    public static Optional<String> findSelectableColumn(TableIf table, ConnectContext connectContext) {
        if (table == null || table instanceof PluginDrivenSysExternalTable) {
            return Optional.empty();
        }
        DatabaseIf db = table.getDatabase();
        if (db == null || db.getCatalog() == null || isClusterSnapshotTable(db.getFullName(), table.getName())) {
            return Optional.empty();
        }
        Set<String> visibleColumns = new LinkedHashSet<>();
        for (Column column : table.getBaseSchemaOrEmpty()) {
            if (column.isVisible()) {
                visibleColumns.add(column.getName());
            }
        }
        return connectContext.getEnv().getAccessManager().findColumnWithPriv(connectContext,
                db.getCatalog().getName(), db.getFullName(), table.getName(), visibleColumns, PrivPredicate.SELECT);
    }

    private static boolean isClusterSnapshotTable(String dbName, String tableName) {
        return dbName.equalsIgnoreCase(InfoSchemaDb.DATABASE_NAME)
                && (tableName.equalsIgnoreCase("cluster_snapshots")
                    || tableName.equalsIgnoreCase("cluster_snapshot_properties"));
    }
}
