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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.paimon.PaimonSysExternalTable;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import org.apache.commons.collections4.CollectionUtils;

import java.util.Collections;
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
        // do not check priv when replaying dump file
        if (connectContext.getSessionVariable().isPlayNereidsDump()) {
            return;
        }
        TableIf authTable = table;
        Set<String> authColumns = columns;
        if (table instanceof PaimonSysExternalTable) {
            authTable = ((PaimonSysExternalTable) table).getSourceTable();
            authColumns = Collections.emptySet();
        }
        String tableName = authTable.getName();
        DatabaseIf db = authTable.getDatabase();
        // when table instanceof FunctionGenTable, db will be null
        if (db == null) {
            return;
        }
        String dbName = ClusterNamespace.getNameFromFullName(db.getFullName());

        // Special handling: cluster snapshot related tables in information_schema
        // require privilege based on configuration
        if (dbName.equalsIgnoreCase(InfoSchemaDb.DATABASE_NAME)
                && (tableName.equalsIgnoreCase("cluster_snapshots")
                    || tableName.equalsIgnoreCase("cluster_snapshot_properties"))) {
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
}
