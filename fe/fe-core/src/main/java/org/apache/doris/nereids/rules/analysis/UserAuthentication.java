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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.qe.ConnectContext;

/**
 * Check whether a user is permitted to scan specific tables.
 */
public class UserAuthentication extends OneAnalysisRuleFactory {

    @Override
    public Rule build() {
        return logicalRelation()
                .when(CatalogRelation.class::isInstance)
                .thenApply(ctx -> checkPermission((CatalogRelation) ctx.root, ctx.connectContext))
                .toRule(RuleType.RELATION_AUTHENTICATION);
    }

    private Plan checkPermission(CatalogRelation relation, ConnectContext connectContext) {
        // do not check priv when replaying dump file
        if (connectContext.getSessionVariable().isPlayNereidsDump()) {
            return null;
        }
        TableIf table = relation.getTable();
        if (table == null) {
            return null;
        }
        String tableName = table.getName();
        DatabaseIf db = table.getDatabase();
        // when table inatanceof FunctionGenTable,db will be null
        if (db == null) {
            return null;
        }
        String dbName = db.getFullName();
        CatalogIf catalog = db.getCatalog();
        if (catalog == null) {
            return null;
        }
        String ctlName = catalog.getName();
        // TODO: 2023/7/19 checkColumnsPriv
        if (!connectContext.getEnv().getAccessManager().checkTblPriv(connectContext, ctlName, dbName,
                tableName, PrivPredicate.SELECT)) {
            String message = ErrorCode.ERR_TABLEACCESS_DENIED_ERROR.formatErrorMsg("SELECT",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    ctlName + ": " + dbName + ": " + tableName);
            throw new AnalysisException(message);
        }
        return null;
    }

}
