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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.policy.RowPolicy;

import java.util.List;

/**
 * CheckPolicy.
 */
public class CheckRowPolicy extends OneAnalysisRuleFactory {

    @Override
    public Rule build() {
        return unboundRelation()
            .whenNot(relation -> relation.isPolicyChecked())
            .thenApply(ctx -> {
                Env env = ctx.connectContext.getEnv();
                UnboundRelation unbindRelation = ctx.root;
                UnboundRelation checkedRelation = new UnboundRelation(unbindRelation.getNameParts(), true);
                // 1. check user
                UserIdentity currentUserIdentity = ctx.connectContext.getCurrentUserIdentity();
                String user = ctx.connectContext.getQualifiedUser();
                if (currentUserIdentity.isRootUser() || currentUserIdentity.isAdminUser()) {
                    return checkedRelation;
                }
                if (!env.getPolicyMgr().existPolicy(user)) {
                    return checkedRelation;
                }
                // 2. whether there is a policy on this table for this user
                List<String> nameParts = unbindRelation.getNameParts();
                if (nameParts.isEmpty() || nameParts.size() > 2) {
                    throw new IllegalStateException("Table name [" + unbindRelation.getTableName() + "] is invalid.");
                }
                String dbName = ctx.connectContext.getDatabase();
                if (nameParts.size() == 2 && !nameParts.get(0).equals(dbName)) {
                    dbName = ctx.connectContext.getClusterName() + ":" + nameParts.get(0);
                }
                String tableName = nameParts.get(nameParts.size() - 1);
                RowPolicy matchPolicy = getRowPolicy(dbName, tableName, user, env);
                if (matchPolicy == null) {
                    return checkedRelation;
                }
                // 3. add filter for relation
                Expression wherePredicate = matchPolicy.getNereidsPredicate();
                if (wherePredicate == null) {
                    throw new AnalysisException("Invaild row policy [" + matchPolicy.getPolicyName() + "]");
                }
                return new LogicalFilter<UnboundRelation>(wherePredicate, checkedRelation);
            }).toRule(RuleType.CHECK_ROW_POLICY);
    }

    private RowPolicy getRowPolicy(String dbName, String tableName, String user, Env env) {
        Database db = env.getInternalCatalog().getDb(dbName)
                .orElseThrow(() -> new RuntimeException("Database [" + dbName + "] does not exist."));
        Table table;
        db.readLock();
        try {
            table = db.getTable(tableName).orElseThrow(() -> new RuntimeException(
                "Table [" + tableName + "] does not exist in database [" + dbName + "]."));
        } finally {
            db.readUnlock();
        }
        long dbId = db.getId();
        long tableId = table.getId();
        return env.getPolicyMgr().getMatchTablePolicy(dbId, tableId, user);
    }
}
