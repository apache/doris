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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Rule to bind relations in query plan.
 */
public class AnalysisUnboundRelation extends OneAnalysisRuleFactory {
    @Override
    public Rule<Plan> build() {
        // fixme, just for example now
        return unboundRelation().thenApply(ctx -> {
            ConnectContext connectContext = ctx.plannerContext.getConnectContext();
            List<String> nameParts = ctx.root.getNameParts();
            switch (nameParts.size()) {
                case 1: {
                    List<String> qualifier = Lists.newArrayList(connectContext.getDatabase(), nameParts.get(0));
                    Table table = getTable(qualifier, connectContext.getCatalog());
                    return new LogicalRelation(table, qualifier);
                }
                case 2: {
                    Table table = getTable(nameParts, connectContext.getCatalog());
                    return new LogicalRelation(table, nameParts);
                }
                default:
                    throw new IllegalStateException("Table name [" + ctx.root.getTableName() + "] is invalid.");
            }
        }).toRule(RuleType.BINDING_UNBOUND_RELATION_RULE);
    }

    private Table getTable(List<String> qualifier, Catalog catalog) {
        String dbName = qualifier.get(0);
        Database db = catalog.getDb(dbName)
                .orElseThrow(() -> new RuntimeException("Database [" + dbName + "] does not exist."));
        db.readLock();
        try {
            String tableName = qualifier.get(1);
            return db.getTable(tableName).orElseThrow(() -> new RuntimeException(
                    "Table [" + tableName + "] does not exist in database [" + dbName + "]."));
        } finally {
            db.readUnlock();
        }
    }
}
