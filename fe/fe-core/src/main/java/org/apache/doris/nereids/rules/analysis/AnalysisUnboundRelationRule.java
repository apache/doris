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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.pattern.Pattern;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;

import com.alibaba.google.common.collect.ImmutableList;
import com.alibaba.google.common.collect.Lists;

import java.util.List;

/**
 * Rule to bind relations in query plan.
 */
public class AnalysisUnboundRelationRule extends AnalysisRule {
    public AnalysisUnboundRelationRule() {
        super(RuleType.BINDING_UNBOUND_RELATION_RULE, new Pattern(NodeType.LOGICAL_UNBOUND_RELATION));
    }

    @Override
    public List<Plan<?>> transform(Plan<?> plan, PlannerContext context) throws AnalysisException {
        UnboundRelation unboundRelation = (UnboundRelation) plan;
        List<String> nameParts = unboundRelation.getNameParts();
        switch (nameParts.size()) {
            case 1: {
                List<String> qualifier = Lists.newArrayList(
                        context.getConnectContext().getDatabase(), nameParts.get(0));
                Table table = getTable(qualifier, context.getConnectContext().getCatalog());
                return ImmutableList.of(new LogicalRelation(table, qualifier));
            }
            case 2: {
                Table table = getTable(nameParts, context.getConnectContext().getCatalog());
                return ImmutableList.of(new LogicalRelation(table, nameParts));
            }
            default:
                throw new AnalysisException("Table name [" + unboundRelation.getTableName() + "] is invalid.");
        }
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
