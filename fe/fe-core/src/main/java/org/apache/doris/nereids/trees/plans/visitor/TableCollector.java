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

package org.apache.doris.nereids.trees.plans.visitor;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.visitor.TableCollector.TableCollectorContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Collect the table in plan
 * Note: will not get table if table is eliminated by EmptyRelation in rewrite.
 */
public class TableCollector extends DefaultPlanVisitor<Void, TableCollectorContext> {

    public static final TableCollector INSTANCE = new TableCollector();

    @Override
    public Void visit(Plan plan, TableCollectorContext context) {
        if (plan instanceof CatalogRelation) {
            TableIf table = ((CatalogRelation) plan).getTable();
            if (context.getTargetTableTypes().isEmpty() || context.getTargetTableTypes().contains(table.getType())) {
                context.getCollectedTables().add(table);
            }
        }
        return super.visit(plan, context);
    }

    /**
     * The context for table collecting, it contains the target collect table types
     * and the result of collect.
     */
    public static final class TableCollectorContext {
        private final List<TableIf> collectedTables = new ArrayList<>();
        private final Set<TableType> targetTableTypes;

        public TableCollectorContext(Set<TableType> targetTableTypes) {
            this.targetTableTypes = targetTableTypes;
        }

        public List<TableIf> getCollectedTables() {
            return collectedTables;
        }

        public Set<TableType> getTargetTableTypes() {
            return targetTableTypes;
        }
    }
}
