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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.mtmv.MTMVCache;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.visitor.TableCollector.TableCollectorContext;
import org.apache.doris.qe.ConnectContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.Set;

/**
 * Collect the table in plan
 * Note: will not get table if table is eliminated by EmptyRelation in rewrite.
 * View expand is in RBO, if call this method with the plan after RBO, this will get base tables in view, or will not.
 * Materialized view is extended or not can be controlled by the field expand
 */
public class TableCollector extends DefaultPlanVisitor<Plan, TableCollectorContext> {

    public static final TableCollector INSTANCE = new TableCollector();
    private static final Logger LOG = LogManager.getLogger(TableCollector.class);

    @Override
    public Plan visitLogicalCatalogRelation(LogicalCatalogRelation catalogRelation, TableCollectorContext context) {
        TableIf table = catalogRelation.getTable();
        if (context.getTargetTableTypes().isEmpty() || context.getTargetTableTypes().contains(table.getType())) {
            context.getCollectedTables().add(table);
        }
        if (table instanceof MTMV) {
            expandMvAndCollect((MTMV) table, context);
        }
        return catalogRelation;
    }

    @Override
    public Plan visitPhysicalCatalogRelation(PhysicalCatalogRelation catalogRelation, TableCollectorContext context) {
        TableIf table = catalogRelation.getTable();
        if (context.getTargetTableTypes().isEmpty() || context.getTargetTableTypes().contains(table.getType())) {
            context.getCollectedTables().add(table);
        }
        if (table instanceof MTMV) {
            expandMvAndCollect((MTMV) table, context);
        }
        return catalogRelation;
    }

    private void expandMvAndCollect(MTMV mtmv, TableCollectorContext context) {
        if (!context.isExpand()) {
            return;
        }
        // Make sure use only one connection context when in query to avoid ConnectionContext.get() wrong
        MTMVCache expandedMv = MTMVCache.from(mtmv, context.getConnectContext() == null
                ? MTMVPlanUtil.createMTMVContext(mtmv) : context.getConnectContext(), false);
        expandedMv.getLogicalPlan().accept(this, context);
    }

    /**
     * The context for table collecting, it contains the target collect table types
     * and the result of collect.
     */
    public static final class TableCollectorContext {
        private final Set<TableIf> collectedTables = new HashSet<>();
        private final Set<TableType> targetTableTypes;
        // if expand the mv or not
        private final boolean expand;
        private ConnectContext connectContext;

        public TableCollectorContext(Set<TableType> targetTableTypes, boolean expand) {
            this.targetTableTypes = targetTableTypes;
            this.expand = expand;
        }

        public Set<TableIf> getCollectedTables() {
            return collectedTables;
        }

        public Set<TableType> getTargetTableTypes() {
            return targetTableTypes;
        }

        public boolean isExpand() {
            return expand;
        }

        public ConnectContext getConnectContext() {
            return connectContext;
        }

        public void setConnectContext(ConnectContext connectContext) {
            this.connectContext = connectContext;
        }
    }
}
