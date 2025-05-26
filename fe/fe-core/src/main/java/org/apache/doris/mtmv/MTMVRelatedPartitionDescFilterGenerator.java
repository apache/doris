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

package org.apache.doris.mtmv;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.Maps;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Filter out the unnecessary basic table partitions.
 */
public class MTMVRelatedPartitionDescFilterGenerator implements MTMVRelatedPartitionDescGeneratorService {
    private static final String MTMV_PLANER_DISABLE_RULES = "PRUNE_EMPTY_PARTITION,"
            + "ELIMINATE_GROUP_BY_KEY_BY_UNIFORM";

    @Override
    public void apply(MTMV mtmv, MTMVPartitionInfo mvPartitionInfo, Map<String, String> mvProperties,
                      RelatedPartitionDescResult lastResult) throws AnalysisException {
        if (mvPartitionInfo.getPartitionType() == MTMVPartitionInfo.MTMVPartitionType.SELF_MANAGE) {
            return;
        }
        if (mtmv == null) {
            return;
        }
        Set<String> usedBaseTablePartitions = analyzeUsedPartitions(mtmv);
        Map<String, PartitionItem> relatedPartitionItems = lastResult.getItems();
        Map<String, PartitionItem> newRelatedPartitionItems = Maps.newHashMap();
        for (Map.Entry<String, PartitionItem> entry : relatedPartitionItems.entrySet()) {
            if (usedBaseTablePartitions.contains(entry.getKey())) {
                newRelatedPartitionItems.put(entry.getKey(), entry.getValue());
            }
        }
        lastResult.setItems(newRelatedPartitionItems);
    }

    public static Set<String> analyzeUsedPartitions(MTMV mtmv) {
        ConnectContext ctx = MTMVPlanUtil.createMTMVContext(mtmv);
        List<StatementBase> statements;
        try {
            statements = new NereidsParser().parseSQL(mtmv.getQuerySql());
        } catch (Exception e) {
            throw new ParseException("Nereids parse failed. " + e.getMessage());
        }
        // Should not make table without data to empty relation when analyze the related table,
        // so add disable rules
        Set<String> tempDisableRules = ctx.getSessionVariable().getDisableNereidsRuleNames();
        StatementContext statementContext = null;
        try {
            statementContext = ctx.getStatementContext();
            StatementBase parsedStmt = statements.get(0);
            ctx.getSessionVariable().setDisableNereidsRules(MTMV_PLANER_DISABLE_RULES);
            statementContext.invalidCache(SessionVariable.DISABLE_NEREIDS_RULES);
            LogicalPlan logicalPlan = ((LogicalPlanAdapter) parsedStmt).getLogicalPlan();
            NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
            planner.planWithLock(logicalPlan, PhysicalProperties.ANY, ExplainCommand.ExplainLevel.REWRITTEN_PLAN);
            return analyzeUsedPartitions(planner.getRewrittenPlan(), mtmv.getMvPartitionInfo());
        } finally {
            // after operate, roll back the disable rules
            ctx.getSessionVariable().setDisableNereidsRules(String.join(",", tempDisableRules));
            if (statementContext != null) {
                statementContext.invalidCache(SessionVariable.DISABLE_NEREIDS_RULES);
                statementContext.close();
            }
        }
    }

    public static Set<String> analyzeUsedPartitions(Plan rewrittenPlan, MTMVPartitionInfo mtmvPartitionInfo) {
        Map<BaseTableInfo, Set<String>> queryUsedBaseTablePartitions = new LinkedHashMap<>();
        BaseTableInfo relatedTableInfo = mtmvPartitionInfo.getRelatedTableInfo();
        queryUsedBaseTablePartitions.put(relatedTableInfo, new HashSet<>());
        rewrittenPlan.accept(new StructInfo.QueryScanPartitionsCollector(), queryUsedBaseTablePartitions);
        return queryUsedBaseTablePartitions.get(relatedTableInfo);
    }

}
