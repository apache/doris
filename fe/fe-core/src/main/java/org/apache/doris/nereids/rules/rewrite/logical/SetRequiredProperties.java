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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.commands.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.stream.Collectors;

/**
 * adjust the required properties(which is null before here) to a meaningful value.
 */
public class SetRequiredProperties implements CustomRewriter {
    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        CascadesContext cascadesCtx = jobContext.getCascadesContext();
        StatementContext stmtCtx = jobContext.getCascadesContext().getStatementContext();
        if (stmtCtx.getParsedStatement() != null) {
            Plan parsedStmt = ((LogicalPlanAdapter) stmtCtx.getParsedStatement()).getLogicalPlan();
            if (parsedStmt instanceof ExplainCommand) {
                cascadesCtx.setJobContext(PhysicalProperties.ANY);
            } else if (parsedStmt instanceof InsertIntoTableCommand) {
                Preconditions.checkArgument(stmtCtx.getInsertIntoContext() != null, "insert into"
                        + " table context is null when running insert into table command");
                int keysNum = stmtCtx.getInsertIntoContext().getKeyNums();
                List<ExprId> outputs = plan.getOutput().subList(0, keysNum).stream()
                        .map(NamedExpression::getExprId).collect(Collectors.toList());
                cascadesCtx.setJobContext(PhysicalProperties.createHash(
                        new DistributionSpecHash(outputs, ShuffleType.NATURAL)));
            }
        }
        if (cascadesCtx.getCurrentJobContext().getRequiredProperties() == null) {
            cascadesCtx.setJobContext(PhysicalProperties.GATHER);
        }
        return plan;
    }
}
