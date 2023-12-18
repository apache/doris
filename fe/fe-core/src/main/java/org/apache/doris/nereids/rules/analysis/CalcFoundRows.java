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

import org.apache.doris.catalog.Env;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;

/**
 * CalcFoundRows support.
 */
public class CalcFoundRows extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalResultSink(logicalLimit())
                .thenApply(ctx -> {
                    String user = ctx.cascadesContext.getConnectContext().getQualifiedUser();
                    if (user == null || !Env.getCurrentEnv().getAuth().isEnableFoundRows(user)) {
                        return null;
                    }
                    LogicalResultSink<LogicalLimit<Plan>> rs = ctx.root;
                    LogicalLimit<Plan> limit = rs.child();
                    LogicalPlan limitChild = (LogicalPlan) limit.child();

                    ctx.cascadesContext.getConnectContext().setFoundRowsPlan(limitChild);
                    // put cal_found_rows behind found_rows, otherwise, this limit dropped plan
                    // will match found_rows for some original select count stmt and reset
                    // saved foundRowsPlan as null unexpectedly.
                    return new LogicalResultSink<>(rs.getOutputExprs(),
                            limit.getLimit(), limit.getOffset(), limitChild);
                }).toRule(RuleType.CALC_FOUND_ROWS);
    }
}
