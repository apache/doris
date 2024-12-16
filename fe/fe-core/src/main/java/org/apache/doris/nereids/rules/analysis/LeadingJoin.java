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

import org.apache.doris.nereids.hint.Hint;
import org.apache.doris.nereids.hint.LeadingHint;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmap;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 *  Leading join is used to generate leading join and replace original logical join
*/
public class LeadingJoin implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            logicalJoin()
                    .thenApply(ctx -> {
                        if (!ctx.cascadesContext.isLeadingJoin() || ctx.cascadesContext.isLeadingDisableJoinReorder()) {
                            return ctx.root;
                        }
                        Hint leadingHint = ctx.cascadesContext.getHintMap().get("Leading");
                        ((LeadingHint) leadingHint).setTotalBitmap(ctx.root.getInputRelations());
                        Long currentBitMap = LongBitmap.computeTableBitmap(ctx.root.getInputRelations());
                        if (((LeadingHint) leadingHint).getTotalBitmap().equals(currentBitMap)
                                && leadingHint.isSuccess()) {
                            Plan leadingJoin = ((LeadingHint) leadingHint).generateLeadingJoinPlan();
                            if (leadingHint.isSuccess() && leadingJoin != null) {
                                ctx.cascadesContext.setLeadingDisableJoinReorder(true);
                                ctx.cascadesContext.setLeadingJoin(false);
                                return leadingJoin;
                            }
                        }
                        return ctx.root;
                    }).toRule(RuleType.LEADING_JOIN)
        );
    }
}
