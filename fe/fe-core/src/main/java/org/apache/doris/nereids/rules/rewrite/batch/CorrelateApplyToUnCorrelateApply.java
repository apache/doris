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

package org.apache.doris.nereids.rules.rewrite.batch;

import org.apache.doris.nereids.rules.RuleFactory;
import org.apache.doris.nereids.rules.rewrite.PullUpCorrelatedFilterUnderApplyAggregateProject;
import org.apache.doris.nereids.rules.rewrite.PullUpProjectUnderApply;
import org.apache.doris.nereids.rules.rewrite.UnCorrelatedApplyAggregateFilter;
import org.apache.doris.nereids.rules.rewrite.UnCorrelatedApplyFilter;
import org.apache.doris.nereids.rules.rewrite.UnCorrelatedApplyProjectFilter;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Adjust the plan in logicalApply so that there are no correlated columns in the subquery.
 * Adjust the positions of apply and sub query nodes and apply,
 * and adjust the correlated columns to apply to prepare for de nesting.
 *
 * For the project and filter on AGG, try to adjust them to apply.
 * For the project and filter under AGG, bring the filter under AGG and merge it with agg.
 */
public class CorrelateApplyToUnCorrelateApply implements BatchRewriteRuleFactory {
    public static final List<RuleFactory> RULES = ImmutableList.of(
            new PullUpProjectUnderApply(),
            new UnCorrelatedApplyFilter(),
            new UnCorrelatedApplyProjectFilter(),
            new UnCorrelatedApplyAggregateFilter(),
            new PullUpCorrelatedFilterUnderApplyAggregateProject()
    );

    @Override
    public List<RuleFactory> getRuleFactories() {
        return RULES;
    }
}
