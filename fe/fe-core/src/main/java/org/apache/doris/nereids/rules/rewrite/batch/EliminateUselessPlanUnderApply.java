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
import org.apache.doris.nereids.rules.rewrite.EliminateLimitUnderApply;
import org.apache.doris.nereids.rules.rewrite.EliminateSortUnderApply;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Eliminate useless operators in the subquery, including limit and sort.
 * Compatible with the old optimizer, the sort and limit in the subquery will not take effect, just delete it directly.
 */
public class EliminateUselessPlanUnderApply implements BatchRewriteRuleFactory {
    public static final List<RuleFactory> RULES = ImmutableList.of(
            new EliminateLimitUnderApply(),
            new EliminateSortUnderApply()
    );

    @Override
    public List<RuleFactory> getRuleFactories() {
        return RULES;
    }
}
