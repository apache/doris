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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;

/**
 * Eliminate filter which is FALSE or TRUE.
 */
public class EliminateFilter extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalFilter()
                .when(filter -> filter.getPredicates() instanceof BooleanLiteral)
                .then(filter -> {
                    if (filter.getPredicates() == BooleanLiteral.FALSE) {
                        return new LogicalEmptyRelation(filter.getOutput());
                    } else if (filter.getPredicates() == BooleanLiteral.TRUE) {
                        return filter.child();
                    } else {
                        throw new RuntimeException("predicates is BooleanLiteral but isn't FALSE or TRUE");
                    }
                })
                .toRule(RuleType.ELIMINATE_FILTER);
    }
}
