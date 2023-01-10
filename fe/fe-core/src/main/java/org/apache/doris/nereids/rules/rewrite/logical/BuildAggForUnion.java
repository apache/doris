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
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;

import java.util.Optional;

/**
 * For distinct union, add agg node.
 */
public class BuildAggForUnion extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalUnion().whenNot(LogicalUnion::hasBuildAgg).then(union -> {
            if (union.getQualifier() == Qualifier.DISTINCT) {
                return new LogicalAggregate(union.getOutputs(), union.getOutputs(),
                        true, Optional.empty(), union.withHasBuildAgg());
            }
            return union;
        }).toRule(RuleType.BUILD_AGG_FOR_UNION);
    }
}
