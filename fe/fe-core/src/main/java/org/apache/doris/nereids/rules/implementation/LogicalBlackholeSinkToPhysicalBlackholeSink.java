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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalBlackholeSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalBlackholeSink;

import java.util.Optional;

/**
 * implement blackhole sink.
 */
public class LogicalBlackholeSinkToPhysicalBlackholeSink extends OneImplementationRuleFactory {
    @Override
    public Rule build() {
        return logicalBlackholeSink().thenApply(ctx -> {
            LogicalBlackholeSink<? extends Plan> sink = ctx.root;
            return new PhysicalBlackholeSink<>(
                    sink.getOutputExprs(),
                    Optional.empty(),
                    sink.getLogicalProperties(),
                    sink.child());
        }).toRule(RuleType.LOGICAL_BLACKHOLE_SINK_TO_PHYSICAL_BLACKHOLE_SINK_RULE);
    }
}
