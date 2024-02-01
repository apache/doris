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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Set;

/**
 * merge two generate into one if the top one's input slot with no bottom's generate output.
 */
public class MergeGenerates extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalGenerate(logicalGenerate()).then(top -> {
            LogicalGenerate<Plan> bottom = top.child();
            Set<Slot> topGeneratorSlots = top.getInputSlots();
            if (bottom.getGeneratorOutput().stream().anyMatch(topGeneratorSlots::contains)) {
                // top generators use bottom's generator's output, cannot merge.
                return top;
            }
            List<Function> generators = Lists.newArrayList(bottom.getGenerators());
            generators.addAll(top.getGenerators());
            List<Slot> generatorsOutput = Lists.newArrayList(bottom.getGeneratorOutput());
            generatorsOutput.addAll(top.getGeneratorOutput());
            return new LogicalGenerate<>(generators, generatorsOutput, bottom.child());
        }).toRule(RuleType.MERGE_GENERATES);
    }
}
