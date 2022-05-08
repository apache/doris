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

package org.apache.doris.nereids.rules;

import org.apache.doris.nereids.rules.analysis.AnalysisUnboundRelation;
import org.apache.doris.nereids.rules.exploration.JoinAssociativeLeftToRight;
import org.apache.doris.nereids.rules.exploration.JoinCommutative;
import org.apache.doris.nereids.rules.implementation.LogicalJoinToHashJoin;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.List;

/**
 * Containers for set of different type rules.
 */
public class RuleSet {
    public static final List<Rule> ANALYSIS_RULES = factories()
            .add(new AnalysisUnboundRelation())
            .build();

    public static final List<Rule> EXPLORATION_RULES = factories()
            .add(new JoinCommutative())
            .add(new JoinAssociativeLeftToRight())
            .build();

    public static final List<Rule> IMPLEMENTATION_RULES = factories()
            .add(new LogicalJoinToHashJoin())
            .build();

    public List<Rule> getAnalysisRules() {
        return ANALYSIS_RULES;
    }

    public List<Rule> getExplorationRules() {
        return EXPLORATION_RULES;
    }

    public List<Rule> getImplementationRules() {
        return IMPLEMENTATION_RULES;
    }

    private static RuleFactories factories() {
        return new RuleFactories();
    }

    private static class RuleFactories {
        final Builder<Rule> rules = ImmutableList.builder();

        public RuleFactories add(RuleFactory ruleFactory) {
            rules.addAll(ruleFactory.buildRules());
            return this;
        }

        public List<Rule> build() {
            return rules.build();
        }
    }
}
