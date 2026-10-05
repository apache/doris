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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.pattern.Pattern;
import org.apache.doris.nereids.trees.SuperClassId;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** FilteredRules */
public class FilteredRules extends Rules {
    private BitSet typePatternIds;
    private Map<Class<?>, List<Rule>> classToRules = new ConcurrentHashMap<>();
    private List<Rule> typeFilterRules;
    private List<Rule> otherRules;
    private List<Rule> allRules;

    /** FilteredRules */
    public FilteredRules(List<Rule> rules) {
        super(rules);
        this.allRules = Utils.fastToImmutableList(rules);

        this.typeFilterRules = new ArrayList<>();
        this.otherRules = new ArrayList<>();

        BitSet typePatternIds = new BitSet();
        for (Rule rule : rules) {
            Pattern<? extends Plan> pattern = rule.getPattern();
            Class<? extends Plan> topType = pattern.getMatchedType();
            if (topType != null) {
                typePatternIds.set(SuperClassId.getClassId(topType));
                typeFilterRules.add(rule);
            } else {
                otherRules.add(rule);
            }
        }
        this.typePatternIds = typePatternIds;
    }

    @Override
    public List<Rule> getCurrentAndChildrenRules(TreeNode<?> treeNode) {
        if (otherRules.isEmpty()) {
            BitSet classTypes = treeNode.getAllChildrenTypes();
            if (!typePatternIds.intersects(classTypes)) {
                return ImmutableList.of();
            }
        }
        return rules;
    }

    @Override
    public List<Rule> getCurrentRules(TreeNode<?> treeNode) {
        if (otherRules.isEmpty()) {
            BitSet classTypes = treeNode.getSuperClassTypes();
            if (!typePatternIds.intersects(classTypes)) {
                return ImmutableList.of();
            } else {
                Class<?> treeClass = treeNode.getClass();
                List<Rule> rulesCache = classToRules.get(treeClass);
                if (rulesCache != null) {
                    return rulesCache;
                }
                return classToRules.computeIfAbsent(treeClass, c -> {
                    ImmutableList.Builder<Rule> matchedTopTypeRules = ImmutableList.builder();
                    for (Rule rule : this.rules) {
                        Class<? extends Plan> type = rule.getPattern().getMatchedType();
                        if (type.isAssignableFrom(c)) {
                            matchedTopTypeRules.add(rule);
                        }
                    }
                    return matchedTopTypeRules.build();
                });
            }
        }
        return this.rules;
    }

    @Override
    public List<Rule> getAllRules() {
        return allRules;
    }

    @Override
    public List<Rule> filterValidRules(CascadesContext cascadesContext) {
        BitSet disableRules = cascadesContext.getAndCacheDisableRules();
        if (disableRules.isEmpty()) {
            return allRules;
        }
        List<Rule> validRules = new ArrayList<>(allRules);
        for (Rule rule : allRules) {
            if (!disableRules.get(rule.getRuleType().type())) {
                validRules.add(rule);
            }
        }
        return validRules;
    }

    @Override
    public String toString() {
        return "rules: " + rules.stream()
                .map(r -> r.getRuleType().name())
                .collect(Collectors.joining(", ", "[", "]"));
    }
}
