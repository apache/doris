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

package org.apache.doris.nereids.pattern.generator;

import org.apache.doris.nereids.pattern.generator.javaast.ClassDeclaration;

import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * used to analyze plan class extends hierarchy and then generated pattern builder methods.
 */
public class PlanPatternGeneratorAnalyzer {
    private final JavaAstAnalyzer analyzer;

    public PlanPatternGeneratorAnalyzer(JavaAstAnalyzer analyzer) {
        this.analyzer = analyzer;
    }

    public JavaAstAnalyzer getAnalyzer() {
        return analyzer;
    }

    /** generate pattern methods. */
    public String generatePatterns(String className, String parentClassName, boolean isMemoPattern) {
        Map<ClassDeclaration, Set<String>> planClassMap = analyzer.getParentClassMap().entrySet().stream()
                .filter(kv -> kv.getValue().contains("org.apache.doris.nereids.trees.plans.Plan"))
                .filter(kv -> !kv.getKey().name.equals("GroupPlan"))
                .filter(kv -> !Modifier.isAbstract(kv.getKey().modifiers.mod)
                        && kv.getKey() instanceof ClassDeclaration)
                .collect(Collectors.toMap(kv -> (ClassDeclaration) kv.getKey(), kv -> kv.getValue()));

        List<PlanPatternGenerator> generators = planClassMap.entrySet()
                .stream()
                .map(kv -> PlanPatternGenerator.create(this, kv.getKey(), kv.getValue(), isMemoPattern))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .sorted((g1, g2) -> {
                    // logical first
                    if (g1.isLogical() != g2.isLogical()) {
                        return g1.isLogical() ? -1 : 1;
                    }
                    // leaf first
                    if (g1.childrenNum() != g2.childrenNum()) {
                        return g1.childrenNum() - g2.childrenNum();
                    }
                    // string dict sort
                    return g1.opType.name.compareTo(g2.opType.name);
                })
                .collect(Collectors.toList());

        return PlanPatternGenerator.generateCode(className, parentClassName, generators, this, isMemoPattern);
    }
}
