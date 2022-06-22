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

import java.util.Set;
import java.util.TreeSet;

/** used to generate pattern for LogicalBinaryOperator. */
public class LogicalBinaryPatternGenerator extends PatternGenerator {

    public LogicalBinaryPatternGenerator(PatternGeneratorAnalyzer analyzer,
            ClassDeclaration opType, Set<String> parentClass) {
        super(analyzer, opType, parentClass);
    }

    @Override
    public String genericType() {
        return "<LogicalBinaryPlan<" + opType.name + ", GroupPlan, GroupPlan>, Plan>";
    }

    @Override
    public String genericTypeWithChildren() {
        return "<LogicalBinaryPlan<" + opType.name + ", C1, C2>, Plan>";
    }

    @Override
    public Set<String> getImports() {
        Set<String> imports = new TreeSet<>();
        imports.add(opType.getFullQualifiedName());
        imports.add("org.apache.doris.nereids.trees.plans.GroupPlan");
        imports.add("org.apache.doris.nereids.trees.plans.Plan");
        imports.add("org.apache.doris.nereids.trees.plans.logical.LogicalBinaryPlan");
        enumFieldPatternInfos.stream()
                .map(info -> info.enumFullName)
                .forEach(imports::add);
        return imports;
    }

    @Override
    public boolean isLogical() {
        return true;
    }

    @Override
    public int childrenNum() {
        return 2;
    }
}
