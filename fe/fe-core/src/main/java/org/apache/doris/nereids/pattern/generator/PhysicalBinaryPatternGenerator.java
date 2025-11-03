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

/** used to generate pattern for PhysicalBinary. */
public class PhysicalBinaryPatternGenerator extends PlanPatternGenerator {

    public PhysicalBinaryPatternGenerator(PlanPatternGeneratorAnalyzer analyzer,
            ClassDeclaration opType, Set<String> parentClass, boolean isMemoPattern) {
        super(analyzer, opType, parentClass, isMemoPattern);
    }

    @Override
    public String genericType() {
        return "<" + opType.name + "<" + childType() + ", " + childType() + ">>";
    }

    @Override
    public String genericTypeWithChildren() {
        return "<" + opType.name + "<C1, C2>>";
    }

    @Override
    public Set<String> getImports() {
        Set<String> imports = new TreeSet<>();
        imports.add(opType.getFullQualifiedName());
        if (isMemoPattern) {
            imports.add("org.apache.doris.nereids.trees.plans.GroupPlan");
        }
        imports.add("org.apache.doris.nereids.trees.plans.Plan");
        enumFieldPatternInfos.stream()
                .map(info -> info.enumFullName)
                .forEach(imports::add);
        return imports;
    }

    @Override
    public boolean isLogical() {
        return false;
    }

    @Override
    public int childrenNum() {
        return 2;
    }
}
