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

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/** PlanTypeMappingGenerator */
public class PlanTypeMappingGenerator {
    private final JavaAstAnalyzer analyzer;

    public PlanTypeMappingGenerator(JavaAstAnalyzer javaAstAnalyzer) {
        this.analyzer = javaAstAnalyzer;
    }

    public JavaAstAnalyzer getAnalyzer() {
        return analyzer;
    }

    /** generate the source of GeneratedPlanRelations. */
    public String generateCode() {
        Set<String> superPlans = findSuperPlan();
        Map<String, Set<String>> childrenNameMap = analyzer.getChildrenNameMap();
        Map<String, Set<String>> parentNameMap = analyzer.getParentNameMap();
        return renderCode(childrenNameMap, parentNameMap, superPlans);
    }

    private Set<String> findSuperPlan() {
        Map<String, Set<String>> parentNameMap = analyzer.getParentNameMap();
        Map<String, Set<String>> childrenNameMap = analyzer.getChildrenNameMap();
        // sorted, so that the generated code does not depend on the file discovery order
        Set<String> superPlans = new TreeSet<>();
        for (Entry<String, Set<String>> entry : childrenNameMap.entrySet()) {
            String parentName = entry.getKey();
            Set<String> childrenNames = entry.getValue();

            if (parentName.startsWith("org.apache.doris.nereids.trees.plans.")) {
                for (String childrenName : childrenNames) {
                    Set<String> parentNames = parentNameMap.get(childrenName);
                    if (parentNames != null
                            && parentNames.contains("org.apache.doris.nereids.trees.plans.Plan")) {
                        superPlans.add(parentName);
                        break;
                    }
                }
            }
        }
        return superPlans;
    }

    private String renderCode(Map<String, Set<String>> childrenNameMap,
            Map<String, Set<String>> parentNameMap, Set<String> superPlans) {
        String generateCode
                = "// Licensed to the Apache Software Foundation (ASF) under one\n"
                + "// or more contributor license agreements.  See the NOTICE file\n"
                + "// distributed with this work for additional information\n"
                + "// regarding copyright ownership.  The ASF licenses this file\n"
                + "// to you under the Apache License, Version 2.0 (the\n"
                + "// \"License\"); you may not use this file except in compliance\n"
                + "// with the License.  You may obtain a copy of the License at\n"
                + "//\n"
                + "//   http://www.apache.org/licenses/LICENSE-2.0\n"
                + "//\n"
                + "// Unless required by applicable law or agreed to in writing,\n"
                + "// software distributed under the License is distributed on an\n"
                + "// \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\n"
                + "// KIND, either express or implied.  See the License for the\n"
                + "// specific language governing permissions and limitations\n"
                + "// under the License.\n"
                + "\n"
                + "package org.apache.doris.nereids.pattern;\n"
                + "\n"
                + "import org.apache.doris.nereids.trees.plans.Plan;\n"
                + "\n"
                + "import com.google.common.collect.ImmutableMap;\n"
                + "import com.google.common.collect.ImmutableSet;\n"
                + "\n"
                + "import java.util.Map;\n"
                + "import java.util.Set;\n"
                + "\n";
        generateCode += "/** GeneratedPlanRelations */\npublic class GeneratedPlanRelations {\n";
        String childrenClassesGenericType = "<Class<?>, Set<Class<? extends Plan>>>";
        generateCode +=
                "    public static final Map" + childrenClassesGenericType + " CHILDREN_CLASS_MAP;\n\n";
        generateCode +=
                "    static {\n"
              + "        ImmutableMap.Builder" + childrenClassesGenericType + " childrenClassesBuilder\n"
              + "                = ImmutableMap.builderWithExpectedSize(" + childrenNameMap.size() + ");\n";

        for (String superPlan : superPlans) {
            Set<String> childrenClasseSet = childrenNameMap.get(superPlan)
                    .stream()
                    .filter(childClass -> parentNameMap.get(childClass)
                            .contains("org.apache.doris.nereids.trees.plans.Plan")
                    )
                    .collect(Collectors.toSet());

            List<String> childrenClasses = Lists.newArrayList(childrenClasseSet);
            Collections.sort(childrenClasses, Comparator.naturalOrder());

            String childClassesString = childrenClasses.stream()
                    .map(childClass -> "                    " + childClass + ".class")
                    .collect(Collectors.joining(",\n"));
            generateCode += "        childrenClassesBuilder.put(\n                " + superPlan
                    + ".class,\n                ImmutableSet.<Class<? extends Plan>>of(\n" + childClassesString
                    + "\n                )\n        );\n\n";
        }

        generateCode += "        CHILDREN_CLASS_MAP = childrenClassesBuilder.build();\n";

        return generateCode + "    }\n}\n";
    }
}
