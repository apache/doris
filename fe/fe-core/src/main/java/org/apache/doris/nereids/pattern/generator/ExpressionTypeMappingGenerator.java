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
import com.google.common.collect.Sets;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.processing.ProcessingEnvironment;
import javax.tools.StandardLocation;

/** ExpressionTypeMappingGenerator */
public class ExpressionTypeMappingGenerator {
    private static final Set<String> FORBIDDEN_CLASS = Sets.newHashSet(
            "org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait",
            "org.apache.doris.nereids.trees.expressions.shape.LeafExpression",
            "org.apache.doris.nereids.trees.expressions.shape.UnaryExpression",
            "org.apache.doris.nereids.trees.expressions.shape.BinaryExpression",
            "org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable",
            "org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable",
            "org.apache.doris.nereids.trees.expressions.functions.PropagateNullLiteral",
            "org.apache.doris.nereids.trees.expressions.typecoercion.ImplicitCastInputTypes",
            "org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature",
            "org.apache.doris.nereids.trees.expressions.functions.Function",
            "org.apache.doris.nereids.trees.expressions.functions.FunctionTrait",
            "org.apache.doris.nereids.trees.expressions.functions.ComputeSignature",
            "org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction",
            "org.apache.doris.nereids.trees.expressions.typecoercion.ExpectsInputTypes",
            "org.apache.doris.nereids.trees.expressions.functions.ComputeNullable",
            "org.apache.doris.nereids.trees.expressions.functions.PropagateNullable"
    );

    private final JavaAstAnalyzer analyzer;

    public ExpressionTypeMappingGenerator(JavaAstAnalyzer javaAstAnalyzer) {
        this.analyzer = javaAstAnalyzer;
    }

    public JavaAstAnalyzer getAnalyzer() {
        return analyzer;
    }

    /** generate */
    public void generate(ProcessingEnvironment processingEnv) throws IOException {
        Set<String> superExpressions = findSuperExpression();
        Map<String, Set<String>> childrenNameMap = analyzer.getChildrenNameMap();
        Map<String, Set<String>> parentNameMap = analyzer.getParentNameMap();
        String code = generateCode(childrenNameMap, parentNameMap, superExpressions);
        generateFile(processingEnv, code);
    }

    private void generateFile(ProcessingEnvironment processingEnv, String code) throws IOException {
        File generatePatternFile = new File(processingEnv.getFiler()
                .getResource(StandardLocation.SOURCE_OUTPUT, "org.apache.doris.nereids.pattern",
                        "GeneratedExpressionRelations.java").toUri());
        if (generatePatternFile.exists()) {
            generatePatternFile.delete();
        }
        if (!generatePatternFile.getParentFile().exists()) {
            generatePatternFile.getParentFile().mkdirs();
        }

        // bypass create file for processingEnv.getFiler(), compile GeneratePatterns in next compile term
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(generatePatternFile))) {
            bufferedWriter.write(code);
        }
    }

    private Set<String> findSuperExpression() {
        Map<String, Set<String>> parentNameMap = analyzer.getParentNameMap();
        Map<String, Set<String>> childrenNameMap = analyzer.getChildrenNameMap();
        Set<String> superExpressions = Sets.newLinkedHashSet();
        for (Entry<String, Set<String>> entry : childrenNameMap.entrySet()) {
            String parentName = entry.getKey();
            Set<String> childrenNames = entry.getValue();

            if (parentName.startsWith("org.apache.doris.nereids.trees.expressions.")) {
                for (String childrenName : childrenNames) {
                    Set<String> parentNames = parentNameMap.get(childrenName);
                    if (parentNames != null
                            && parentNames.contains("org.apache.doris.nereids.trees.expressions.Expression")) {
                        superExpressions.add(parentName);
                        break;
                    }
                }
            }
        }
        return superExpressions;
    }

    private String generateCode(Map<String, Set<String>> childrenNameMap,
            Map<String, Set<String>> parentNameMap, Set<String> superExpressions) {
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
                + "import org.apache.doris.nereids.trees.expressions.Expression;\n"
                + "\n"
                + "import com.google.common.collect.ImmutableMap;\n"
                + "import com.google.common.collect.ImmutableSet;\n"
                + "\n"
                + "import java.util.Map;\n"
                + "import java.util.Set;\n"
                + "\n";
        generateCode += "/** GeneratedExpressionRelations */\npublic class GeneratedExpressionRelations {\n";
        String childrenClassesGenericType = "<Class<?>, Set<Class<? extends Expression>>>";
        generateCode +=
                "    public static final Map" + childrenClassesGenericType + " CHILDREN_CLASS_MAP;\n\n";
        generateCode +=
                "    static {\n"
              + "        ImmutableMap.Builder" + childrenClassesGenericType + " childrenClassesBuilder\n"
              + "                = ImmutableMap.builderWithExpectedSize(" + childrenNameMap.size() + ");\n";

        for (String superExpression : superExpressions) {
            if (FORBIDDEN_CLASS.contains(superExpression)) {
                continue;
            }

            Set<String> childrenClasseSet = childrenNameMap.get(superExpression)
                    .stream()
                    .filter(childClass -> parentNameMap.get(childClass)
                            .contains("org.apache.doris.nereids.trees.expressions.Expression")
                    )
                    .collect(Collectors.toSet());

            List<String> childrenClasses = Lists.newArrayList(childrenClasseSet);
            Collections.sort(childrenClasses, Comparator.naturalOrder());

            String childClassesString = childrenClasses.stream()
                    .map(childClass -> "                    " + childClass + ".class")
                    .collect(Collectors.joining(",\n"));
            generateCode += "        childrenClassesBuilder.put(\n                " + superExpression
                    + ".class,\n                ImmutableSet.<Class<? extends Expression>>of(\n" + childClassesString
                    + "\n                )\n        );\n\n";
        }

        generateCode += "        CHILDREN_CLASS_MAP = childrenClassesBuilder.build();\n";

        return generateCode + "    }\n}\n";
    }
}
