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
import org.apache.doris.nereids.pattern.generator.javaast.EnumConstant;
import org.apache.doris.nereids.pattern.generator.javaast.EnumDeclaration;
import org.apache.doris.nereids.pattern.generator.javaast.FieldDeclaration;
import org.apache.doris.nereids.pattern.generator.javaast.MethodDeclaration;
import org.apache.doris.nereids.pattern.generator.javaast.VariableDeclarator;

import com.google.common.base.Joiner;
import org.apache.commons.lang3.StringUtils;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** used to generate pattern by plan. */
public abstract class PlanPatternGenerator {
    protected final JavaAstAnalyzer analyzer;
    protected final ClassDeclaration opType;
    protected final Set<String> parentClass;
    protected final List<EnumFieldPatternInfo> enumFieldPatternInfos;
    protected final List<String> generatePatterns = new ArrayList<>();
    protected final boolean isMemoPattern;

    /** constructor. */
    public PlanPatternGenerator(PlanPatternGeneratorAnalyzer analyzer, ClassDeclaration opType,
            Set<String> parentClass, boolean isMemoPattern) {
        this.analyzer = analyzer.getAnalyzer();
        this.opType = opType;
        this.parentClass = parentClass;
        this.enumFieldPatternInfos = getEnumFieldPatternInfos();
        this.isMemoPattern = isMemoPattern;
    }

    public abstract String genericType();

    public abstract String genericTypeWithChildren();

    public abstract Set<String> getImports();

    public abstract boolean isLogical();

    public abstract int childrenNum();

    public String getPatternMethodName() {
        return opType.name.substring(0, 1).toLowerCase(Locale.ENGLISH) + opType.name.substring(1);
    }

    /** generate code by generators and analyzer. */
    public static String generateCode(String className, String parentClassName, List<PlanPatternGenerator> generators,
            PlanPatternGeneratorAnalyzer analyzer, boolean isMemoPattern) {
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
                + generateImports(generators)
                + "\n";
        generateCode += "public interface " + className + " extends " + parentClassName + " {\n";
        generateCode += generators.stream()
                .map(generator -> {
                    String patternMethods = generator.generate(isMemoPattern);
                    // add indent
                    return Arrays.stream(patternMethods.split("\n"))
                            .map(line -> "  " + line + "\n")
                            .collect(Collectors.joining(""));
                }).collect(Collectors.joining("\n"));
        return generateCode + "}\n";
    }

    protected List<EnumFieldPatternInfo> getEnumFieldPatternInfos() {
        List<EnumFieldPatternInfo> enumFieldInfos = new ArrayList<>();
        for (Entry<FieldDeclaration, EnumDeclaration> pair : findEnumFieldType()) {
            FieldDeclaration fieldDecl = pair.getKey();
            EnumDeclaration enumDecl = pair.getValue();

            Set<String> enumClassNameParts = splitCase(enumDecl.name)
                    .stream()
                    .map(part -> part.toLowerCase(Locale.ENGLISH))
                    .collect(Collectors.toSet());

            for (VariableDeclarator varDecl : fieldDecl.variableDeclarators.variableDeclarators) {
                String enumFieldName = varDecl.variableDeclaratorId.identifier;
                Optional<String> getter = findGetter(enumDecl.name, enumFieldName);
                if (getter.isPresent()) {
                    for (EnumConstant constant : enumDecl.constants) {
                        String enumInstance = constant.identifier;
                        String enumPatternName = getEnumPatternName(enumInstance, enumClassNameParts) + opType.name;

                        enumFieldInfos.add(new EnumFieldPatternInfo(enumPatternName,
                                enumDecl.getFullQualifiedName(), enumDecl.name, enumInstance, getter.get()));
                    }
                }
            }

        }
        return enumFieldInfos;
    }

    protected Optional<String> findGetter(String type, String name) {
        String getterName = "get" + name.substring(0, 1).toUpperCase(Locale.ENGLISH) + name.substring(1);
        for (MethodDeclaration methodDecl : opType.methodDeclarations) {
            if (methodDecl.typeTypeOrVoid.isVoid) {
                continue;
            }
            if (methodDecl.typeTypeOrVoid.typeType.isPresent()
                    && methodDecl.typeTypeOrVoid.typeType.get().toString().equals(type)) {
                if (methodDecl.identifier.equals(getterName) && methodDecl.paramNum == 0) {
                    return Optional.of(getterName);
                }
            }
        }
        return Optional.empty();
    }

    protected String getEnumPatternName(String enumInstance, Set<String> enumClassNameParts) {
        String[] instanceNameParts = enumInstance.split("_+");
        List<String> newParts = new ArrayList<>();

        boolean isFirst = true;
        for (int i = 0; i < instanceNameParts.length; i++) {
            String part = instanceNameParts[i].toLowerCase(Locale.ENGLISH);
            // skip instanceNameParts, e.g. INNER_JOIN has two part: [inner and Join].
            // because 'Join' is the part of the 'JoinType' enum className, so skip 'Join' and return 'inner'
            if (part.isEmpty() || enumClassNameParts.contains(part)) {
                continue;
            }
            if (!isFirst) {
                newParts.add(part.substring(0, 1).toUpperCase(Locale.ENGLISH) + part.substring(1));
            } else {
                newParts.add(part.substring(0, 1).toLowerCase(Locale.ENGLISH) + part.substring(1));
            }
            isFirst = false;
        }

        return Joiner.on("").join(newParts);
    }

    protected List<Map.Entry<FieldDeclaration, EnumDeclaration>> findEnumFieldType() {
        return opType.fieldDeclarations
                .stream()
                .map(f -> new SimpleEntry<>(f, analyzer.getType(opType, f.type)))
                .filter(pair -> pair.getValue().isPresent() && pair.getValue().get() instanceof EnumDeclaration)
                .map(pair -> new SimpleEntry<>(pair.getKey(), (EnumDeclaration) (pair.getValue().get())))
                .collect(Collectors.toList());
    }

    // e.g. split PhysicalBroadcastHashJoin to [Physical, Broadcast, Hash, Join]
    // e.g. split JoinType to [Join, Type]
    protected List<String> splitCase(String name) {
        Pattern pattern = Pattern.compile("([A-Z]+[^A-Z]*)");
        Matcher matcher = pattern.matcher(name);
        List<String> parts = new ArrayList<>();
        while (matcher.find()) {
            parts.add(matcher.group(0));
        }
        return parts;
    }

    protected String childType() {
        return isMemoPattern ? "GroupPlan" : "Plan";
    }

    /** create generator by plan's type. */
    public static Optional<PlanPatternGenerator> create(PlanPatternGeneratorAnalyzer analyzer,
            ClassDeclaration opType, Set<String> parentClass, boolean isMemoPattern) {
        if (parentClass.contains("org.apache.doris.nereids.trees.plans.logical.LogicalLeaf")) {
            return Optional.of(new LogicalLeafPatternGenerator(analyzer, opType, parentClass, isMemoPattern));
        } else if (parentClass.contains("org.apache.doris.nereids.trees.plans.logical.LogicalUnary")) {
            return Optional.of(new LogicalUnaryPatternGenerator(analyzer, opType, parentClass, isMemoPattern));
        } else if (parentClass.contains("org.apache.doris.nereids.trees.plans.logical.LogicalBinary")) {
            return Optional.of(new LogicalBinaryPatternGenerator(analyzer, opType, parentClass, isMemoPattern));
        } else if (parentClass.contains("org.apache.doris.nereids.trees.plans.physical.PhysicalLeaf")) {
            return Optional.of(new PhysicalLeafPatternGenerator(analyzer, opType, parentClass, isMemoPattern));
        } else if (parentClass.contains("org.apache.doris.nereids.trees.plans.physical.PhysicalUnary")) {
            return Optional.of(new PhysicalUnaryPatternGenerator(analyzer, opType, parentClass, isMemoPattern));
        } else if (parentClass.contains("org.apache.doris.nereids.trees.plans.physical.PhysicalBinary")) {
            return Optional.of(new PhysicalBinaryPatternGenerator(analyzer, opType, parentClass, isMemoPattern));
        } else {
            return Optional.empty();
        }
    }

    private static String generateImports(List<PlanPatternGenerator> generators) {
        Set<String> imports = new HashSet<>();
        for (PlanPatternGenerator generator : generators) {
            imports.addAll(generator.getImports());
        }
        List<String> sortedImports = new ArrayList<>(imports);
        sortedImports.sort(Comparator.naturalOrder());

        return sortedImports.stream()
                .map(it -> "import " + it + ";\n")
                .collect(Collectors.joining(""));
    }

    /** generate some pattern method code. */
    public String generate(boolean isMemoPattern) {
        String opClassName = opType.name;
        String methodName = getPatternMethodName();

        generateTypePattern(methodName, opClassName, genericType(), "", false, isMemoPattern);
        if (childrenNum() > 0) {
            generateTypePattern(methodName, opClassName, genericTypeWithChildren(),
                    "", true, isMemoPattern);
        }

        for (EnumFieldPatternInfo info : enumFieldPatternInfos) {
            String predicate = ".when(p -> p." + info.enumInstanceGetter + "() == "
                    + info.enumType + "." + info.enumInstance + ")";
            generateTypePattern(info.patternName, opClassName, genericType(),
                    predicate, false, isMemoPattern);
            if (childrenNum() > 0) {
                generateTypePattern(info.patternName, opClassName, genericTypeWithChildren(),
                        predicate, true, isMemoPattern);
            }
        }
        return generatePatterns();
    }

    /** generate a pattern method code. */
    public String generateTypePattern(String patterName, String className,
            String genericParam, String predicate, boolean specifyChildren, boolean isMemoPattern) {
        int childrenNum = childrenNum();
        if (specifyChildren) {
            StringBuilder methodGenericBuilder = new StringBuilder("<");
            StringBuilder methodParamBuilder = new StringBuilder();
            StringBuilder childrenPatternBuilder = new StringBuilder();
            int min = Math.min(1, childrenNum);
            int max = Math.max(1, childrenNum);
            for (int i = min; i <= max; i++) {
                methodGenericBuilder.append("C").append(i).append(" extends Plan");
                methodParamBuilder.append("PatternDescriptor<C").append(i).append("> child").append(i);
                childrenPatternBuilder.append("child").append(i).append(".pattern");

                if (i < max) {
                    methodGenericBuilder.append(", ");
                    methodParamBuilder.append(", ");
                    childrenPatternBuilder.append(", ");
                }
            }
            methodGenericBuilder.append(">");

            if (childrenNum > 0) {
                childrenPatternBuilder.insert(0, ", ");
            }
            String pattern = "default " + methodGenericBuilder + "\n"
                    + "PatternDescriptor" + genericParam + "\n"
                    + "        " + patterName + "(" + methodParamBuilder + ") {\n"
                    + "    return new PatternDescriptor" + genericParam + "(\n"
                    + "        new TypePattern(" + className + ".class" + childrenPatternBuilder + "),\n"
                    + "        defaultPromise()\n"
                    + "    )" + predicate + ";\n"
                    + "}\n";
            generatePatterns.add(pattern);
            return pattern;
        }

        String childrenPattern = StringUtils.repeat(
                isMemoPattern ? "Pattern.GROUP" : "Pattern.ANY", ", ", childrenNum);
        if (childrenNum > 0) {
            childrenPattern = ", " + childrenPattern;
        }

        String pattern = "default PatternDescriptor" + genericParam + " " + patterName + "() {\n"
                + "    return new PatternDescriptor" + genericParam + "(\n"
                + "        new TypePattern(" + className + ".class" + childrenPattern + "),\n"
                + "        defaultPromise()\n"
                + "    )" + predicate + ";\n"
                + "}\n";
        generatePatterns.add(pattern);
        return pattern;
    }

    public String generatePatterns() {
        return generatePatterns.stream().collect(Collectors.joining("\n"));
    }

    static class EnumFieldPatternInfo {
        public final String patternName;
        public final String enumFullName;
        public final String enumType;
        public final String enumInstance;
        public final String enumInstanceGetter;

        public EnumFieldPatternInfo(String patternName, String enumFullName, String enumType,
                String enumInstance, String enumInstanceGetter) {
            this.patternName = patternName;
            this.enumFullName = enumFullName;
            this.enumType = enumType;
            this.enumInstance = enumInstance;
            this.enumInstanceGetter = enumInstanceGetter;
        }
    }
}
