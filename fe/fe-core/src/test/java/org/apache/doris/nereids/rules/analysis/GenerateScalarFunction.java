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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.functions.SignatureSupplier;
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.shape.LeafExpression;
import org.apache.doris.nereids.trees.expressions.shape.TernaryExpression;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.CaseUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class GenerateScalarFunction {
    @Test
    @Disabled
    @Developing
    public void test() {
        FunctionSet<Object> functionSet = new FunctionSet<>();
        functionSet.init();

        ArrayListMultimap<String, ScalarFunction> scalarFunctionMap = ArrayListMultimap.create();
        for (Entry<String, List<Function>> kv : functionSet.getVectorizedFunctions().entrySet()) {
            String functionName = kv.getKey();
            for (Function function : kv.getValue()) {
                if (function instanceof ScalarFunction && function.isUserVisible()) {
                    scalarFunctionMap.put(functionName, (ScalarFunction) function);
                }
            }
        }

        List<ScalarFunction> scalarFunctionInfo = scalarFunctionMap
                .entries()
                .stream()
                .map(kv -> kv.getValue())
                .collect(Collectors.toList());

        Map<String, Map<Integer, List<ScalarFunction>>> name2Functions = Maps.newTreeMap();
        for (ScalarFunction functionInfo : scalarFunctionInfo) {
            String functionName = functionInfo.getFunctionName().getFunction();
            Map<Integer, List<ScalarFunction>> arity2Functions = name2Functions.get(functionName);
            if (arity2Functions == null) {
                arity2Functions = new TreeMap<>();
                name2Functions.put(functionName, arity2Functions);
            }
            List<ScalarFunction> functionInfos = arity2Functions.get(functionInfo.getArgs().length);
            if (functionInfos == null) {
                functionInfos = Lists.newArrayList();
                arity2Functions.put(functionInfo.getArgs().length, functionInfos);
            }
            functionInfos.add(functionInfo);
        }

        Set<String> customNullableFunctions = ImmutableSet.of("if", "ifnull", "nvl", "coalesce",
                "concat_ws", "rand", "random", "unix_timestamp");

        List<String> codes = name2Functions
                .entrySet()
                .stream()
                .map(kv -> {
                    if (kv.getKey().startsWith("castto") || customNullableFunctions.contains(kv.getKey())) {
                        return "";
                    }
                    List<ScalarFunction> scalarFunctions = Lists.newArrayList();
                    for (List<ScalarFunction> functions : kv.getValue().values()) {
                        for (ScalarFunction function : functions) {
                            if (function.isUserVisible()) {
                                scalarFunctions.add(function);
                            }
                        }
                    }
                    String code = generateScalarFunctions(scalarFunctions, functionSet);
                    if (code.isEmpty()) {
                        throw new IllegalStateException("can not generate code for " + scalarFunctions.get(0).functionName());
                    }
                    return code;
                })
                .filter(code -> !code.isEmpty())
                .collect(Collectors.toList());

        // work in process, remove print in the future
        System.out.println(codes);
    }

    private String generateScalarFunctions(List<ScalarFunction> scalarFunctions, FunctionSet functionSet) {
        checkScalarFunctions(scalarFunctions);

        ScalarFunction scalarFunction = scalarFunctions.get(0);

        String functionName = scalarFunction.getFunctionName().getFunction();
        String className = CaseUtils.toCamelCase(functionName.replaceAll("%", ""), true, '_');

        boolean hasVarArg = scalarFunctions.stream().anyMatch(ScalarFunction::hasVarArgs);
        List<Class> interfaces = getInterfaces(functionName, hasVarArg, scalarFunctions, functionSet);

        String interfaceStr = interfaces.isEmpty()
                ? ""
                : " implements " + interfaces.stream()
                        .map(Class::getSimpleName)
                        .collect(Collectors.joining(", "));

        String code = generateScalaFunctionHeader(hasVarArg, interfaces)
                + "/**\n"
                + " * ScalarFunction '" + functionName + "'. This class is generated by GenerateScalarFunction.\n"
                + " */\n"
                + "public class " + className + " extends ScalarFunction" + interfaceStr + " {\n";

        code += "\n"
                + "    public static final List<FuncSig> SIGNATURES = ImmutableList.of(\n";

        List<String> signatures = Lists.newArrayList();
        for (ScalarFunction function : scalarFunctions) {
            String returnType = getDataTypeAndInstance(function.getReturnType()).second;
            String args = Arrays.stream(function.getArgs())
                    .map(type -> getDataTypeAndInstance(type).second)
                    .collect(Collectors.joining(", "));
            String buildArgs = function.hasVarArgs() ? "varArgs" : "args";
            signatures.add("            FuncSig.ret(" + returnType + ")." + buildArgs + "(" + args + ")");
        }
        code += StringUtils.join(signatures, ",\n")
                + "\n"
                + "    );\n"
                + "\n"
                + generateConstructors(className, scalarFunctions)
                + generateWithChildren(className, hasVarArg, scalarFunctions);

        return code + "}\n";
    }

    private List<Class> getInterfaces(String functionName, boolean hasVarArgs,
            List<ScalarFunction> scalarFunctions, FunctionSet functionSet) {
        Set<Integer> aritySet = scalarFunctions.stream()
                .map(f -> f.getArgs().length)
                .collect(Collectors.toSet());
        Class arityExpressionType = getArityExpressionType(hasVarArgs, aritySet);
        List<Class> interfaces = Lists.newArrayList();

        if (arityExpressionType != null) {
            interfaces.add(arityExpressionType);
        }
        interfaces.add(SignatureSupplier.class);

        ScalarFunction scalarFunction = scalarFunctions.get(0);
        boolean isPropagateNullable = scalarFunction.getNullableMode() == NullableMode.DEPEND_ON_ARGUMENT
                || functionSet.isNullResultWithOneNullParamFunctions(functionName);
        if (isPropagateNullable) {
            interfaces.add(PropagateNullable.class);
        } else if (scalarFunction.getNullableMode() == NullableMode.ALWAYS_NULLABLE) {
            interfaces.add(AlwaysNullable.class);
        } else if (scalarFunction.getNullableMode() == NullableMode.ALWAYS_NOT_NULLABLE) {
            interfaces.add(AlwaysNotNullable.class);
        } else {
            throw new IllegalStateException("Not support compute custom nullable property: " + functionName);
        }
        return interfaces;
    }

    private String generateWithChildren(String className, boolean hasVarArg, List<ScalarFunction> scalarFunctions) {
        Optional<Integer> minVarArity = scalarFunctions.stream()
                .filter(ScalarFunction::hasVarArgs)
                .map(f -> f.getArgs().length)
                .min(Ordering.natural());
        Set<Integer> sortedAritySet = Sets.newTreeSet(scalarFunctions.stream()
                .map(f -> f.getArgs().length)
                .collect(Collectors.toSet())
        );
        int minArity = sortedAritySet.stream().max(Ordering.natural()).get();
        int maxArity = sortedAritySet.stream().max(Ordering.natural()).get();

        String code = "    /**\n"
                + "     * withChildren.\n"
                + "     */\n"
                + "    @Override\n"
                + "    public Expression withChildren(List<Expression> children) {\n";
        List<String> argumentNumChecks = Lists.newArrayList();
        if (hasVarArg) {
            argumentNumChecks.add("children.size() >= " + minVarArity.get());
        }
        argumentNumChecks.addAll(sortedAritySet.stream()
                .filter(arity -> !hasVarArg || arity < minVarArity.get())
                .map(arity -> "children.size() == " + arity)
                .collect(Collectors.toList()));

        String argumentNumCheck = argumentNumChecks.stream()
                .collect(Collectors.joining("\n                || "));
        code += "        Preconditions.checkArgument(" + argumentNumCheck + ");\n";

        if (hasVarArg) {
            Iterator<Integer> arityIt = sortedAritySet.iterator();
            Integer arity;

            boolean isFirstIf = true;
            if (minArity == 0) {
                code += "        if (children.isEmpty() && arity() == 0) {\n"
                        + "            return this;\n"
                        + "        }";
                isFirstIf = false;
                arityIt.next(); // consume arity 0
            }

            // invoke new Function with fixed-length arguments
            while (arityIt.hasNext()) {
                arity = arityIt.next();
                if (arity >= minVarArity.get()) {
                    break;
                }

                String conditionPrefix = isFirstIf ? "        if" : "        else if";
                code += conditionPrefix + " (children.size() == " + arity + ") {\n"
                        + "            return new " + className + "(" + getWithChildrenParams(arity, false) + ");\n"
                        + "        }";
                isFirstIf = false;
            }

            // invoke new Function with variable-length arguments
            if (isFirstIf) {
                code += "        return new " + className + "(" + getWithChildrenParams(minVarArity.get(), true) + ");\n";
            } else {
                code += "        else {\n"
                        + "            return new " + className + "(" + getWithChildrenParams(minVarArity.get(), true) + ");\n"
                        + "        }";
            }
        } else {
            if (maxArity == 0) {
                // LeafExpression has default withChildren method
                return "";
            } else if (sortedAritySet.size() == 1) {
                String withChildrenParams = getWithChildrenParams(sortedAritySet.iterator().next(), false);
                code += "        return new " + className + "(" + withChildrenParams + ");\n";
            } else {
                Iterator<Integer> arityIt = sortedAritySet.iterator();
                Integer firstArity = arityIt.next();
                if (firstArity == 0) {
                    code += "        if (children.isEmpty() && arity() == 0) {\n"
                            + "            return this;\n"
                            + "        }";
                } else {
                    code += "        if (children.size() == " + firstArity + ") {\n"
                            + "            return new " + className + "(" + getWithChildrenParams(firstArity, false) + ");\n"
                            + "        }";
                }

                while (arityIt.hasNext()) {
                    Integer arity = arityIt.next();

                    if (arityIt.hasNext()) {
                        code += " else if (children.size() == " + arity + ") {\n"
                                + "            return new " + className + "(" + getWithChildrenParams(arity, false) + ");\n"
                                + "        }";
                    } else {
                        code += " else {\n"
                                + "            return new " + className + "(" + getWithChildrenParams(arity, false) + ");\n"
                                + "        }\n";
                    }
                }
            }
        }
        code += "    }\n";
        return code;
    }

    private String generateConstructors(String className, List<ScalarFunction> scalarFunctions) {
        Set<Integer> generatedConstructorArity = Sets.newTreeSet();

        String code = "";
        for (ScalarFunction scalarFunction : scalarFunctions) {
            int arity = scalarFunction.getArgs().length;
            if (generatedConstructorArity.contains(arity)) {
                continue;
            }
            generatedConstructorArity.add(arity);
            boolean isVarArg = scalarFunction.hasVarArgs();
            String constructorDeclareParams = getConstructorDeclareParams(arity, isVarArg);
            String constructorParams = getConstructorParams(arity, isVarArg);
            String functionName = scalarFunction.getFunctionName().getFunction();

            code += "    /**\n"
                    + "     * constructor with " + arity + (isVarArg ? " or more" : "") + " argument" + (arity > 1 || isVarArg ? "s.\n" : ".\n")
                    + "     */\n"
                    + "    public " + className + "(" + constructorDeclareParams + ") {\n"
                    + "        super(\"" + functionName + "\"" + (constructorParams.isEmpty() ? "" : ", " + constructorParams) + ");\n"
                    + "    }\n"
                    + "\n";
        }

        return code;
    }

    private void checkScalarFunctions(List<ScalarFunction> scalarFunctions) {
        if (scalarFunctions.size() <= 0) {
            return;
        }

        ScalarFunction firstFunction = scalarFunctions.get(0);
        String functionName = firstFunction.getFunctionName().getFunction();
        NullableMode nullableMode = firstFunction.getNullableMode();

        for (int i = 1; i < scalarFunctions.size(); i++) {
            Assertions.assertEquals(functionName, scalarFunctions.get(i).getFunctionName().getFunction());
            Assertions.assertEquals(nullableMode, scalarFunctions.get(i).getNullableMode(),
                    scalarFunctions.get(0).functionName() + " nullable mode not consistent");
        }
    }

    private Pair<String, String> getDataTypeAndInstance(Type type) {
        DataType dataType = DataType.fromLegacyType(type);
        String dataTypeClassName = dataType.getClass().getSimpleName();
        try {
            Field instanceField = dataType.getClass().getDeclaredField("INSTANCE");
            if (Modifier.isPublic(instanceField.getModifiers()) && Modifier.isStatic(instanceField.getModifiers())) {
                Object instance = instanceField.get(null);
                if (instance == dataType) {
                    return Pair.of(dataTypeClassName, dataTypeClassName + ".INSTANCE");
                }
            }
        } catch (Throwable t) {
            // skip exception
        }

        try {
            Field systemDefaultField = dataType.getClass().getDeclaredField("SYSTEM_DEFAULT");
            if (Modifier.isPublic(systemDefaultField.getModifiers())
                    && Modifier.isStatic(systemDefaultField.getModifiers())) {
                Object systemDefault = systemDefaultField.get(null);
                if (systemDefault == dataType) {
                    return Pair.of(dataTypeClassName, dataTypeClassName + ".SYSTEM_DEFAULT");
                }
            }
        } catch (Throwable t) {
            // skip exception
        }

        if (type.isArrayType()) {
            Type itemType = ((ArrayType) type).getItemType();
            return Pair.of("ArrayType", "ArrayType.of(" + getDataTypeAndInstance(itemType).second + ")");
        }

        throw new IllegalStateException("Unsupported generate code by data type: " + type);
    }

    private Class getArityExpressionType(boolean hasVarArg, Set<Integer> aritySet) {
        if (hasVarArg) {
            return null;
        }

        if (aritySet.size() > 1) {
            return null;
        }

        int arity = aritySet.iterator().next();
        if (arity == 0) {
            return LeafExpression.class;
        } else if (arity == 1) {
            return UnaryExpression.class;
        } else if (arity == 2) {
            return BinaryExpression.class;
        } else if (arity == 3) {
            return TernaryExpression.class;
        }
        return null;
    }

    private String getConstructorDeclareParams(int arity, boolean isVarArg) {
        List<String> params = Lists.newArrayList();
        for (int i = 0; i < arity; i++) {
            if (arity > 1) {
                params.add("Expression arg" + i);
            } else {
                params.add("Expression arg");
            }
        }
        if (isVarArg) {
            params.add("Expression... varArgs");
        }
        return StringUtils.join(params, ", ");
    }

    private String getConstructorParams(int arity, boolean isVarArg) {
        List<String> params = Lists.newArrayList();
        for (int i = 0; i < arity; i++) {
            if (arity > 1) {
                params.add("arg" + i);
            } else {
                params.add("arg");
            }
        }
        if (isVarArg) {
            params.add("varArgs");
            return "ExpressionUtils.mergeArguments(" + StringUtils.join(params, ", ") + ")";
        } else {
            return StringUtils.join(params, ", ");
        }
    }

    private String getWithChildrenParams(int arity, boolean isVarArg) {
        List<String> params = Lists.newArrayList();
        for (int i = 0; i < arity; i++) {
            params.add("children.get(" + i + ")");
        }
        if (isVarArg) {
            params.add("children.subList(" + arity + ", children.size()).toArray(new Expression[0])");
        }
        return StringUtils.join(params, ", ");
    }

    private String generateScalaFunctionHeader(boolean hasVarArgs, List<Class> interfaces) {
        List<Class> importDorisClasses = Lists.newArrayList(
                interfaces.stream().filter(i -> i.getPackage().getName().startsWith("org.apache.doris")).collect(
                        Collectors.toList()
                )
        );

        if (hasVarArgs) {
            importDorisClasses.add(ExpressionUtils.class);
        }

        List<Class> importThirdPartyClasses = Lists.newArrayList(
                interfaces.stream().filter(i -> !i.getPackage().getName().startsWith("org.apache.doris")).collect(
                        Collectors.toList()
                )
        );
        if (!interfaces.contains(LeafExpression.class)) {
            importThirdPartyClasses.add(Preconditions.class);
        }

        String code = "// Licensed to the Apache Software Foundation (ASF) under one\n"
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
                + "package org.apache.doris.nereids.trees.expressions.functions.scalar;\n"
                + "\n";

        if (!importDorisClasses.isEmpty()) {
            code += importDorisClasses.stream()
                    .sorted((c1, c2) -> Ordering.natural().compare(c1.getName(), c2.getName()))
                    .map(c -> "import " + c.getName() + ";\n")
                    .collect(Collectors.joining("")) + "\n";
        }

        if (!importThirdPartyClasses.isEmpty()) {
            code += importThirdPartyClasses.stream()
                    .sorted((c1, c2) -> Ordering.natural().compare(c1.getName(), c2.getName()))
                    .map(c -> "import " + c.getName() + ";\n")
                    .collect(Collectors.joining("")) + "\n";
        }
        return code;
    }
}
