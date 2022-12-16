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

import org.apache.doris.analysis.ArithmeticExpr;
import org.apache.doris.analysis.ArithmeticExpr.Operator;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.TimestampArithmeticExpr.TimeUnit;
import org.apache.doris.catalog.AggregateFunction;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.DateTimeWithPrecision;
import org.apache.doris.nereids.trees.expressions.functions.DecimalSamePrecision;
import org.apache.doris.nereids.trees.expressions.functions.DecimalStddevPrecision;
import org.apache.doris.nereids.trees.expressions.functions.DecimalWiderPrecision;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.IdenticalSignature;
import org.apache.doris.nereids.trees.expressions.functions.ImplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.Nondeterministic;
import org.apache.doris.nereids.trees.expressions.functions.NullOrIdenticalSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.shape.LeafExpression;
import org.apache.doris.nereids.trees.expressions.shape.TernaryExpression;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.CaseUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class GenerateScalarFunction {

    static final Set<String> unaryArithmeticOperators = Arrays.stream(ArithmeticExpr.Operator.values())
            .filter(ArithmeticExpr.Operator::isUnary)
            .map(Operator::getName)
            .collect(Collectors.toSet());

    static final Set<String> binaryArithmeticOperators = Arrays.stream(ArithmeticExpr.Operator.values())
            .filter(ArithmeticExpr.Operator::isBinary)
            .map(Operator::getName)
            .collect(Collectors.toSet());

    static final Set<String> binaryComparisonOperators = Arrays.stream(BinaryPredicate.Operator.values())
            .map(BinaryPredicate.Operator::getName)
            .collect(Collectors.toSet());

    static final Set<String> binaryCompoundPredicateOperators = ImmutableSet.of("and", "or");

    static final Set<String> unaryCompoundPredicateOperators = ImmutableSet.of("not");

    static final Set<String> inPredicateOperators = ImmutableSet.of("in_iterate", "not_in_iterate",
            "in_set_lookup", "not_in_set_lookup");

    static final Set<String> timestampOperators = ImmutableList.<String>builder()
            // the function name exists in TimestampArithmetic
            .add(
                "TIMESTAMPDIFF",
                // add
                "DATE_ADD",
                "DAYS_ADD",
                "ADDDATE",
                "TIMESTAMPADD",
                //sub
                "DATE_SUB",
                "DAYS_SUB",
                "SUBDATE"
            )
            // add other function which TimestampArithmetic.functionName is null
            .addAll(Arrays.stream(TimeUnit.values()).map(unit -> unit.toString() + "S_ADD").collect(Collectors.toSet()))
            .addAll(Arrays.stream(TimeUnit.values()).map(unit -> unit.toString() + "S_SUB").collect(Collectors.toSet()))
                    .build().stream().map(String::toLowerCase).collect(Collectors.toSet());

    static final Set<String> isNullOperators = ImmutableSet.of("is_null_pred", "is_not_null_pred");

    static final Set<String> likeOperators = ImmutableSet.of("like", "regexp");

    static final Set<String> groupingOperators = ImmutableSet.of("grouping", "grouping_id");

    static final Set<String> operators = ImmutableSet.<String>builder()
            .addAll(unaryArithmeticOperators)
            .addAll(binaryArithmeticOperators)
            .addAll(binaryComparisonOperators)
            .addAll(binaryCompoundPredicateOperators)
            .addAll(unaryCompoundPredicateOperators)
            .addAll(inPredicateOperators)
            .addAll(timestampOperators)
            .addAll(isNullOperators)
            .addAll(likeOperators)
            .addAll(groupingOperators)
            .build();

    static final Set<String> identicalSignatureFunctions = ImmutableSet.of(
            "multiply", "divide", "mod", "int_divide", "add", "subtract", "bitand", "bitor", "bitxor", "factorial",
            "grouping", "grouping_id"
    );

    static final Set<String> nullOrIdenticalSignatureFunctions = ImmutableSet.of(
            "is_null_pred", "is_not_null_pred"
    );

    static final Set<String> implicitlyCastableSignatureFunctions = ImmutableSet.of(
            "bitnot", "eq", "ne", "le", "ge", "lt", "gt", "eq_for_null"
    );

    static final Map<String, String> aliasToName = ImmutableMap.<String, String>builder()
            .put("substr", "substring")
            .put("ifnull", "nvl")
            .put("rand", "random")
            .put("add_months", "months_add")
            .put("curdate", "current_date")
            .put("ucase", "upper")
            .put("lcase", "lower")
            .build();

    static final Map<String, String> formatClassName = ImmutableMap.<String, String>builder()
            .put("localtimestamp", "LocalTimestamp")
            .put("localtime", "LocalTime")
            .put("weekofyear", "WeekOfYear")
            .put("yearweek", "YearWeek")
            .put("strright", "StrRight")
            .put("strleft", "StrLeft")
            .put("monthname", "MonthName")
            .put("md5sum", "Md5Sum")
            .put("makedate", "MakeDate")
            .put("datediff", "DateDiff")
            .put("timediff", "TimeDiff")
            .put("dayofmonth", "DayOfMonth")
            .put("dayofweek", "DayOfWeek")
            .put("dayofyear", "DayOfYear")
            .put("datev2", "DateV2")
            .put("dayname", "DayName")
            .put("esquery", "EsQuery")
            .put("nullif", "NullIf")
            .put("to_datev2", "ToDateV2")
            .build();

    static final Set<String> customNullableFunctions = ImmutableSet.of("if", "ifnull", "nvl", "coalesce",
            "concat_ws", "rand", "random", "unix_timestamp");

    static final List<FunctionCodeGenerator> customCodeGenerators = ImmutableList.of(
            new GenIf(),
            new GenNvl(),
            new GenCoalesce(),
            new GenConcatWs(),
            new GenRandom(),
            new GenUnixTimestamp(),
            new GenStrToDate(),
            new GenSubstring()
    );

    static final Set<String> customCastFunctions = ImmutableSet.of("%element_extract%", "concat_ws", "str_to_date");

    private static final ImmutableSet<String> DECIMAL_SAME_TYPE_SET =
            new ImmutableSortedSet.Builder(String.CASE_INSENSITIVE_ORDER)
                    .add("min").add("max").add("lead").add("lag")
                    .add("first_value").add("last_value").add("abs")
                    .add("positive").add("negative").build();
    private static final ImmutableSet<String> DECIMAL_WIDER_TYPE_SET =
            new ImmutableSortedSet.Builder(String.CASE_INSENSITIVE_ORDER)
                    .add("sum").add("avg").add("multi_distinct_sum").build();

    static boolean isIdenticalSignature(String functionName) {
        return functionName.startsWith("castto") || identicalSignatureFunctions.contains(functionName);
    }

    static boolean isNullOrIdenticalSignature(String functionName) {
        return nullOrIdenticalSignatureFunctions.contains(functionName);
    }

    static boolean isImplicitlyCastableSignature(String functionName) {
        return implicitlyCastableSignatureFunctions.contains(functionName);
    }

    static boolean isArrayFunction(Function function) {
        return function.getReturnType().isArrayType() || Arrays.stream(function.getArgs()).anyMatch(Type::isArrayType);
    }

    @Test
    @Disabled
    @Developing
    public void generate() throws IOException {
        Map<String, String> functionCodes = collectFunctionCodes();
        // Pair<className, Pair<functionName, code>>
        List<Pair<String, Pair<String, String>>> codeInfos = functionCodes.entrySet()
                .stream()
                .map(kv -> Pair.of(getClassName(kv.getKey()), Pair.of(kv.getKey(), kv.getValue())))
                .sorted(Comparator.comparing(Pair::key))
                .collect(Collectors.toList());

        System.out.println(codeInfos.stream().map(kv -> kv.second.first).collect(Collectors.joining("\n")));

        generateFunctionsFile(codeInfos);

        generateScalarFunctionVisitorFile(codeInfos);

        generateBuiltinScalarFunctionsFile(codeInfos);
    }

    private void generateFunctionsFile(List<Pair<String, Pair<String, String>>> codesInfo) throws IOException {
        File scalarFunPath = new File(getClass().getResource("/").getFile(),
                "../generated-sources/org/apache/doris/nereids/trees/expressions/functions/scalar");
        scalarFunPath.mkdirs();
        for (Pair<String, Pair<String, String>> codeInfo : codesInfo) {
            String className = codeInfo.key();
            String code = codeInfo.value().value();
            File functionFile = new File(scalarFunPath, className + ".java");
            if (!functionFile.exists()) {
                functionFile.createNewFile();
                FileUtils.writeStringToFile(functionFile, code, StandardCharsets.UTF_8);
            }
        }
    }

    private void generateBuiltinScalarFunctionsFile(List<Pair<String, Pair<String, String>>> codesInfo) throws IOException {
        File catalogPath = new File(getClass().getResource("/").getFile(),
                "../generated-sources/org/apache/doris/catalog");
        catalogPath.mkdirs();

        List<String> importFunctions = Lists.newArrayList();
        codesInfo.forEach(kv -> {
            importFunctions.add("org.apache.doris.nereids.trees.expressions.functions.scalar." + kv.first);
        });
        importFunctions.sort(this::sortImportPackageByCheckStyle);

        Multimap<String, String> aliasReverseMap = aliasReverseMap();
        List<String> registerCodes = Lists.newArrayList();
        for (Pair<String, Pair<String, String>> info : codesInfo) {
            String className = info.key();
            String functionName = info.value().key();

            Collection<String> aliases = aliasReverseMap.get(functionName);
            List<String> aliasList = Lists.newArrayList(functionName);
            aliasList.addAll(aliases);

            String alias = aliasList.stream()
                    .sorted()
                    .map(s -> '"' + s + '"')
                    .collect(Collectors.joining(", "));
            Collections.sort(aliasList);

            registerCodes.add("scalar(" + className + ".class, " + alias + ")");
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
                + "package org.apache.doris.catalog;\n"
                + "\n"
                + importFunctions.stream().map(i -> "import " + i + ";\n").collect(Collectors.joining())
                + "\n"
                + "import com.google.common.collect.ImmutableList;\n"
                + "\n"
                + "import java.util.List;\n"
                + "\n"
                + "/**\n"
                + " * Builtin scalar functions.\n"
                + " *\n"
                + " * Note: Please ensure that this class only has some lists and no procedural code.\n"
                + " *       It helps to be clear and concise.\n"
                + " */\n"
                + "public class BuiltinScalarFunctions implements FunctionHelper {\n"
                + "    public final List<ScalarFunc> scalarFunctions = ImmutableList.of(\n"
                + registerCodes.stream().map(r -> "            " + r).collect(Collectors.joining(",\n", "", "\n"))
                + "    );\n"
                + "\n"
                + "    public static final BuiltinScalarFunctions INSTANCE = new BuiltinScalarFunctions();\n"
                + "\n"
                + "    // Note: Do not add any code here!\n"
                + "    private BuiltinScalarFunctions() {}\n"
                + "}\n";

        FileUtils.writeStringToFile(new File(catalogPath, "BuiltinScalarFunctions.java"), code, StandardCharsets.UTF_8);
    }

    private Multimap<String, String> aliasReverseMap() {
        ArrayListMultimap<String, String> multimap = ArrayListMultimap.create();
        for (Entry<String, String> entry : aliasToName.entrySet()) {
            multimap.put(entry.getValue(), entry.getKey());
        }
        return multimap;
    }

    private void generateScalarFunctionVisitorFile(List<Pair<String, Pair<String, String>>> codesInfo) throws IOException {

        File visitorPath = new File(getClass().getResource("/").getFile(),
                "../generated-sources/org/apache/doris/nereids/trees/expressions/visitor");
        visitorPath.mkdirs();

        List<String> importFunctions = Lists.newArrayList("org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction");
        codesInfo.forEach(kv -> {
            importFunctions.add("org.apache.doris.nereids.trees.expressions.functions.scalar." + kv.first);
        });

        importFunctions.sort(this::sortImportPackageByCheckStyle);

        String code =
                "// Licensed to the Apache Software Foundation (ASF) under one\n"
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
                + "package org.apache.doris.nereids.trees.expressions.visitor;\n"
                + "\n";

        for (String importFunction : importFunctions) {
            code += "import " + importFunction + ";\n";
        }

        code += "\n"
                + "/** ScalarFunctionVisitor. */\n"
                + "public interface ScalarFunctionVisitor<R, C> {\n"
                + "\n"
                + "    R visitScalarFunction(ScalarFunction scalarFunction, C context);\n"
                + "\n";

        for (Pair<String, Pair<String, String>> kv : codesInfo) {
            code += generateVisitMethod(kv.key()) + "\n";
        }
        code = code.trim() + "\n}\n";
        FileUtils.writeStringToFile(new File(visitorPath, "ScalarFunctionVisitor.java"), code, StandardCharsets.UTF_8);
    }

    private String generateVisitMethod(String className) {
        String instanceName = className.substring(0, 1).toLowerCase() + className.substring(1);

        if (instanceName.equals("if") || instanceName.length() > 20) {
            instanceName = "function";
        }
        return "    default R visit" + className + "(" + className + " " + instanceName + ", C context) {\n"
                + "        return visitScalarFunction(" + instanceName + ", context);\n"
                + "    }\n";
    }

    private Map<String, String> collectFunctionCodes() {
        FunctionSet<Object> functionSet = new FunctionSet<>();
        functionSet.init();

        ArrayListMultimap<String, Function> functionMap = ArrayListMultimap.create();
        for (Entry<String, List<Function>> kv : functionSet.getVectorizedFunctions().entrySet()) {
            String functionName = kv.getKey();
            if (aliasToName.containsKey(functionName)) {
                continue;
            }
            for (Function function : kv.getValue()) {
                if (function instanceof ScalarFunction) {
                    // if (function.getArgs().length == 1 && function.getArgs()[0].isDatetimeV2()) {
                    //     System.out.println(function.getReturnType() + " " + functionName + "(datetimev2)");
                    // }
                    ScalarFunction scalarFunction = (ScalarFunction) function;
                    if (operators.contains(functionName) || functionName.startsWith("castto") || isArrayFunction(function)) {
                        continue;
                    }
                    functionMap.put(functionName, scalarFunction);
                }

                if (function instanceof AggregateFunction) {
                    functionMap.put(functionName, function);
                }
            }
        }

        List<Function> functionInfoList = functionMap
                .entries()
                .stream()
                .map(kv -> kv.getValue())
                .collect(Collectors.toList());

        Map<String, Map<Integer, List<Function>>> name2Functions = Maps.newTreeMap();
        for (Function functionInfo : functionInfoList) {
            String functionName = functionInfo.getFunctionName().getFunction();
            if (aliasToName.containsKey(functionName)) {
                continue;
            }
            Map<Integer, List<Function>> arity2Functions = name2Functions.get(functionName);
            if (arity2Functions == null) {
                arity2Functions = new TreeMap<>();
                name2Functions.put(functionName, arity2Functions);
            }
            List<Function> functionInfos = arity2Functions.get(functionInfo.getArgs().length);
            if (functionInfos == null) {
                functionInfos = Lists.newArrayList();
                arity2Functions.put(functionInfo.getArgs().length, functionInfos);
            }
            functionInfos.add(functionInfo);
        }

        Map<String, FunctionCodeGenerator> name2Generator = customCodeGenerators.stream()
                .collect(Collectors.toMap(
                        FunctionCodeGenerator::getFunctionName,
                        java.util.function.Function.identity())
                );

        return name2Functions
                .entrySet()
                .stream()
                .sorted(Comparator.comparing(Entry::getKey))
                .map(kv -> {
                    List<Function> functionsNeedGenerate = Lists.newArrayList();
                    for (List<Function> functions : kv.getValue().values()) {
                        for (Function function : functions) {
                            if (function.isUserVisible()) {
                                functionsNeedGenerate.add(function);
                            }
                        }
                    }
                    String code = generateFunctions(kv.getKey(), functionsNeedGenerate, functionSet, name2Generator);
                    if (code.isEmpty()) {
                        throw new IllegalStateException(
                                "can not generate code for " + functionsNeedGenerate.get(0).functionName());
                    }
                    return Pair.of(kv.getKey(), code);
                })
                .collect(Collectors.toMap(Pair::key, Pair::value));

    }

    private String getClassName(String functionName) {
        String className = formatClassName.get(functionName);
        if (className == null) {
            className = CaseUtils.toCamelCase(functionName.replaceAll("%", ""), true, '_');
        }
        return className;
    }

    private String generateFunctions(String functionName, List<Function> functions,
            FunctionSet functionSet, Map<String, FunctionCodeGenerator> name2Generator) {
        checkFunctions(functions);

        Class functionType = getFunctionType(functions.get(0));
        String className = getClassName(functionName);

        boolean hasVarArg = functions.stream().anyMatch(Function::hasVarArgs);
        if (hasVarArg && !functions.stream().allMatch(Function::hasVarArgs)) {
            throw new IllegalStateException("can not generate");
        }
        List<Class> interfaces = getInterfaces(functionName, hasVarArg, functions, functionSet);

        String interfaceStr = interfaces.isEmpty()
                ? ""
                : "\n        implements " + interfaces.stream()
                        .map(Class::getSimpleName)
                        .collect(Collectors.joining(", "));

        Optional<FunctionCodeGenerator> generator = Optional.ofNullable(name2Generator.get(functionName));

        Set<Class> imports = Sets.newHashSet(ExpressionVisitor.class);
        if (generator.isPresent()) {
            imports.addAll(generator.get().imports());
        }

        List<String> signatures = generateSignatures(functions, imports);

        String extendsClass = functionType.getSimpleName();
        if (FunctionCallExpr.TIME_FUNCTIONS_WITH_PRECISION.contains(functionName)) {
            extendsClass = "DateTimeWithPrecision";
            imports.add(DateTimeWithPrecision.class);
        }

        String code = generateFunctionHeader(functions, hasVarArg, interfaces, imports, functionType)
                + "/**\n"
                + " * " + functionType.getSimpleName() + " '" + functionName + "'. This class is generated by GenerateFunction.\n"
                + " */\n";

        if (generator.isPresent()) {
            code += generator.get().classComment();
        }
        code += "public class " + className + " extends " + extendsClass + interfaceStr + " {\n"
                + "\n"
                + "    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(\n";
        code += StringUtils.join(signatures, ",\n")
                + "\n"
                + "    );\n"
                + "\n";

        if (generator.isPresent()) {
            code += generator.get().fields();
        }

        code += generateConstructors(className, functions);

        if (generator.isPresent()) {
            code += generator.get().computeSignature();
        }

        if (generator.isPresent()) {
            code += generator.get().nullable();
        } else if (customNullableFunctions.contains(functionName)) {
            throw new IllegalStateException("custom nullable function should contains code generator");
        }

        if (generator.isPresent()) {
            code += generator.get().methods();
        }

        code += generateWithChildren(className, hasVarArg, functions);

        if (!signatures.isEmpty()) {
            code += "    @Override\n"
                    + "    public List<FunctionSignature> getSignatures() {\n"
                    + "        return SIGNATURES;\n"
                    + "    }\n"
                    + "\n";
        }

        code += generateAccept(className);

        return code.trim() + "\n}\n";
    }

    private Class getFunctionType(Function function) {
        if (function instanceof ScalarFunction) {
            return org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction.class;
        } else if (function instanceof AggregateFunction) {
            return org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction.class;
        } else {
            throw new IllegalStateException("Unknown function type: " + function.getClass());
        }
    }

    private List<String> generateSignatures(List<Function> functions, Set<Class> imports) {
        List<String> signatures = Lists.newArrayList();
        for (Function function : functions) {
            imports.add(DataType.fromCatalogType(function.getReturnType()).getClass());
            String returnType = getDataTypeAndInstance(function.getReturnType()).second;
            String args = Arrays.stream(function.getArgs())
                    .map(type -> {
                        imports.add(DataType.fromCatalogType(type).getClass());
                        return getDataTypeAndInstance(type).second;
                    })
                    .collect(Collectors.joining(", "));
            String buildArgs = function.hasVarArgs() ? "varArgs" : "args";
            String signature = "            FunctionSignature.ret(" + returnType + ")." + buildArgs + "(" + args + ")";
            if (signature.length() <= 119) {
                signatures.add(signature);
                continue;
            }

            String returnLine = "            FunctionSignature.ret(" + returnType + ")\n";
            String argumentsLine = "                    ." + buildArgs + "(" + args + ")";
            if (argumentsLine.length() <= 119) {
                signatures.add(returnLine + argumentsLine);
                continue;
            }

            String[] arguments = args.split(", ");
            String oneArgumentOneLine = Arrays.stream(arguments).collect(Collectors.joining(",\n"
                    + "                            "));
            signatures.add("            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT)\n"
                    + "                    .args(" + oneArgumentOneLine + ")");
        }
        return signatures;
    }

    private String generateAccept(String className) {
        return "    @Override\n"
                + "    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {\n"
                + "        return visitor.visit" + className + "(this, context);\n"
                + "    }\n"
                + "\n";
    }

    private List<Class> getInterfaces(String functionName, boolean hasVarArgs,
            List<Function> scalarFunctions, FunctionSet functionSet) {
        Set<Integer> aritySet = scalarFunctions.stream()
                .map(f -> f.getArgs().length)
                .collect(Collectors.toSet());
        Class arityExpressionType = getArityExpressionType(hasVarArgs, aritySet);
        List<Class> interfaces = Lists.newArrayList();

        if (arityExpressionType != null) {
            interfaces.add(arityExpressionType);
        }
        interfaces.add(getComputeSignatureInterface(functionName));
        if (functionSet.isNondeterministicFunction(functionName)) {
            interfaces.add(Nondeterministic.class);
        }

        Function scalarFunction = scalarFunctions.get(0);
        if (!customNullableFunctions.contains(functionName)) {
            boolean isPropagateNullable = scalarFunction.getNullableMode() == NullableMode.DEPEND_ON_ARGUMENT;
            if (isPropagateNullable && !functionName.equals("substring")) {
                interfaces.add(PropagateNullable.class);
            } else if (scalarFunction.getNullableMode() == NullableMode.ALWAYS_NULLABLE || functionName.equals("substring")) {
                interfaces.add(AlwaysNullable.class);
            } else if (scalarFunction.getNullableMode() == NullableMode.ALWAYS_NOT_NULLABLE) {
                interfaces.add(AlwaysNotNullable.class);
            }
        }

        if (DECIMAL_SAME_TYPE_SET.contains(functionName)) {
            interfaces.add(DecimalSamePrecision.class);
        } else if (DECIMAL_WIDER_TYPE_SET.contains(functionName)) {
            interfaces.add(DecimalWiderPrecision.class);
        } else if (FunctionCallExpr.STDDEV_FUNCTION_SET.contains(functionName)) {
            interfaces.add(DecimalStddevPrecision.class);
        }
        return interfaces;
    }

    private Class getComputeSignatureInterface(String functionName) {
        if (isIdenticalSignature(functionName)) {
            return IdenticalSignature.class;
        } else if (isNullOrIdenticalSignature(functionName)) {
            return NullOrIdenticalSignature.class;
        } else if (isImplicitlyCastableSignature(functionName)) {
            return ImplicitlyCastableSignature.class;
        } else {
            return ExplicitlyCastableSignature.class;
        }
    }

    private String generateWithChildren(String className, boolean hasVarArg, List<Function> scalarFunctions) {
        Optional<Integer> minVarArity = scalarFunctions.stream()
                .filter(Function::hasVarArgs)
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
                + "    public " + className + " withChildren(List<Expression> children) {\n";
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
        code += "    }\n\n";
        return code;
    }

    private String generateConstructors(String className, List<Function> scalarFunctions) {
        Set<Integer> generatedConstructorArity = Sets.newTreeSet();

        String code = "";
        for (Function scalarFunction : scalarFunctions) {
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

    private void checkFunctions(List<Function> functions) {
        if (functions.size() <= 0) {
            return;
        }

        Function firstFunction = functions.get(0);
        String functionName = firstFunction.getFunctionName().getFunction();
        NullableMode nullableMode = firstFunction.getNullableMode();

        boolean isScalarFunction = firstFunction instanceof ScalarFunction;
        boolean isAggregateFunction = firstFunction instanceof AggregateFunction;

        for (int i = 1; i < functions.size(); i++) {
            boolean functionIsScalarFunction = functions.get(i) instanceof ScalarFunction;
            boolean functionIsAggregateFunction = functions.get(i) instanceof AggregateFunction;

            Assertions.assertEquals(isScalarFunction, functionIsScalarFunction);
            Assertions.assertEquals(isAggregateFunction, functionIsAggregateFunction);

            Assertions.assertEquals(functionName, functions.get(i).getFunctionName().getFunction());

            if (!customNullableFunctions.contains(functionName)) {
                Assertions.assertEquals(nullableMode, functions.get(i).getNullableMode(),
                        functions.get(0).functionName() + " nullable mode not consistent");
            }
        }
    }

    private Pair<String, String> getDataTypeAndInstance(Type type) {
        DataType dataType = DataType.fromCatalogType(type);
        String dataTypeClassName = dataType.getClass().getSimpleName();

        String constantInstanceName = findConstantInstanceName(dataType);
        if (constantInstanceName != null) {
            return Pair.of(dataTypeClassName, dataTypeClassName + "." + constantInstanceName);
        }

        if (type.isArrayType()) {
            Type itemType = ((ArrayType) type).getItemType();
            return Pair.of("ArrayType", "ArrayType.of(" + getDataTypeAndInstance(itemType).second + ")");
        }

        throw new IllegalStateException("Unsupported generate code by data type: " + type);
    }

    private String findConstantInstanceName(DataType dataType) {
        try {
            for (Field field : dataType.getClass().getDeclaredFields()) {
                if (Modifier.isPublic(field.getModifiers()) && Modifier.isStatic(field.getModifiers())
                        && Modifier.isFinal(field.getModifiers())) {
                    Object instance = field.get(null);
                    if (instance == dataType) {
                        return field.getName();
                    }
                }
            }
        } catch (Throwable t) {
            return null;
        }
        return null;
    }

    private boolean instanceExists(DataType dataType, String constantField) {
        try {
            Field field = dataType.getClass().getDeclaredField(constantField);
            if (Modifier.isPublic(field.getModifiers()) && Modifier.isStatic(field.getModifiers())) {
                Object instance = field.get(null);
                if (instance == dataType) {
                    return true;
                }
            }
        } catch (Throwable t) {
            // skip exception
        }
        return false;
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
            return StringUtils.join(params, ", ")
                    + ",\n                children.subList(" + arity + ", children.size()).toArray(new Expression[0])";
        } else {
            return StringUtils.join(params, ", ");
        }
    }

    private String generateFunctionHeader(List<Function> scalarFunctions,
            boolean hasVarArgs, List<Class> interfaces, Set<Class> imports, Class functionType) {
        List<Class> importDorisClasses = Lists.newArrayList(
                interfaces.stream()
                        .filter(i -> i.getPackage().getName().startsWith("org.apache.doris"))
                        .collect(Collectors.toList()
                )
        );
        importDorisClasses.addAll(imports.stream()
                .filter(i -> i.getPackage().getName().startsWith("org.apache.doris"))
                .collect(Collectors.toList()));

        List<Class> importThirdPartyClasses = Lists.newArrayList(
                interfaces.stream()
                        .filter(i -> !i.getPackage().getName().startsWith("org.apache.doris")
                                && !i.getPackage().getName().startsWith("java."))
                        .collect(Collectors.toList()
                )
        );
        importThirdPartyClasses.addAll(imports.stream()
                .filter(i -> !i.getPackage().getName().startsWith("org.apache.doris")
                        && !i.getPackage().getName().startsWith("java."))
                .collect(Collectors.toList()));

        importDorisClasses.add(Expression.class);
        if (!scalarFunctions.isEmpty()) {
            importDorisClasses.add(FunctionSignature.class);
            importThirdPartyClasses.add(ImmutableList.class);
        }
        if (hasVarArgs) {
            importDorisClasses.add(ExpressionUtils.class);
        }

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
                    // sort import package by the checkstyle
                    .sorted(Comparator.comparing(Class::getName, this::sortImportPackageByCheckStyle))
                    .map(c -> "import " + c.getName() + ";\n")
                    .collect(Collectors.joining("")) + "\n";
        }

        if (!importThirdPartyClasses.isEmpty()) {
            code += importThirdPartyClasses.stream()
                    .sorted(Comparator.comparing(Class::getName, this::sortImportPackageByCheckStyle))
                    .map(c -> "import " + c.getName() + ";\n")
                    .collect(Collectors.joining("")) + "\n";
        }

        Set<Class> importJdkClasses = Sets.newHashSet();
        importJdkClasses.addAll(imports.stream()
                .filter(i -> i.getPackage().getName().startsWith("java."))
                .collect(Collectors.toList()));

        if (!scalarFunctions.isEmpty() || !interfaces.contains(LeafExpression.class)) {
            importJdkClasses.add(List.class);
        }
        if (!importJdkClasses.isEmpty()) {
            code += importJdkClasses.stream()
                    .sorted(Comparator.comparing(Class::getName, this::sortImportPackageByCheckStyle))
                    .map(c -> "import " + c.getName() + ";\n")
                    .collect(Collectors.joining("")) + "\n";
        }
        return code;
    }

    private int sortImportPackageByCheckStyle(String c1, String c2) {
        String[] p1 = c1.split("\\.");
        String[] p2 = c2.split("\\.");

        for (int i = 0; i < Math.min(p1.length, p2.length); ++i) {
            boolean leftIsClassName = i + 1 == p1.length;
            boolean rightIsClassName = i + 1 == p2.length;

            if (leftIsClassName && rightIsClassName) {
                return p1[i].compareTo(p2[i]);
            } else if (leftIsClassName) {
                // left package name is shorter than right package name
                return -1;
            } else if (rightIsClassName) {
                // right package name is shorter than left package name
                return 1;
            } else {
                int result = p1[i].compareTo(p2[i]);
                if (result != 0) {
                    return result;
                }
            }
        }
        if (p1.length < p2.length) {
            return -1;
        } else if (p1.length > p2.length) {
            return 1;
        } else {
            return 0;
        }
    }

    static class FunctionCodeGenerator {
        public final String functionName;

        public FunctionCodeGenerator(String functionName) {
            this.functionName = functionName;
        }

        public String getFunctionName() {
            return functionName;
        }

        public List<Class> imports() {
            return ImmutableList.of();
        }

        public String classComment() {
            return "";
        }

        public String fields() {
            return "";
        }

        public String methods() {
            return "";
        }

        public String computeSignature() {
            return "";
        }

        public String nullable() {
            return "";
        }
    }

    static class GenIf extends FunctionCodeGenerator {
        public GenIf() {
            super("if");
        }

        @Override
        public List<Class> imports() {
            return ImmutableList.of(Type.class, ScalarType.class, Supplier.class, Suppliers.class, DataType.class);
        }

        @Override
        public String fields() {
            return "    private final Supplier<DataType> widerType = Suppliers.memoize(() -> {\n"
                    + "        List<DataType> argumentsTypes = getSignature().argumentsTypes;\n"
                    + "        Type assignmentCompatibleType = ScalarType.getAssignmentCompatibleType(\n"
                    + "                argumentsTypes.get(1).toCatalogDataType(),\n"
                    + "                argumentsTypes.get(2).toCatalogDataType(),\n"
                    + "                true);\n"
                    + "        return DataType.fromCatalogType(assignmentCompatibleType);\n"
                    + "    });\n"
                    + "\n";
        }

        @Override
        public String computeSignature() {
            return "    @Override\n"
                    + "    protected FunctionSignature computeSignature(FunctionSignature signature) {\n"
                    + "        DataType widerType = this.widerType.get();\n"
                    + "        signature = signature.withArgumentTypes(children(), (sigType, argType) -> widerType)\n"
                    + "                .withReturnType(widerType);\n"
                    + "        return super.computeSignature(signature);\n"
                    + "    }\n"
                    + "\n";
        }

        @Override
        public String nullable() {
            return "    /**\n"
                    + "     * custom compute nullable.\n"
                    + "     */\n"
                    + "    @Override\n"
                    + "    public boolean nullable() {\n"
                    + "        for (int i = 1; i < arity(); i++) {\n"
                    + "            if (child(i).nullable()) {\n"
                    + "                return true;\n"
                    + "            }\n"
                    + "        }\n"
                    + "        return false;\n"
                    + "    }\n"
                    + "\n";
        }
    }

    static class GenNvl extends FunctionCodeGenerator {
        public GenNvl() {
            super("nvl");
        }

        @Override
        public String nullable() {
            return "    /**\n"
                    + "     * custom compute nullable.\n"
                    + "     */\n"
                    + "    @Override\n"
                    + "    public boolean nullable() {\n"
                    + "        return child(0).nullable() && child(1).nullable();\n"
                    + "    }\n"
                    + "\n";
        }
    }

    static class GenCoalesce extends FunctionCodeGenerator {
        public GenCoalesce() {
            super("coalesce");
        }

        @Override
        public String nullable() {
            return "    /**\n"
                    + "     * custom compute nullable.\n"
                    + "     */\n"
                    + "    @Override\n"
                    + "    public boolean nullable() {\n"
                    + "        for (Expression argument : children) {\n"
                    + "            if (!argument.nullable()) {\n"
                    + "                return false;\n"
                    + "            }\n"
                    + "        }\n"
                    + "        return true;\n"
                    + "    }\n"
                    + "\n";
        }
    }

    static class GenConcatWs extends FunctionCodeGenerator {
        public GenConcatWs() {
            super("concat_ws");
        }

        @Override
        public String nullable() {
            return "    /**\n"
                    + "     * custom compute nullable.\n"
                    + "     */\n"
                    + "    @Override\n"
                    + "    public boolean nullable() {\n"
                    + "        return child(0).nullable();\n"
                    + "    }\n"
                    + "\n";
        }
    }

    static class GenRandom extends FunctionCodeGenerator {
        public GenRandom() {
            super("random");
        }

        @Override
        public String nullable() {
            return "    /**\n"
                    + "     * custom compute nullable.\n"
                    + "     */\n"
                    + "    @Override\n"
                    + "    public boolean nullable() {\n"
                    + "        if (arity() > 0) {\n"
                    + "            return children().stream().anyMatch(Expression::nullable);\n"
                    + "        } else {\n"
                    + "            return false;\n"
                    + "        }\n"
                    + "    }\n"
                    + "\n";
        }
    }

    static class GenUnixTimestamp extends FunctionCodeGenerator {
        public GenUnixTimestamp() {
            super("unix_timestamp");
        }

        @Override
        public String nullable() {
            return "    /**\n"
                    + "     * custom compute nullable.\n"
                    + "     */\n"
                    + "    @Override\n"
                    + "    public boolean nullable() {\n"
                    + "        return arity() > 0;\n"
                    + "    }\n"
                    + "\n";
        }
    }

    static class GenStrToDate extends FunctionCodeGenerator {
        public GenStrToDate() {
            super("str_to_date");
        }

        @Override
        public List<Class> imports() {
            return ImmutableList.of(
                    ScalarType.class,
                    DataType.class,
                    DateLiteral.class,
                    Type.class,
                    StringLikeLiteral.class
            );
        }

        @Override
        public String computeSignature() {
            return "    @Override\n"
                    + "    protected FunctionSignature computeSignature(FunctionSignature signature) {\n"
                    + "        /*\n"
                    + "         * The return type of str_to_date depends on whether the time part is included in the format.\n"
                    + "         * If included, it is datetime, otherwise it is date.\n"
                    + "         * If the format parameter is not constant, the return type will be datetime.\n"
                    + "         * The above judgment has been completed in the FE query planning stage,\n"
                    + "         * so here we directly set the value type to the return type set in the query plan.\n"
                    + "         *\n"
                    + "         * For example:\n"
                    + "         * A table with one column k1 varchar, and has 2 lines:\n"
                    + "         *     \"%Y-%m-%d\"\n"
                    + "         *     \"%Y-%m-%d %H:%i:%s\"\n"
                    + "         * Query:\n"
                    + "         *     SELECT str_to_date(\"2020-09-01\", k1) from tbl;\n"
                    + "         * Result will be:\n"
                    + "         *     2020-09-01 00:00:00\n"
                    + "         *     2020-09-01 00:00:00\n"
                    + "         *\n"
                    + "         * Query:\n"
                    + "         *      SELECT str_to_date(\"2020-09-01\", \"%Y-%m-%d\");\n"
                    + "         * Return type is DATE\n"
                    + "         *\n"
                    + "         * Query:\n"
                    + "         *      SELECT str_to_date(\"2020-09-01\", \"%Y-%m-%d %H:%i:%s\");\n"
                    + "         * Return type is DATETIME\n"
                    + "         */\n"
                    + "        DataType returnType;\n"
                    + "        if (child(1) instanceof StringLikeLiteral) {\n"
                    + "            if (DateLiteral.hasTimePart(((StringLikeLiteral) child(1)).getStringValue())) {\n"
                    + "                returnType = DataType.fromCatalogType(ScalarType.getDefaultDateType(Type.DATETIME));\n"
                    + "            } else {\n"
                    + "                returnType = DataType.fromCatalogType(ScalarType.getDefaultDateType(Type.DATE));\n"
                    + "            }\n"
                    + "        } else {\n"
                    + "            returnType = DataType.fromCatalogType(ScalarType.getDefaultDateType(Type.DATETIME));\n"
                    + "        }\n"
                    + "        return signature.withReturnType(returnType);\n"
                    + "    }\n"
                    + "\n";
        }
    }

    static class GenSubstring extends FunctionCodeGenerator {
        public GenSubstring() {
            super("substring");
        }

        @Override
        public List<Class> imports() {
            return ImmutableList.of(DataType.class, Optional.class, IntegerLiteral.class);
        }

        public String classComment() {
            return "// TODO: to be compatible with BE, we set AlwaysNullable here.\n";
        }

        public String methods() {
            return "    public Expression getSource() {\n"
                    + "        return child(0);\n"
                    + "    }\n"
                    + "\n"
                    + "    public Expression getPosition() {\n"
                    + "        return child(1);\n"
                    + "    }\n"
                    + "\n"
                    + "    public Optional<Expression> getLength() {\n"
                    + "        return arity() == 3 ? Optional.of(child(2)) : Optional.empty();\n"
                    + "    }\n"
                    + "\n";
        }

        @Override
        public String computeSignature() {
            return "    @Override\n"
                    + "    protected FunctionSignature computeSignature(FunctionSignature signature) {\n"
                    + "        Optional<Expression> length = getLength();\n"
                    + "        DataType returnType = VarcharType.SYSTEM_DEFAULT;\n"
                    + "        if (length.isPresent() && length.get() instanceof IntegerLiteral) {\n"
                    + "            returnType = VarcharType.createVarcharType(((IntegerLiteral) length.get()).getValue());\n"
                    + "        }\n"
                    + "        return signature.withReturnType(returnType);\n"
                    + "    }\n"
                    + "\n";
        }
    }
}
