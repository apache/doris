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

package org.apache.doris.analysis;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.FunctionName;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.persist.gson.GsonUtilsCatalog;
import org.apache.doris.persist.gson.RuntimeTypeAdapterFactory;

import com.google.gson.annotations.SerializedName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExprGsonSerializationTest {
    private static final Pattern CLASS_DECLARATION_PATTERN = Pattern.compile(
            "public\\s+(abstract\\s+)?(?:final\\s+)?class\\s+(\\w+)\\s+extends\\s+(\\w+)\\b");

    private static class ExprHolder {
        @SerializedName("expr")
        private Expr expr;
        @SerializedName("exprs")
        private List<Expr> exprs;

        private ExprHolder() {
        }

        private ExprHolder(Expr expr, List<Expr> exprs) {
            this.expr = expr;
            this.exprs = exprs;
        }
    }

    @Test
    public void testExprSamplesCoverAllConcreteRegisteredSubtypes() throws Exception {
        Assertions.assertEquals(getRegisteredConcreteSubtypes(GsonUtilsCatalog.class), createExprSamples().keySet());
        Assertions.assertEquals(getRegisteredConcreteSubtypes(GsonUtils.class), createExprSamples().keySet());
    }

    @Test
    public void testExprSamplesCoverAllConcreteExprSubtypesInHierarchyOrder() throws Exception {
        Assertions.assertEquals(getConcreteExprSubtypesFromSource(), new ArrayList<>(createExprSamples().keySet()));
    }

    @Test
    public void testExprRoundTripForAllConcreteRegisteredSubtypes() throws Exception {
        Map<Class<? extends Expr>, Expr> samples = createExprSamples();
        Assertions.assertAll(samples.entrySet().stream()
                .map(entry -> () -> assertExprRoundTrip(entry.getKey(), entry.getValue())));
    }

    @Test
    public void testExprHolderRoundTrip() {
        ExprHolder holder = new ExprHolder(
                createArithmeticExpr(),
                Arrays.asList(createSearchPredicate(), createVirtualSlotRef(), createLambdaFunctionExpr()));

        String json = GsonUtilsCatalog.GSON.toJson(holder);
        ExprHolder restored = GsonUtilsCatalog.GSON.fromJson(json, ExprHolder.class);

        Assertions.assertEquals(holder.expr.getClass(), restored.expr.getClass());
        Assertions.assertEquals(holder.exprs.size(), restored.exprs.size());
        for (int i = 0; i < holder.exprs.size(); i++) {
            Assertions.assertEquals(holder.exprs.get(i).getClass(), restored.exprs.get(i).getClass());
        }
        Assertions.assertEquals(json, GsonUtilsCatalog.GSON.toJson(restored));
    }

    private void assertExprRoundTrip(Class<? extends Expr> expectedClass, Expr expr) {
        String json = GsonUtilsCatalog.GSON.toJson(expr, Expr.class);
        Expr restored = GsonUtilsCatalog.GSON.fromJson(json, Expr.class);
        Assertions.assertEquals(expectedClass, restored.getClass());
        Assertions.assertEquals(json, GsonUtilsCatalog.GSON.toJson(restored, Expr.class));
    }

    private Map<Class<? extends Expr>, Expr> createExprSamples() throws Exception {
        LinkedHashMap<Class<? extends Expr>, Expr> samples = new LinkedHashMap<>();
        samples.put(ArithmeticExpr.class, createArithmeticExpr());
        samples.put(CaseExpr.class, createCaseExpr());
        samples.put(CastExpr.class, createCastExpr());
        samples.put(TryCastExpr.class, createTryCastExpr());
        samples.put(ColumnRefExpr.class, createColumnRefExpr());
        samples.put(EncryptKeyRef.class, new EncryptKeyRef(new EncryptKeyName(Arrays.asList("db1", "key1"))));
        samples.put(FunctionCallExpr.class, createFunctionCallExpr());
        samples.put(LambdaFunctionCallExpr.class, createLambdaFunctionCallExpr());
        samples.put(InformationFunction.class, createInformationFunction());
        samples.put(LambdaFunctionExpr.class, createLambdaFunctionExpr());
        samples.put(ArrayLiteral.class, new ArrayLiteral(new ArrayType(Type.INT), new IntLiteral(1L), new IntLiteral(2L)));
        samples.put(BoolLiteral.class, new BoolLiteral(true));
        samples.put(DateLiteral.class, new DateLiteral(2024, 4, 27, 12, 34, 56, Type.DATETIMEV2));
        samples.put(IPv4Literal.class, new IPv4Literal("192.168.1.7"));
        samples.put(IPv6Literal.class, new IPv6Literal("2001:db8::1"));
        samples.put(JsonLiteral.class, new JsonLiteral("{\"key\":\"value\",\"num\":1}"));
        samples.put(MapLiteral.class, createMapLiteral());
        samples.put(MaxLiteral.class, MaxLiteral.MAX_VALUE);
        samples.put(NullLiteral.class, NullLiteral.create(Type.VARCHAR));
        samples.put(DecimalLiteral.class, new DecimalLiteral(new BigDecimal("123.45"), Type.DECIMALV2));
        samples.put(FloatLiteral.class, new FloatLiteral(3.14159));
        samples.put(IntLiteral.class, new IntLiteral(123L));
        samples.put(LargeIntLiteral.class, new LargeIntLiteral("12345678901234567890"));
        samples.put(PlaceHolderExpr.class, new PlaceHolderExpr(new StringLiteral("placeholder")));
        samples.put(StringLiteral.class, new StringLiteral("expr-gson"));
        samples.put(StructLiteral.class, new StructLiteral(new StructType(),
                new IntLiteral(7L), new StringLiteral("field")));
        samples.put(TimeV2Literal.class, new TimeV2Literal(12, 34, 56, 123456, 6, false));
        samples.put(VarBinaryLiteral.class, new VarBinaryLiteral("bin".getBytes(StandardCharsets.UTF_8)));
        samples.put(BetweenPredicate.class, createBetweenPredicate());
        samples.put(BinaryPredicate.class, createBinaryPredicate());
        samples.put(CompoundPredicate.class, createCompoundPredicate());
        samples.put(InPredicate.class, createInPredicate());
        samples.put(IsNullPredicate.class, createIsNullPredicate());
        samples.put(LikePredicate.class, createLikePredicate());
        samples.put(MatchPredicate.class, createMatchPredicate());
        samples.put(SearchPredicate.class, createSearchPredicate());
        samples.put(SlotRef.class, createSlotRef());
        samples.put(VirtualSlotRef.class, createVirtualSlotRef());
        samples.put(TimestampArithmeticExpr.class, createTimestampArithmeticExpr());
        samples.put(VariableExpr.class, createVariableExpr());
        return samples;
    }

    private FunctionCallExpr createFunctionCallExpr() {
        return new FunctionCallExpr("ifnull",
                Arrays.asList(NullLiteral.create(Type.VARCHAR), new StringLiteral("fallback")), true);
    }

    private LambdaFunctionCallExpr createLambdaFunctionCallExpr() {
        ArrayType arrayType = new ArrayType(Type.INT);
        ScalarFunction function = new ScalarFunction(new FunctionName("array_map"),
                Arrays.asList(Type.LAMBDA_FUNCTION, arrayType), arrayType, false, true);
        FunctionParams params = new FunctionParams(Arrays.asList(
                createLambdaFunctionExpr(),
                new ArrayLiteral(arrayType, new IntLiteral(1L), new IntLiteral(2L))));
        return new LambdaFunctionCallExpr(function, params, false);
    }

    private ColumnRefExpr createColumnRefExpr() {
        ColumnRefExpr columnRefExpr = new ColumnRefExpr(false);
        columnRefExpr.setColumnId(1);
        columnRefExpr.setName("c1");
        columnRefExpr.setType(Type.INT);
        return columnRefExpr;
    }

    private CastExpr createCastExpr() {
        return new CastExpr(Type.BIGINT, new StringLiteral("7"), false);
    }

    private TryCastExpr createTryCastExpr() {
        return new TryCastExpr(Type.BIGINT, new StringLiteral("8"), true, false);
    }

    private TimestampArithmeticExpr createTimestampArithmeticExpr() {
        return new TimestampArithmeticExpr("date_add", ArithmeticExpr.Operator.ADD,
                new DateLiteral(2024, 4, 27, 12, 30, 0, Type.DATETIMEV2),
                new IntLiteral(2L), "DAY", Type.DATETIMEV2, false);
    }

    private IsNullPredicate createIsNullPredicate() {
        return new IsNullPredicate(new StringLiteral("value"), true);
    }

    private BetweenPredicate createBetweenPredicate() throws Exception {
        Constructor<BetweenPredicate> ctor = BetweenPredicate.class.getDeclaredConstructor();
        ctor.setAccessible(true);
        BetweenPredicate predicate = ctor.newInstance();
        predicate.addChildren(Arrays.asList(new IntLiteral(3L), new IntLiteral(1L), new IntLiteral(5L)));
        setDeclaredField(BetweenPredicate.class, predicate, "isNotBetween", true);
        return predicate;
    }

    private BinaryPredicate createBinaryPredicate() {
        return new BinaryPredicate(BinaryPredicate.Operator.GE, new IntLiteral(3L), new IntLiteral(2L));
    }

    private LikePredicate createLikePredicate() {
        return new LikePredicate(LikePredicate.Operator.LIKE,
                new StringLiteral("doris"), new StringLiteral("%or%"));
    }

    private MatchPredicate createMatchPredicate() {
        return new MatchPredicate(MatchPredicate.Operator.MATCH_ANY,
                new SlotRef(Type.VARCHAR, false), new StringLiteral("hello"),
                Type.BOOLEAN, NullableMode.DEPEND_ON_ARGUMENT, null, false, "english");
    }

    private SearchPredicate createSearchPredicate() {
        String dsl = "title:hello";
        SearchDslParser.QsPlan plan = SearchDslParser.parseDsl(dsl, "{\"mode\":\"standard\"}");
        return new SearchPredicate(dsl, plan, Collections.singletonList(createNamedSlotRef("title")), false);
    }

    private InPredicate createInPredicate() {
        return new InPredicate(new IntLiteral(1L),
                Arrays.asList(new IntLiteral(1L), new IntLiteral(2L), new IntLiteral(3L)),
                false, true, false);
    }

    private CompoundPredicate createCompoundPredicate() {
        return new CompoundPredicate(CompoundPredicate.Operator.OR, new BoolLiteral(true), new BoolLiteral(false), false);
    }

    private MapLiteral createMapLiteral() {
        return new MapLiteral(new MapType(Type.VARCHAR, Type.INT),
                Arrays.asList(new StringLiteral("k1"), new StringLiteral("k2")),
                Arrays.asList(new IntLiteral(1L), new IntLiteral(2L)));
    }

    private CaseExpr createCaseExpr() {
        return new CaseExpr(Collections.singletonList(
                new CaseWhenClause(new BoolLiteral(true), new IntLiteral(1L))), new IntLiteral(0L), false);
    }

    private LambdaFunctionExpr createLambdaFunctionExpr() {
        ColumnRefExpr columnRefExpr = new ColumnRefExpr(true);
        ArithmeticExpr lambdaBody = new ArithmeticExpr(ArithmeticExpr.Operator.ADD,
                columnRefExpr, new IntLiteral(1L), Type.BIGINT, NullableMode.ALWAYS_NOT_NULLABLE, false);
        return new LambdaFunctionExpr(lambdaBody, Collections.singletonList("x"),
                Collections.singletonList(columnRefExpr), false);
    }

    private ArithmeticExpr createArithmeticExpr() {
        return new ArithmeticExpr(ArithmeticExpr.Operator.ADD, new IntLiteral(5L), new IntLiteral(6L),
                Type.BIGINT, NullableMode.ALWAYS_NOT_NULLABLE, false);
    }

    private SlotRef createSlotRef() {
        SlotRef slotRef = createNamedSlotRef("col1");
        slotRef.setType(Type.BIGINT);
        return slotRef;
    }

    private VirtualSlotRef createVirtualSlotRef() {
        String json = GsonUtilsCatalog.GSON.toJson(createSlotRef(), Expr.class)
                .replace("\"clazz\":\"SlotRef\"", "\"clazz\":\"VirtualSlotRef\"");
        return (VirtualSlotRef) GsonUtilsCatalog.GSON.fromJson(json, Expr.class);
    }

    private InformationFunction createInformationFunction() throws Exception {
        InformationFunction informationFunction = new InformationFunction("CURRENT_USER");
        informationFunction.setType(Type.VARCHAR);
        setDeclaredField(InformationFunction.class, informationFunction, "strValue", "root");
        return informationFunction;
    }

    private VariableExpr createVariableExpr() {
        VariableExpr variableExpr = new VariableExpr("sql_mode");
        variableExpr.setStringValue("STRICT_TRANS_TABLES");
        variableExpr.setType(Type.VARCHAR);
        return variableExpr;
    }

    private SlotRef createNamedSlotRef(String col) {
        SlotRef slotRef = new SlotRef(new TableNameInfo("db1", "tbl1"), col);
        slotRef.setType(Type.VARCHAR);
        return slotRef;
    }

    @SuppressWarnings("unchecked")
    private Set<Class<? extends Expr>> getRegisteredConcreteSubtypes(Class<?> gsonUtilClass) throws Exception {
        Field factoryField = gsonUtilClass.getDeclaredField("exprAdapterFactory");
        factoryField.setAccessible(true);
        RuntimeTypeAdapterFactory<Expr> factory = (RuntimeTypeAdapterFactory<Expr>) factoryField.get(null);

        Field labelToSubtypeField = RuntimeTypeAdapterFactory.class.getDeclaredField("labelToSubtype");
        labelToSubtypeField.setAccessible(true);
        Map<String, Class<?>> labelToSubtype = (Map<String, Class<?>>) labelToSubtypeField.get(factory);

        return labelToSubtype.values().stream()
                .filter(clazz -> !Modifier.isAbstract(clazz.getModifiers()))
                .map(clazz -> (Class<? extends Expr>) clazz)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @SuppressWarnings("unchecked")
    private List<Class<? extends Expr>> getConcreteExprSubtypesFromSource() throws Exception {
        Map<String, ClassDeclaration> declarations = parseExprClassDeclarations();
        Map<String, List<String>> childrenByParent = new HashMap<>();
        declarations.forEach((className, declaration) -> childrenByParent
                .computeIfAbsent(declaration.parentClassName, key -> new ArrayList<>())
                .add(className));
        childrenByParent.values().forEach(children -> children.sort(Comparator.naturalOrder()));

        List<Class<? extends Expr>> orderedConcreteSubtypes = new ArrayList<>();
        collectConcreteExprSubtypes("Expr", declarations, childrenByParent, orderedConcreteSubtypes);
        return orderedConcreteSubtypes;
    }

    @SuppressWarnings("unchecked")
    private void collectConcreteExprSubtypes(String parentClassName, Map<String, ClassDeclaration> declarations,
            Map<String, List<String>> childrenByParent, List<Class<? extends Expr>> orderedConcreteSubtypes)
            throws Exception {
        for (String childClassName : childrenByParent.getOrDefault(parentClassName, Collections.emptyList())) {
            ClassDeclaration declaration = declarations.get(childClassName);
            if (!declaration.isAbstract) {
                orderedConcreteSubtypes.add((Class<? extends Expr>) Class.forName(
                        "org.apache.doris.analysis." + childClassName));
            }
            collectConcreteExprSubtypes(childClassName, declarations, childrenByParent, orderedConcreteSubtypes);
        }
    }

    private Map<String, ClassDeclaration> parseExprClassDeclarations() throws Exception {
        Path sourceDir = getExprSourceDir();
        Map<String, ClassDeclaration> declarations = new HashMap<>();
        try (Stream<Path> paths = Files.walk(sourceDir)) {
            for (Path path : paths.filter(Files::isRegularFile).filter(file -> file.toString().endsWith(".java"))
                    .collect(Collectors.toList())) {
                String content = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
                Matcher matcher = CLASS_DECLARATION_PATTERN.matcher(content);
                if (!matcher.find()) {
                    continue;
                }
                declarations.put(matcher.group(2),
                        new ClassDeclaration(matcher.group(1) != null, matcher.group(3)));
            }
        }
        return declarations;
    }

    private Path getExprSourceDir() {
        Path current = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
        List<Path> candidates = Arrays.asList(
                current.resolve("fe-catalog/src/main/java/org/apache/doris/analysis"),
                current.resolve("../fe-catalog/src/main/java/org/apache/doris/analysis").normalize());
        return candidates.stream().filter(Files::isDirectory).findFirst()
                .orElseThrow(() -> new IllegalStateException("Cannot locate Expr source directory from " + current));
    }

    private void setDeclaredField(Class<?> owner, Object target, String fieldName, Object value) throws Exception {
        Field field = owner.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static class ClassDeclaration {
        private final boolean isAbstract;
        private final String parentClassName;

        private ClassDeclaration(boolean isAbstract, String parentClassName) {
            this.isAbstract = isAbstract;
            this.parentClassName = parentClassName;
        }
    }
}
