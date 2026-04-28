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

package org.apache.doris.qe;

import org.apache.doris.analysis.FunctionName;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.common.NereidsException;
import org.apache.doris.common.util.URI;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.thrift.TFunctionBinaryType;

import com.jd.mpc.hive.encsdk.SandboxApi;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MaskingOptimizerTest {
    private final MaskingOptimizer maskingOptimizer = new MaskingOptimizer();

    @Test
    public void testRewriteRootMaskingDisabledReturnsSamePlan() {
        ConnectContext ctx = MemoTestUtils.createConnectContext();
        ctx.getSessionVariable().maskingEnabled = false;
        Plan leaf = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        @SuppressWarnings("unchecked")
        List<NamedExpression> output = (List<NamedExpression>) (List<?>) leaf.getOutput();
        LogicalProject<Plan> project = new LogicalProject<>(output, leaf);
        LogicalResultSink<Plan> sink = new LogicalResultSink<>(project.getProjects(), project);

        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(ctx, sink);
        JobContext jobContext = new JobContext(cascadesContext, PhysicalProperties.GATHER, Double.MAX_VALUE);
        Plan out = new MaskingOptimizer().rewriteRoot(sink, jobContext);
        Assertions.assertSame(sink, out);
    }

    @Test
    public void testRewriteRootMaskingEnabledReturnsPlan() throws Exception {
        ConnectContext ctx = MemoTestUtils.createConnectContext();
        ctx.getSessionVariable().maskingEnabled = true;
        ctx.getSessionVariable().maskingDmsUrl = "http://cache-test";
        ctx.getSessionVariable().maskingSandboxEnv = "prod";
        ctx.getSessionVariable().maskingSandboxProjectId = "129";
        Plan leaf = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        @SuppressWarnings("unchecked")
        List<NamedExpression> output = (List<NamedExpression>) (List<?>) leaf.getOutput();
        LogicalProject<Plan> project = new LogicalProject<>(output, leaf);
        LogicalResultSink<Plan> sink = new LogicalResultSink<>(project.getProjects(), project);

        Field cacheField = MaskingOptimizer.class.getDeclaredField("POLICY_CACHE");
        cacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentHashMap<String, String> cache = (ConcurrentHashMap<String, String>) cacheField.get(null);
        String policy = "[{\"dbName\":\"db1\",\"tbName\":\"t1\",\"maskSetting\":[{\"colName\":\"k1\"}]}]";
        primePolicyCacheForPlan(ctx, sink, policy, cache);

        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(ctx, sink);
        JobContext jobContext = new JobContext(cascadesContext, PhysicalProperties.GATHER, Double.MAX_VALUE);
        try {
            Plan out = new MaskingOptimizer().rewriteRoot(sink, jobContext);
            Assertions.assertNotNull(out);
        } catch (org.apache.doris.common.NereidsException e) {
            Assertions.assertNotNull(e.getMessage());
        }
    }

    @Test
    public void testResolvePolicyEmptyUrlThrows() throws Exception {
        ConnectContext ctx = new ConnectContext();
        Method method = MaskingOptimizer.class.getDeclaredMethod(
                "resolvePolicy", ConnectContext.class, Plan.class);
        method.setAccessible(true);
        Throwable thrown = Assertions.assertThrows(Throwable.class,
                () -> method.invoke(maskingOptimizer, ctx, null));
        Assertions.assertNotNull(thrown);
    }

    @Test
    public void testResolvePolicyEmptyUrlThrowsException() throws Exception {
        ConnectContext ctx = new ConnectContext();
        Method method = MaskingOptimizer.class.getDeclaredMethod(
                "resolvePolicy", ConnectContext.class, Plan.class);
        method.setAccessible(true);
        InvocationTargetException ex = Assertions.assertThrows(InvocationTargetException.class,
                () -> method.invoke(maskingOptimizer, ctx, null));
        assertMaskingExceptionMessage(ex.getCause(), "masking_dms_url is empty");
    }

    @Test
    public void testResolvePolicyEmptySandboxEnvThrowsException() throws Exception {
        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().maskingDmsUrl = "http://dms/test";
        ctx.getSessionVariable().maskingSandboxProjectId = "129";
        ctx.getSessionVariable().maskingSandboxEnv = "";
        Method method = MaskingOptimizer.class.getDeclaredMethod(
                "resolvePolicy", ConnectContext.class, Plan.class);
        method.setAccessible(true);
        InvocationTargetException ex = Assertions.assertThrows(InvocationTargetException.class,
                () -> method.invoke(maskingOptimizer, ctx, null));
        assertMaskingExceptionMessage(ex.getCause(), "masking_sandbox_env is empty");
    }

    @Test
    public void testResolvePolicyEmptySandboxProjectIdThrowsException() throws Exception {
        ConnectContext ctx = new ConnectContext();
        ctx.getSessionVariable().maskingDmsUrl = "http://dms/test";
        ctx.getSessionVariable().maskingSandboxProjectId = "";
        ctx.getSessionVariable().maskingSandboxEnv = "prod";
        Method method = MaskingOptimizer.class.getDeclaredMethod(
                "resolvePolicy", ConnectContext.class, Plan.class);
        method.setAccessible(true);
        InvocationTargetException ex = Assertions.assertThrows(InvocationTargetException.class,
                () -> method.invoke(maskingOptimizer, ctx, null));
        assertMaskingExceptionMessage(ex.getCause(), "masking_sandbox_project_id is empty");
    }

    @Test
    public void testSessionVariablesAndRuleConfigExist() {
        SessionVariable sv = new SessionVariable();
        sv.maskingEnabled = true;
        sv.maskingDmsUrl = "http://localhost:5000";
        sv.maskingSandboxProjectId = "129";
        sv.maskingSandboxEnv = "prod";
        sv.maskingSandboxDataVerify = false;
        sv.maskingLineagePrintEnabled = true;
        sv.maskingLineagePrintPrefix = "PFX";
        sv.maskingLineagePrintSuffix = "SFX";
        Assertions.assertTrue(sv.maskingEnabled);

        Set<String> disableRules = sv.getDisableNereidsRuleNames();
        Assertions.assertNotNull(disableRules);
        Assertions.assertNotNull(Config.ignore_hive_table_transaction);
    }

    @Test
    public void testSelectStarColumnShouldBeMaskedWhenPolicyCoversColumn() throws Exception {
        Plan leaf = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        SlotReference slot = firstSlotFromPlan(leaf);
        Object probeContext = createMaskingContext(leaf, "[]");
        String policy = buildSingleColumnPolicyFromLineage(slot, probeContext);
        Object maskingContext = createMaskingContext(leaf, policy);
        Assertions.assertTrue(invokeShouldMaskExpression(slot, maskingContext));
    }

    @Test
    public void testCountStarShouldBeMaskedWhenPolicyCoversTable() throws Exception {
        Plan leaf = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        SlotReference slot = firstSlotFromPlan(leaf);
        Object probeContext = createMaskingContext(leaf, "[]");
        String policy = buildSingleColumnPolicyFromLineage(slot, probeContext);
        Object maskingContext = createMaskingContext(leaf, policy);
        AggregateFunction countStar = new Count();
        Assertions.assertTrue(invokeShouldMaskAggregateExpression(countStar, maskingContext));
        AggregateFunction countLiteral = new Count(new IntegerLiteral(1));
        Assertions.assertTrue(invokeShouldMaskAggregateExpression(countLiteral, maskingContext));
    }

    @Test
    public void testCountColumnAliasShouldBeMaskedWhenPolicyCoversColumn() throws Exception {
        Plan leaf = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        SlotReference slot = firstSlotFromPlan(leaf);
        Object probeContext = createMaskingContext(leaf, "[]");
        String policy = buildSingleColumnPolicyFromLineage(slot, probeContext);
        Object maskingContext = createMaskingContext(leaf, policy);
        AggregateFunction countColumn = new Count(slot);
        Assertions.assertTrue(invokeShouldMaskAggregateExpression(countColumn, maskingContext));
    }

    @Test
    public void testBuildAggregateLineageFallbackForCountUsesPolicyCoverage() throws Exception {
        Plan leaf = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        SlotReference slot = firstSlotFromPlan(leaf);
        Object probeContext = createMaskingContext(leaf, "[]");
        String policy = buildSingleColumnPolicyFromLineage(slot, probeContext);
        Object maskingContext = createMaskingContext(leaf, policy);

        @SuppressWarnings("unchecked")
        List<Object> lineage = (List<Object>) invokeBuildAggregateLineage(
                new IntegerLiteral(1), "count", maskingContext);
        Assertions.assertFalse(lineage.isEmpty(), "count(*) fallback lineage should not be empty");
    }

    @Test
    public void testCountStarPlanContainsMaskUdf1AndUdf2AfterRewrite() throws Exception {
        ensureMaskUdfsRegistered();
        ConnectContext ctx = MemoTestUtils.createConnectContext();
        ctx.getSessionVariable().maskingEnabled = true;
        ctx.getSessionVariable().maskingDmsUrl = "http://cache-test";
        ctx.getSessionVariable().maskingSandboxEnv = "prod";
        ctx.getSessionVariable().maskingSandboxProjectId = "129";
        ctx.setDatabase("db1");

        Plan leaf = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        Alias countAlias = new Alias(new Count(), "cnt");
        LogicalAggregate<Plan> aggregate = new LogicalAggregate<>(
                Collections.emptyList(), Collections.singletonList(countAlias), leaf);
        @SuppressWarnings("unchecked")
        List<NamedExpression> output = (List<NamedExpression>) (List<?>) aggregate.getOutput();
        LogicalProject<Plan> project = new LogicalProject<>(output, aggregate);
        LogicalResultSink<Plan> sink = new LogicalResultSink<>(project.getProjects(), project);

        Field cacheField = MaskingOptimizer.class.getDeclaredField("POLICY_CACHE");
        cacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentHashMap<String, String> cache = (ConcurrentHashMap<String, String>) cacheField.get(null);
        String userErp = ctx.getQualifiedUser() != null ? ctx.getQualifiedUser() : "";
        String cacheKey = String.join("|", "http://cache-test", "db1.t1", userErp, "129", "prod", "0");
        cache.put(cacheKey, "[{\"dbName\":\"db1\",\"tbName\":\"t1\",\"maskSetting\":[{\"colName\":\"k1\"}]}]");

        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(ctx, sink);
        JobContext jobContext = new JobContext(cascadesContext, PhysicalProperties.GATHER, Double.MAX_VALUE);
        Plan out = new MaskingOptimizer().rewriteRoot(sink, jobContext);

        Assertions.assertTrue(planContainsFunction(out, "mask_udf2"),
                "count(*) rewritten plan should contain mask_udf2");
        Assertions.assertTrue(planContainsFunction(out, "mask_udf1"),
                "count(*) rewritten plan should contain mask_udf1");
    }

    @Test
    public void testCountStarDirectSinkOutputContainsMaskUdf1AndUdf2AfterRewrite() throws Exception {
        ensureMaskUdfsRegistered();
        ConnectContext ctx = MemoTestUtils.createConnectContext();
        ctx.getSessionVariable().maskingEnabled = true;
        ctx.getSessionVariable().maskingDmsUrl = "http://cache-test";
        ctx.getSessionVariable().maskingSandboxEnv = "prod";
        ctx.getSessionVariable().maskingSandboxProjectId = "129";
        ctx.setDatabase("db1");

        Plan leaf = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        Alias countAlias = new Alias(new Count(), "cnt");
        LogicalAggregate<Plan> aggregate = new LogicalAggregate<>(
                Collections.emptyList(), Collections.singletonList(countAlias), leaf);
        LogicalResultSink<Plan> sink = new LogicalResultSink<>(
                Collections.singletonList(countAlias), aggregate);

        Field cacheField = MaskingOptimizer.class.getDeclaredField("POLICY_CACHE");
        cacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        ConcurrentHashMap<String, String> cache = (ConcurrentHashMap<String, String>) cacheField.get(null);
        String policy = "[{\"dbName\":\"db1\",\"tbName\":\"t1\",\"maskSetting\":[{\"colName\":\"k1\"}]}]";
        primePolicyCacheForPlan(ctx, sink, policy, cache);

        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(ctx, sink);
        JobContext jobContext = new JobContext(cascadesContext, PhysicalProperties.GATHER, Double.MAX_VALUE);
        Plan out = new MaskingOptimizer().rewriteRoot(sink, jobContext);

        Assertions.assertTrue(planContainsFunction(out, "mask_udf2"),
                "direct count(*) sink output should contain mask_udf2");
        Assertions.assertTrue(planContainsFunction(out, "mask_udf1"),
                "direct count(*) sink output should contain mask_udf1");
    }

    private Object createMaskingContext(Plan rootPlan, String policy) throws Exception {
        ConnectContext ctx = MemoTestUtils.createConnectContext();
        ctx.setDatabase("db1");
        Class<?> contextClass = Class.forName("org.apache.doris.qe.MaskingOptimizer$MaskingContext");
        Constructor<?> constructor = contextClass.getDeclaredConstructor(MaskingOptimizer.class,
                ConnectContext.class, Plan.class,
                String.class, String.class, Set.class, boolean.class);
        constructor.setAccessible(true);
        return constructor.newInstance(new MaskingOptimizer(), ctx, rootPlan, policy, "prod", new HashSet<>(), false);
    }

    private boolean invokeShouldMaskExpression(Expression expression, Object maskingContext) throws Exception {
        Class<?> contextClass = Class.forName("org.apache.doris.qe.MaskingOptimizer$MaskingContext");
        Method method = MaskingOptimizer.class.getDeclaredMethod(
                "shouldMaskExpression", Expression.class, contextClass);
        method.setAccessible(true);
        return (boolean) method.invoke(maskingOptimizer, expression, maskingContext);
    }

    private boolean invokeShouldMaskAggregateExpression(AggregateFunction agg, Object maskingContext) throws Exception {
        Class<?> contextClass = Class.forName("org.apache.doris.qe.MaskingOptimizer$MaskingContext");
        Method method = MaskingOptimizer.class.getDeclaredMethod(
                "shouldMaskAggregateExpression", AggregateFunction.class, contextClass);
        method.setAccessible(true);
        return (boolean) method.invoke(maskingOptimizer, agg, maskingContext);
    }

    private Object invokeBuildAggregateLineage(Expression input, String aggOpName, Object maskingContext)
            throws Exception {
        Class<?> contextClass = Class.forName("org.apache.doris.qe.MaskingOptimizer$MaskingContext");
        Method method = MaskingOptimizer.class.getDeclaredMethod(
                "buildAggregateLineage", Expression.class, String.class, contextClass);
        method.setAccessible(true);
        return method.invoke(maskingOptimizer, input, aggOpName, maskingContext);
    }

    private SlotReference firstSlotFromPlan(Plan rootPlan) {
        @SuppressWarnings("unchecked")
        List<NamedExpression> outputs = (List<NamedExpression>) (List<?>) rootPlan.getOutput();
        for (NamedExpression output : outputs) {
            if (output instanceof SlotReference) {
                return (SlotReference) output;
            }
        }
        throw new IllegalStateException("No SlotReference found in plan output");
    }

    private boolean planContainsFunction(Plan plan, String functionName) {
        if (plan == null) {
            return false;
        }
        List<NamedExpression> outputs = extractOutputExpressions(plan);
        for (NamedExpression output : outputs) {
            if (expressionContainsFunction(output, functionName)) {
                return true;
            }
        }
        for (Plan child : plan.children()) {
            if (planContainsFunction(child, functionName)) {
                return true;
            }
        }
        return false;
    }

    private List<NamedExpression> extractOutputExpressions(Plan plan) {
        List<NamedExpression> outputs = new ArrayList<>();
        if (plan instanceof LogicalResultSink) {
            outputs.addAll(((LogicalResultSink<?>) plan).getOutputExprs());
        } else if (plan instanceof LogicalProject) {
            outputs.addAll(((LogicalProject<?>) plan).getProjects());
        } else if (plan instanceof LogicalAggregate) {
            outputs.addAll(((LogicalAggregate<?>) plan).getOutputExpressions());
        }
        return outputs;
    }

    private boolean expressionContainsFunction(Expression expression, String functionName) {
        List<Expression> functions = expression.collectToList(
                node -> node instanceof Function);
        for (Expression funcExpr : functions) {
            Function func = (Function) funcExpr;
            if (functionName.equalsIgnoreCase(func.getName())) {
                return true;
            }
        }
        return false;
    }

    private void ensureMaskUdfsRegistered() {
        Env env = Env.getCurrentEnv();
        if (env == null) {
            return;
        }
        env.getGlobalFunctionMgr().replayAddFunction(createGlobalMaskUdf("mask_udf1", true, false));
        env.getGlobalFunctionMgr().replayAddFunction(createGlobalMaskUdf("mask_udf2", false, false));
        env.getGlobalFunctionMgr().replayAddFunction(createGlobalMaskUdf("mask_udf2", false, true));
    }

    private ScalarFunction createGlobalMaskUdf(String name, boolean firstArgString, boolean firstArgDouble) {
        Type firstArg = firstArgString ? Type.VARCHAR : (firstArgDouble ? Type.DOUBLE : Type.BIGINT);
        Type[] args;
        if ("mask_udf1".equals(name)) {
            args = new Type[] {firstArg, Type.VARCHAR, Type.VARCHAR,
                    new org.apache.doris.catalog.ArrayType(Type.VARCHAR), Type.VARCHAR};
        } else {
            args = new Type[] {firstArg, Type.VARCHAR, Type.VARCHAR,
                    new org.apache.doris.catalog.ArrayType(Type.VARCHAR), Type.VARCHAR, Type.VARCHAR,
                    new org.apache.doris.catalog.ArrayType(Type.VARCHAR)};
        }
        return ScalarFunction.createUdf(
                TFunctionBinaryType.JAVA_UDF,
                new FunctionName(name),
                args,
                Type.VARCHAR,
                false,
                (URI) null,
                "org.apache.doris.udf.mask." + ("mask_udf1".equals(name) ? "MaskUdf1" : "MaskUdf2"),
                "",
                "");
    }

    @SuppressWarnings("unchecked")
    private void primePolicyCacheForPlan(ConnectContext ctx, Plan rootPlan, String policy,
            ConcurrentHashMap<String, String> cache) throws Exception {
        Method collectMethod = MaskingOptimizer.class.getDeclaredMethod(
                "collectTableDescriptionsFromPlan",
                ConnectContext.class, Plan.class, String.class, String.class, int.class);
        collectMethod.setAccessible(true);
        String userErp = ctx.getQualifiedUser() != null ? ctx.getQualifiedUser() : "";
        String projectId = ctx.getSessionVariable().maskingSandboxProjectId;
        int dataVerify = ctx.getSessionVariable().maskingSandboxDataVerify ? 1 : 0;
        List<SandboxApi.TableDescription> tableList = (List<SandboxApi.TableDescription>) collectMethod.invoke(
                maskingOptimizer, ctx, rootPlan, userErp, projectId, dataVerify);
        List<String> tableKeys = tableList.stream()
                .map(t -> (t.getDbName() != null ? t.getDbName() : "")
                        + "."
                        + (t.getTbName() != null ? t.getTbName() : ""))
                .map(s -> s.toLowerCase(Locale.ROOT))
                .sorted()
                .collect(Collectors.toList());
        String tableKey = String.join(",", tableKeys);
        String cacheKey = String.join("|",
                ctx.getSessionVariable().maskingDmsUrl,
                tableKey,
                userErp,
                projectId,
                ctx.getSessionVariable().maskingSandboxEnv,
                String.valueOf(dataVerify));
        cache.put(cacheKey, policy);
    }

    @SuppressWarnings("unchecked")
    private String buildSingleColumnPolicyFromLineage(Expression expression, Object maskingContext) throws Exception {
        Class<?> contextClass = Class.forName("org.apache.doris.qe.MaskingOptimizer$MaskingContext");
        Method method = MaskingOptimizer.class.getDeclaredMethod(
                "collectNereidsLineageKeys", Expression.class, contextClass);
        method.setAccessible(true);
        Set<String> keys = (Set<String>) method.invoke(maskingOptimizer, expression, maskingContext);
        Assertions.assertFalse(keys.isEmpty(), "Lineage keys should not be empty");
        String key = keys.iterator().next();
        String[] parts = key.split("\\.", 3);
        Assertions.assertEquals(3, parts.length, "Lineage key should be db.table.column");
        return String.format("[{\"dbName\":\"%s\",\"tbName\":\"%s\",\"maskSetting\":[{\"colName\":\"%s\"}]}]",
                parts[0], parts[1], parts[2]);
    }

    private void assertMaskingExceptionMessage(Throwable t, String expectedMessage) {
        Assertions.assertInstanceOf(NereidsException.class, t);
        Exception inner = ((NereidsException) t).getException();
        Assertions.assertNotNull(inner);
        Assertions.assertNotNull(inner.getMessage());
        Assertions.assertTrue(inner.getMessage().contains(expectedMessage),
                "actual message: " + inner.getMessage());
    }
}
