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

package org.apache.doris.horn;

import org.apache.doris.common.UserException;
import org.apache.doris.horn.doris2horn.HornLogicalPlanThriftBuilder;
import org.apache.doris.horn.horn2doris.DorisPhysicalPlanBuilder;
import org.apache.horn4j.thrift.TFlattenedExpression;
import org.apache.horn4j.thrift.THornOptimizeProfile;
import org.apache.horn4j.thrift.THornOptimizeResult;
import org.apache.horn4j.HornNative;
import org.apache.doris.metric.HornMetric;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;

/** Doris-side entry to the Horn CBO optimizer */
public class HornOptimizer {

    private static final Logger LOG = LogManager.getLogger(HornOptimizer.class);

    /** Horn 优化失败的分类，对齐 Horn 三段式流水线（正向翻译 → Horn 优化 → 反向翻译） */
    public enum HornFailureKind {
        // A: Doris→Horn 正向翻译失败 → 计 translate_plan_error（Doris 集成翻译层缺口）
        HORN_FORWARD_ERROR,
        // B: Horn 声明不处理（HornNotHandleError/External） → 计 fallback（对齐 kernel countHornFallBack）
        HORN_NOT_HANDLE,
        // C: Horn 优化器内部错误（HornOptimizerError） → 计 error（对齐 kernel countHornError）
        HORN_OPTIMIZE_ERROR,
        // D: Horn→Doris 反向翻译失败 → 计 translate_plan_error（Doris 集成翻译层缺口）
        HORN_BACKWARD_ERROR,
        // 兜底：native 抛异常但分类前缀识别不出（JNI 胶水错 / 未知 std 异常 / 契约漂移） → 计 error
        UNKNOWN
    }

    private final CascadesContext cascadesContext;
    private String hornExplainString;
    private String fallbackReason;
    // 仅在 optimize() 失败（返回 null）时赋值并被 NereidsPlanner 读取；成功路径不读它。
    private HornFailureKind failureKind;

    public HornOptimizer(CascadesContext cascadesContext) {
        this.cascadesContext = cascadesContext;
    }

    public String getHornExplainString() {
        return hornExplainString;
    }

    public String getFallbackReason() {
        return fallbackReason;
    }

    public HornFailureKind getFailureKind() {
        return failureKind;
    }

    public PhysicalPlan optimize(LogicalPlan logicalPlan) throws UserException {
        HornOptimizationContext hornCtx = new HornOptimizationContext(cascadesContext);

        // 阶段 A：Doris LogicalPlan → TFlattenedExpression → 序列化。
        byte[] planBytes;
        byte[] tableBatchBytes;
        byte[] queryOptionBytes;
        try {
            LOG.info("Horn CBO: translating Doris plan to Horn input");
            HornLogicalPlanThriftBuilder planVisitor = new HornLogicalPlanThriftBuilder(hornCtx);
            TFlattenedExpression plan = planVisitor.translate(logicalPlan);

            TSerializer serializer = new TSerializer();
            planBytes = serializer.serialize(plan);
            tableBatchBytes = serializer.serialize(hornCtx.getTableBatch());
            queryOptionBytes = serializer.serialize(hornCtx.getQueryOptionConfig());
        } catch (Exception e) {
            failureKind = HornFailureKind.HORN_FORWARD_ERROR;
            fallbackReason = e.getMessage();
            LOG.info("Horn CBO: forward-translation failed, fallback: {}", fallbackReason);
            return null;
        }

        // 阶段 B/C：JNI 调 Horn native（in-process horn4j）。
        byte[] resultBytes;
        try {
            LOG.info("Horn CBO: calling Horn via horn4j JNI");
            resultBytes = new HornNative().optimize(planBytes, tableBatchBytes, queryOptionBytes);
        } catch (Exception e) {
            // 依据 kernel HornError 的分类前缀（既定契约：horn_error.h GetErrorTypeString 把类型拼进 message）
            String msg = e.getMessage();
            if (msg != null && msg.contains("HornNotHandle")) {
                failureKind = HornFailureKind.HORN_NOT_HANDLE;
            } else if (msg != null && msg.contains("HornOptimizerError")) {
                failureKind = HornFailureKind.HORN_OPTIMIZE_ERROR;
            } else {
                failureKind = HornFailureKind.UNKNOWN;
            }
            fallbackReason = msg;
            LOG.warn("Horn CBO: native failed ({}), fallback: {}", failureKind, fallbackReason);
            return null;
        }

        // 阶段 D：反序列化 + 消费 profile + Horn output → Doris PhysicalPlan。
        try {
            LOG.info("Horn CBO: rebuilding Doris plan from Horn output");
            THornOptimizeResult hornResult = new THornOptimizeResult();
            TDeserializer deserializer = new TDeserializer();
            deserializer.deserialize(hornResult, resultBytes);

            hornExplainString = hornResult.getExplain_string();
            if (hornExplainString != null) {
                LOG.info("Horn CBO explain:\n{}", hornExplainString);
            }

            consumeProfile(hornResult);

            DorisPhysicalPlanBuilder builder = new DorisPhysicalPlanBuilder(hornCtx);
            PhysicalPlan physicalPlan = builder.build(hornResult.getTfexpr());

            LOG.info("Horn CBO: optimization complete");
            return physicalPlan;
        } catch (Exception e) {
            failureKind = HornFailureKind.HORN_BACKWARD_ERROR;
            // 反译失败时 horn C++ 已产出计划文本，getHornExplainString() 已被上面赋值，
            fallbackReason = e.getMessage();
            LOG.warn("Horn CBO: backward-translation failed, fallback: {}", fallbackReason, e);
            return null;
        }
    }

    /** 消费 Horn profile → 更新 FE 量表指标（profile 无条件产出，无需任何 session 开关） */
    private static void consumeProfile(THornOptimizeResult hornResult) {
        if (hornResult.isSetProfile() && MetricRepo.isInit) {
            THornOptimizeProfile p = hornResult.getProfile();
            HornMetric.GAUGE_HORN_OPTIMIZE_LATENCY_NS.setValue(p.getHorn_optimize_latency());          // #6
            HornMetric.GAUGE_HORN_CASCADE_GROUP_COUNT.setValue(p.getCascade_group_count());            // #7
            HornMetric.GAUGE_HORN_CASCADE_GROUP_EXPR_COUNT.setValue(
                    p.getCascade_group_expression_count());                                            // #8
            HornMetric.GAUGE_HORN_CASCADE_SAFE_TO_PRUNE_RATIO.setValue(
                    p.getCascade_safe_to_prune_ratio());                                               // #9
            HornMetric.GAUGE_HORN_SCHEDULER_JOB_COUNT.setValue(p.getScheduler_job_count());            // #10
            HornMetric.GAUGE_HORN_DPHYPER_CSG_CMP_COUNT.setValue(p.getDphyper_csg_cmp_count());        // #11
            HornMetric.GAUGE_HORN_DPHYPER_ENUM_SUCCESS_RATIO.setValue(
                    p.getDphyper_enumeration_successful_ratio());                                      // #12
        }
    }

}
