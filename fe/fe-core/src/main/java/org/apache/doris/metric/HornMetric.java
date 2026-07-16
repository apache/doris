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

package org.apache.doris.metric;

import org.apache.doris.metric.Metric.MetricUnit;

/**
 * Horn CBO 优化器监控指标（对齐 kernel 的 HornMetrics.java）。
 * 独立成类、统一 HORN_PREFIX 前缀，注册到全局 MetricRepo.DORIS_METRIC_REGISTER。
 * init() 由 MetricRepo.init() 调用一次。
 */
public class HornMetric {
    // Horn 指标统一业务前缀（框架还会自动再加 doris_fe_）。
    // 改前缀只改这一处，例如想要 horn_cbo_ 就把值换成 "horn_cbo_"。
    private static final String HORN_PREFIX = "horn_";

    // 计数器（累计）
    public static LongCounterMetric COUNTER_HORN_QUERY;           // doris_fe_horn_query_total
    public static LongCounterMetric COUNTER_HORN_QUERY_SUPPORT;   // ..._query_support_total
    public static LongCounterMetric COUNTER_HORN_QUERY_SUCCESS;   // ..._query_success_total
    public static LongCounterMetric COUNTER_HORN_QUERY_ERROR;     // ..._query_error_total
    public static LongCounterMetric COUNTER_HORN_QUERY_FALLBACK;  // ..._query_fallback_total（算 ratio 用）
    // Doris 集成专属：forward/backward 计划翻译失败合并桶（不算 kernel 语义的 error/fallback）
    public static LongCounterMetric COUNTER_HORN_QUERY_TRANSLATE_PLAN_ERROR; // ..._query_translate_plan_error_total

    // 量表（反映最近一次查询；并发弱可见性见 porting-plan §2）
    public static GaugeMetricImpl<Double> GAUGE_HORN_FALLBACK_RATIO;
    public static GaugeMetricImpl<Long>   GAUGE_HORN_OPTIMIZE_LATENCY_NS;
    public static GaugeMetricImpl<Long>   GAUGE_HORN_CASCADE_GROUP_COUNT;
    public static GaugeMetricImpl<Long>   GAUGE_HORN_CASCADE_GROUP_EXPR_COUNT;
    public static GaugeMetricImpl<Double> GAUGE_HORN_CASCADE_SAFE_TO_PRUNE_RATIO;
    public static GaugeMetricImpl<Long>   GAUGE_HORN_SCHEDULER_JOB_COUNT;
    public static GaugeMetricImpl<Long>   GAUGE_HORN_DPHYPER_CSG_CMP_COUNT;
    public static GaugeMetricImpl<Double> GAUGE_HORN_DPHYPER_ENUM_SUCCESS_RATIO;

    /** 拼上统一前缀，指标名主体不再各自硬编码 horn_。 */
    private static String name(String suffix) {
        return HORN_PREFIX + suffix;
    }

    public static void init() {
        COUNTER_HORN_QUERY = register(new LongCounterMetric(name("query_total"),
                MetricUnit.REQUESTS, "total queries entering horn optimize path"));
        COUNTER_HORN_QUERY_SUPPORT = register(new LongCounterMetric(name("query_support_total"),
                MetricUnit.REQUESTS, "queries that attempted horn optimization"));
        COUNTER_HORN_QUERY_SUCCESS = register(new LongCounterMetric(name("query_success_total"),
                MetricUnit.REQUESTS, "queries optimized by horn successfully"));
        COUNTER_HORN_QUERY_ERROR = register(new LongCounterMetric(name("query_error_total"),
                MetricUnit.REQUESTS, "queries where horn threw an error"));
        COUNTER_HORN_QUERY_FALLBACK = register(new LongCounterMetric(name("query_fallback_total"),
                MetricUnit.REQUESTS, "queries that fell back from horn to nereids"));
        COUNTER_HORN_QUERY_TRANSLATE_PLAN_ERROR = register(new LongCounterMetric(
                name("query_translate_plan_error_total"),
                MetricUnit.REQUESTS,
                "queries where doris<->horn plan translation (forward or backward) failed"));

        // 注意：GaugeMetricImpl 构造函数是 4 参数 (name, unit, description, defaultValue)。
        GAUGE_HORN_FALLBACK_RATIO = register(new GaugeMetricImpl<>(name("fallback_ratio"),
                MetricUnit.PERCENT, "fraction of supported queries that fell back to nereids", 0.0));
        GAUGE_HORN_OPTIMIZE_LATENCY_NS = register(new GaugeMetricImpl<>(name("optimize_latency_ns"),
                MetricUnit.NANOSECONDS, "horn optimize wall time of last query (ns)", 0L));
        GAUGE_HORN_CASCADE_GROUP_COUNT = register(new GaugeMetricImpl<>(name("cascade_group_count"),
                MetricUnit.NOUNIT, "cascades memo group count of last query", 0L));
        GAUGE_HORN_CASCADE_GROUP_EXPR_COUNT = register(new GaugeMetricImpl<>(name("cascade_group_expression_count"),
                MetricUnit.NOUNIT, "cascades memo group expression count of last query", 0L));
        GAUGE_HORN_CASCADE_SAFE_TO_PRUNE_RATIO = register(new GaugeMetricImpl<>(name("cascade_safe_to_prune_ratio"),
                MetricUnit.PERCENT, "safe-to-prune ratio of last query", 0.0));
        GAUGE_HORN_SCHEDULER_JOB_COUNT = register(new GaugeMetricImpl<>(name("scheduler_job_count"),
                MetricUnit.NOUNIT, "optimizer scheduler job count of last query", 0L));
        GAUGE_HORN_DPHYPER_CSG_CMP_COUNT = register(new GaugeMetricImpl<>(name("dphyper_csg_cmp_count"),
                MetricUnit.NOUNIT, "dphyper csg-cmp emit count of last query", 0L));
        GAUGE_HORN_DPHYPER_ENUM_SUCCESS_RATIO = register(new GaugeMetricImpl<>(
                name("dphyper_enumeration_successful_ratio"),
                MetricUnit.PERCENT, "dphyper enumeration successful ratio of last query", 0.0));
    }

    /** 统一注册到全局 registry 并返回自身，便于链式赋值。 */
    private static <M extends Metric> M register(M metric) {
        MetricRepo.DORIS_METRIC_REGISTER.addMetrics(metric);
        return metric;
    }

    /** fallback ratio = fallback / support，带除零保护；由 MetricCalculator 周期调用。 */
    public static void updateFallbackRatio() {
        long support = COUNTER_HORN_QUERY_SUPPORT.getValue();
        long fallback = COUNTER_HORN_QUERY_FALLBACK.getValue();
        GAUGE_HORN_FALLBACK_RATIO.setValue(support == 0 ? 0.0 : (double) fallback / (double) support);
    }
}
