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


import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.hint.LeadingHint;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;

import org.apache.horn4j.thrift.TEngineType;
import org.apache.horn4j.thrift.TExplainLevel;
import org.apache.horn4j.thrift.TQueryOptionConfig;
import org.apache.horn4j.thrift.TTable;
import org.apache.horn4j.thrift.TTableBatch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Horn optimization context shared across the input-translation, JNI, and
 * output-rebuild phases.
 */
public class HornOptimizationContext {

    private final CascadesContext cascadesContext;

    /** Table metadata + statistics. */
    private TTableBatch tableBatch = new TTableBatch();

    /** Horn optimizer configuration. */
    private TQueryOptionConfig queryOptionConfig;

    /**
     * forward 每个 LogicalOlapScan 的裁剪后分区集，按 relationId 索引（Horn 只透传 relationId，
     * 不进 thrift）。backward DorisPhysicalPlanBuilder.buildScan 用它重建 scan 分区。
     */
    private final Map<Long, List<Long>> scanPrunedPartitions = new HashMap<>();

    /**
     * backward 阶段 horn scalar_unique_id → 反译出的 Doris Slot 全局映射，保证上下游 plan 节点的
     * slot 引用一致（{@link org.apache.doris.horn.horn2doris.DorisPhysicalPlanBuilder} 写入、
     * {@link org.apache.doris.horn.horn2doris.DorisExpressionBuilder} 反译 ColumnRef 时按 uid 查）。
     */
    private final Map<Long, Slot> scalarUidToSlot = new HashMap<>();

    /**
     * backward 阶段 horn agg fn 的 scalar_unique_id → 反译出的 Doris AggregateFunction，专给两阶段
     * agg 用：local agg put、global agg 按 uid get 回同一实例（Doris 用引用关联 phase 间的同一 fn）。
     */
    private final Map<Long, AggregateFunction> scalarUidToAggFunc = new HashMap<>();

    public HornOptimizationContext(CascadesContext cascadesContext) {
        this.cascadesContext = cascadesContext;
        this.queryOptionConfig = buildDefaultQueryOptionConfig();
        applyLeadingHint(this.queryOptionConfig);
    }

    /**
     * 把 SQL 的 {@code LEADING(...)} hint 转发给 horn，让内部 {@code XformLeadingJoin} 按指定顺序重排 join。
     */
    private void applyLeadingHint(TQueryOptionConfig config) {
        if (!cascadesContext.isLeadingJoin()) {
            return;
        }
        LeadingHint leading = (LeadingHint) cascadesContext.getHintMap().get("Leading");
        if (leading != null && !leading.getTablelist().isEmpty()) {

            config.setJoin_leading_string(new ArrayList<>(leading.getTablelist()));
        }
    }

    public CascadesContext getCascadesContext() {
        return cascadesContext;
    }

    public TTableBatch getTableBatch() {
        return tableBatch;
    }

    public void setTableBatch(TTableBatch tableBatch) {
        this.tableBatch = tableBatch;
    }

    public void putScanPrunedPartitions(long relationId, List<Long> prunedPartitionIds) {
        scanPrunedPartitions.put(relationId, prunedPartitionIds);
    }

    public List<Long> getScanPrunedPartitions(long relationId) {
        return scanPrunedPartitions.get(relationId);
    }

    public void addTable(TTable ttable) {
        if (tableBatch.getTables() == null) {
            tableBatch.setTables(new ArrayList<>());
        }
        tableBatch.getTables().add(ttable);
    }


    public Map<Long, Slot> getScalarUidToSlot() {
        return scalarUidToSlot;
    }

    public Map<Long, AggregateFunction> getScalarUidToAggFunc() {
        return scalarUidToAggFunc;
    }

    public TQueryOptionConfig getQueryOptionConfig() {
        return queryOptionConfig;
    }

    public TExplainLevel getExplainLevel() {
        return TExplainLevel.EXTRACTED_PLAN;
    }

    private TQueryOptionConfig buildDefaultQueryOptionConfig() {
        TQueryOptionConfig config = new TQueryOptionConfig();
        config.setHorn_dphyper_enable(true);
        config.setHorn_statistics_default_selectivity(0.4);
        config.setHorn_statistics_damping_factor_filter(0.75);
        config.setHorn_statistics_damping_factor_groupby(0.75);
        config.setHorn_statistics_damping_factor_join(0.1);
        // 默认允许无统计的表也走 horn（用 default selectivity 估算）——否则缺 stats 的表
        // 会大量 fallback Nereids。严格 stats 后续由 session var 控制。
        config.setEnable_horn_no_statistics_support(true);
        config.setLocal_time_zone(cascadesContext.getConnectContext()
                .getSessionVariable().getTimeZone());
        config.setMt_dop(1);
        config.setEngine_type(TEngineType.kDoris);
        boolean detailInfo = cascadesContext.getConnectContext()
                .getSessionVariable().enableHornDetailInfo;
        config.setEnable_horn_cascade_log(detailInfo);
        config.setEnable_horn_cascade_detail_log(detailInfo);
        config.setEnable_horn_stats_log(detailInfo);
        config.setHorn_print_memo_after_optimization(detailInfo);  // detail 模式下 dump memo
        config.setEnable_horn_profile(cascadesContext.getConnectContext()
                .getSessionVariable().enableHornProfile);
        config.setExplain_level(getExplainLevel());
        return config;
    }
}
