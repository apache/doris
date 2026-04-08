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

package org.apache.doris.nereids.lineage;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class describes the common in-memory lineage data format used in Doris.
 * Based on this data structure, complete lineage information and corresponding event details can be parsed.
 *
 * <p>Example SQL for the field comments below:
 * <pre>
 * INSERT INTO tgt_join_window_case
 * SELECT o.o_orderkey AS orderkey,
 *        ROW_NUMBER() OVER (PARTITION BY c.c_nationkey ORDER BY o.o_orderdate DESC) AS rn,
 *        CASE WHEN l.l_discount > 0.05 THEN 'HIGH' ELSE 'LOW' END AS price_flag
 * FROM orders o
 * JOIN customer c ON o.o_custkey = c.c_custkey
 * LEFT JOIN lineitem l ON o.o_orderkey = l.l_orderkey
 * WHERE o.o_orderdate >= DATE '1994-01-01'
 *   AND c.c_nationkey IS NOT NULL;
 * </pre>
 */
public class LineageInfo {
    // Output-slot direct dependencies based on ExpressionLineageReplacer.
    // Example: orderkey -> {IDENTITY: o.o_orderkey};
    //          rn -> {TRANSFORMATION: row_number() over (partition by c.c_nationkey order by o.o_orderdate)};
    //          price_flag -> {TRANSFORMATION: CASE WHEN l.l_discount > 0.05 THEN 'HIGH' ELSE 'LOW' END}.
    private Map<SlotReference, SetMultimap<DirectLineageType, Expression>> directLineageMap;
    // Per-output indirect lineage for WINDOW/CONDITIONAL only.
    // Example: rn -> {WINDOW: c.c_nationkey, o.o_orderdate};
    //          price_flag -> {CONDITIONAL: l.l_discount > 0.05}.
    private Map<SlotReference, SetMultimap<IndirectLineageType, Expression>> inDirectLineageMap;
    // Dataset-level indirect lineage for JOIN/FILTER/GROUP_BY/SORT.
    // Example: JOIN {o.o_custkey = c.c_custkey, o.o_orderkey = l.l_orderkey},
    //          FILTER {o.o_orderdate >= DATE '1994-01-01', c.c_nationkey IS NOT NULL}.
    private Multimap<IndirectLineageType, Expression> datasetIndirectLineageMap;
    // Tables referenced by the query.
    // Example: {orders, customer, lineitem}.
    private Set<TableIf> tableLineageSet;
    // Target table for this lineage event.
    // Example: tgt_join_window_case.
    private TableIf targetTable;
    // Target columns for this lineage event.
    // Example: [orderkey, rn, price_flag].
    private List<Slot> targetColumns;
    // Query metadata such as user, database, and query text.
    // Example: {sourceCommand=InsertIntoTableCommand, database=<current_db>, user=<session_user>}.
    private LineageContext context;

    /**
     * Indirect lineage type - expressions that indirectly affect output slots
     */
    public enum IndirectLineageType {
        // input used in join condition
        JOIN,
        // output is aggregated based on input
        GROUP_BY,
        // input used as a filtering condition
        FILTER,
        // output is sorted based on input field
        SORT,
        // output is windowed based on input field
        WINDOW,
        // input value is used in IF, CASE WHEN or COALESCE statements
        CONDITIONAL
    }

    /**
     * Direct lineage type - how output value relates to input
     */
    public enum DirectLineageType {
        // output value is taken as is from the input
        IDENTITY,
        // output value is transformed source value from input row
        TRANSFORMATION,
        // output value is aggregation of source values from multiple input rows
        AGGREGATION
    }

    public LineageInfo() {
        this.directLineageMap = new HashMap<>();
        this.inDirectLineageMap = new HashMap<>();
        this.tableLineageSet = new HashSet<>();
        this.datasetIndirectLineageMap = HashMultimap.create();
    }

    public Map<SlotReference, SetMultimap<DirectLineageType, Expression>> getDirectLineageMap() {
        return directLineageMap;
    }

    public void setDirectLineageMap(Map<SlotReference, SetMultimap<DirectLineageType, Expression>> directLineageMap) {
        this.directLineageMap = directLineageMap;
    }

    public void setDatasetIndirectLineageMap(Multimap<IndirectLineageType, Expression> datasetIndirectLineageMap) {
        this.datasetIndirectLineageMap = datasetIndirectLineageMap;
    }

    public void setTableLineageSet(Set<TableIf> tableLineageSet) {
        this.tableLineageSet = tableLineageSet;
    }

    public void setInDirectLineageMap(
            Map<SlotReference, SetMultimap<IndirectLineageType, Expression>> inDirectLineageMap) {
        this.inDirectLineageMap = inDirectLineageMap;
    }

    /**
     * Get dataset-level indirect lineage expressions.
     *
     * @return dataset-level indirect lineage map
     */
    public Multimap<IndirectLineageType, Expression> getDatasetIndirectLineageMap() {
        return datasetIndirectLineageMap;
    }

    public Set<TableIf> getTableLineageSet() {
        return tableLineageSet;
    }

    public void addTableLineage(TableIf table) {
        this.tableLineageSet.add(table);
    }

    public TableIf getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(TableIf targetTable) {
        this.targetTable = targetTable;
    }

    public List<Slot> getTargetColumns() {
        return targetColumns;
    }

    public void setTargetColumns(List<Slot> targetColumns) {
        this.targetColumns = targetColumns;
    }

    /**
     * Get lineage context metadata.
     */
    public LineageContext getContext() {
        return context;
    }

    /**
     * Set lineage context metadata.
     */
    public void setContext(LineageContext context) {
        this.context = context;
    }

    /**
     * Add direct lineage for an output slot
     */
    public void addDirectLineage(SlotReference outputSlot, DirectLineageType type, Expression expr) {
        directLineageMap.computeIfAbsent(outputSlot, k -> HashMultimap.create()).put(type, expr);
    }

    /**
     * Add per-output indirect lineage. WINDOW/CONDITIONAL are stored per-output;
     * other types are stored at dataset-level.
     */
    public void addIndirectLineage(SlotReference outputSlot, IndirectLineageType type, Expression expr) {
        if (type == IndirectLineageType.WINDOW || type == IndirectLineageType.CONDITIONAL) {
            inDirectLineageMap.computeIfAbsent(outputSlot, k -> HashMultimap.create()).put(type, expr);
        } else {
            datasetIndirectLineageMap.put(type, expr);
        }
    }

    /**
     * Get dataset-level indirect lineage info for each output slot.
     *
     * <p>Dataset-level indirect lineage is applied to every output slot. WINDOW/CONDITIONAL are not included here.
     */
    public Map<SlotReference, SetMultimap<IndirectLineageType, Expression>> getInDirectLineageMapByDataset() {
        if (datasetIndirectLineageMap.isEmpty()) {
            return new HashMap<>();
        }
        Map<SlotReference, SetMultimap<IndirectLineageType, Expression>> merged = new HashMap<>();
        Set<SlotReference> outputSlots = new HashSet<>(directLineageMap.keySet());
        for (SlotReference outputSlot : outputSlots) {
            SetMultimap<IndirectLineageType, Expression> combined = HashMultimap.create();
            combined.putAll(datasetIndirectLineageMap);
            merged.put(outputSlot, combined);
        }
        return merged;
    }

    /**
     * Get per-output indirect lineage for WINDOW/CONDITIONAL.
     */
    public Map<SlotReference, SetMultimap<IndirectLineageType, Expression>> getOutputIndirectLineageMap() {
        return inDirectLineageMap;
    }

    /**
     * Add indirect lineage for all output slots.
     * Stored as dataset-level indirect lineage to avoid duplication.
     */
    public void addDatasetIndirectLineage(IndirectLineageType type, Expression expr) {
        datasetIndirectLineageMap.put(type, expr);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LineageInfo{\n");
        sb.append("  context=").append(context).append(",\n");
        sb.append("  tableLineageSet=").append(tableLineageSet).append(",\n");
        sb.append("  directLineageMap=").append(directLineageMap).append(",\n");
        sb.append("  targetTable=").append(targetTable != null ? targetTable.getName() : "null").append("\n");
        sb.append("}");
        return sb.toString();
    }
}
