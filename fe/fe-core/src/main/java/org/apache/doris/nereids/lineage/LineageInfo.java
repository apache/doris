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
 * */
public class LineageInfo {

    /*
     * Example SQL used below:
     * INSERT INTO tgt_region_revenue
     * SELECT n.n_name AS nation_name,
     *        SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
     * FROM customer c
     * JOIN orders o ON c.c_custkey = o.o_custkey
     * JOIN lineitem l ON o.o_orderkey = l.l_orderkey
     * JOIN nation n ON c.c_nationkey = n.n_nationkey
     * JOIN region r ON n.n_regionkey = r.r_regionkey
     * WHERE r.r_name = 'ASIA'
     * GROUP BY n.n_name;
     */

    // the key is the output slot, the value is the shuttled expression which output slot depend directly
    // this is dependent on the ExpressionLineageReplacer
    // Example: nation_name -> {IDENTITY: n.n_name}, revenue -> {AGGREGATION: SUM(l.l_extendedprice * (1-l.l_discount))}
    private Map<SlotReference, SetMultimap<DirectLineageType, Expression>> directLineageMap;
    // inDirectLineageMap stores expressions that indirectly affect output slots. These expressions,
    // which indirectly impact output slots, are categorized as IndirectLineageType.
    // Example: nation_name -> {GROUP_BY: n.n_name} (intersects direct slots), revenue may have no per-output indirects.
    private Map<SlotReference, SetMultimap<IndirectLineageType, Expression>> inDirectLineageMap;
    // datasetIndirectLineageMap stores expressions that affect the whole dataset when some outputs do not directly
    // depend on the referenced slots (e.g. filter or join on non-selected columns).
    // Example: FILTER {r.r_name = 'ASIA'}, JOIN {o.o_orderkey = l.l_orderkey}.
    private Multimap<IndirectLineageType, Expression> datasetIndirectLineageMap;
    // tableLineageSet stores tables that the plan depends on
    // Example: {customer, orders, lineitem, nation, region}.
    private Set<TableIf> tableLineageSet;
    // target table for this lineage event
    // Example: tgt_region_revenue.
    private TableIf targetTable;
    // target columns for this lineage event
    // Example: [nation_name, revenue].
    private List<Slot> targetColumns;
    // query metadata
    // Example: {sourceCommand=InsertIntoTableCommand, queryId=..., database=lineage_tpch}.
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

    /**
     * Get merged indirect lineage info for each output slot.
     *
     * <p>Examples:
     * <ul>
     *   <li>If dataset-level FILTER has {l_shipdate >= '1995-01-01'} and
     *       output slot price has GROUP_BY {l_orderkey}, the merged result for price
     *       contains both FILTER and GROUP_BY expressions.</li>
     *   <li>If dataset-level SORT has {l_shipdate} and output slot orderkey has no
     *       per-output indirect lineage, the merged result for orderkey still contains SORT.</li>
     * </ul>
     */
    public Map<SlotReference, SetMultimap<IndirectLineageType, Expression>> getInDirectLineageMap() {
        if (datasetIndirectLineageMap.isEmpty()) {
            return inDirectLineageMap;
        }
        Map<SlotReference, SetMultimap<IndirectLineageType, Expression>> merged = new HashMap<>();
        Set<SlotReference> outputSlots = new HashSet<>();
        outputSlots.addAll(directLineageMap.keySet());
        outputSlots.addAll(inDirectLineageMap.keySet());
        for (SlotReference outputSlot : outputSlots) {
            SetMultimap<IndirectLineageType, Expression> combined = HashMultimap.create();
            combined.putAll(datasetIndirectLineageMap);
            SetMultimap<IndirectLineageType, Expression> perOutput = inDirectLineageMap.get(outputSlot);
            if (perOutput != null) {
                combined.putAll(perOutput);
            }
            merged.put(outputSlot, combined);
        }
        return merged;
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

    public void setTableLineageSet(Set<TableIf> tableLineageSet) {
        this.tableLineageSet = tableLineageSet;
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
     * Add indirect lineage for an output slot
     */
    public void addIndirectLineage(SlotReference outputSlot, IndirectLineageType type, Expression expr) {
        inDirectLineageMap.computeIfAbsent(outputSlot, k -> HashMultimap.create()).put(type, expr);
    }

    /**
     * Add indirect lineage for all output slots.
     * Stored as dataset-level indirect lineage to avoid duplication.
     */
    public void addDatasetIndirectLineage(IndirectLineageType type, Expression expr) {
        datasetIndirectLineageMap.put(type, expr);
    }

    /**
     * Add indirect lineage for all output slots.
     * Stored as dataset-level indirect lineage to avoid duplication.
     */
    public void addDatasetIndirectLineage(IndirectLineageType type, Set<Expression> exprs) {
        for (Expression expr : exprs) {
            addDatasetIndirectLineage(type, expr);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LineageInfo{\n");
        sb.append("  context=").append(context).append(",\n");
        sb.append("  tableLineageSet=").append(tableLineageSet).append(",\n");
        sb.append("  directLineageMap=").append(directLineageMap).append(",\n");
        sb.append("  inDirectLineageMap=").append(inDirectLineageMap).append(",\n");
        sb.append("  targetTable=").append(targetTable != null ? targetTable.getName() : "null").append("\n");
        sb.append("}");
        return sb.toString();
    }
}
