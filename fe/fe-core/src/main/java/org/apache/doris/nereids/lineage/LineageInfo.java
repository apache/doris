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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.HashMultimap;
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

    // the key is the output slot, the value is the shuttled expression which output slot depend directly
    // this is dependent on the ExpressionUtils.shuttleExpressionWithLineage
    private Map<SlotReference, SetMultimap<DirectLineageType, Expression>> directLineageMap;
    // inDirectLineageMap stores expressions that indirectly affect output slots. These expressions,
    // which indirectly impact output slots, are categorized as IndirectLineageType.
    private Map<SlotReference, SetMultimap<IndirectLineageType, Expression>> inDirectLineageMap;
    // tableLineageSet stores tables that the plan depends on
    private Set<TableIf> tableLineageSet;
    // sourceCommand indicates the type of data manipulation operation that generated this lineage information
    // such as InsertIntoCommand, InsertOverwriteCommand, etc.
    private Class<? extends Command> sourceCommand;

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
    }

    public Map<SlotReference, SetMultimap<DirectLineageType, Expression>> getDirectLineageMap() {
        return directLineageMap;
    }

    public void setDirectLineageMap(Map<SlotReference, SetMultimap<DirectLineageType, Expression>> directLineageMap) {
        this.directLineageMap = directLineageMap;
    }

    public Map<SlotReference, SetMultimap<IndirectLineageType, Expression>> getInDirectLineageMap() {
        return inDirectLineageMap;
    }

    public void setInDirectLineageMap(
            Map<SlotReference, SetMultimap<IndirectLineageType, Expression>> inDirectLineageMap) {
        this.inDirectLineageMap = inDirectLineageMap;
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

    public Class<? extends Command> getSourceCommand() {
        return sourceCommand;
    }

    public void setSourceCommand(Class<? extends Command> sourceCommand) {
        this.sourceCommand = sourceCommand;
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
     * Add indirect lineage for all output slots
     */
    public void addIndirectLineageForAll(IndirectLineageType type, Expression expr) {
        for (SlotReference outputSlot : directLineageMap.keySet()) {
            addIndirectLineage(outputSlot, type, expr);
        }
    }

    /**
     * Add indirect lineage for all output slots
     */
    public void addIndirectLineageForAll(IndirectLineageType type, Set<Expression> exprs) {
        for (Expression expr : exprs) {
            addIndirectLineageForAll(type, expr);
        }
    }

    public static Map<SlotReference, Expression> generateLineageMap(Plan plan) {
        List<Slot> output = plan.getOutput();
        Map<SlotReference, Expression> lineageMap = new HashMap<>();
        List<? extends Expression> expressions = ExpressionUtils.shuttleExpressionWithLineage(output, plan);
        for (int i = 0; i < output.size(); i++) {
            lineageMap.put((SlotReference) output.get(i), expressions.get(i));
        }
        return lineageMap;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LineageInfo{\n");
        sb.append("  sourceCommand=").append(sourceCommand != null ? sourceCommand.getSimpleName() : "null")
                .append(",\n");
        sb.append("  tableLineageSet=").append(tableLineageSet).append(",\n");
        sb.append("  directLineageMap=").append(directLineageMap).append(",\n");
        sb.append("  inDirectLineageMap=").append(inDirectLineageMap).append("\n");
        sb.append("}");
        return sb.toString();
    }
}
