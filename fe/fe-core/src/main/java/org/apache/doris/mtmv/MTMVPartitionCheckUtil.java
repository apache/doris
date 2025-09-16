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

package org.apache.doris.mtmv;

import org.apache.doris.analysis.PartitionExprUtil;
import org.apache.doris.analysis.PartitionExprUtil.FunctionIntervalInfo;
import org.apache.doris.catalog.DynamicPartitionProperty;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.DynamicPartitionUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;

public class MTMVPartitionCheckUtil {
    /**
     * Check if the partitioning method of the table meets the requirements for multi table partitioning updates
     *
     * @param relatedTable base table of materialized view
     * @return Inspection results and reasons
     */
    public static Pair<Boolean, String> checkIfAllowMultiTablePartitionRefresh(MTMVRelatedTableIf relatedTable) {
        if (!(relatedTable instanceof OlapTable)) {
            return Pair.of(false, "only support OlapTable");
        }
        OlapTable olapTable = (OlapTable) relatedTable;
        if (olapTable.getPartitionType() != PartitionType.RANGE) {
            return Pair.of(false, "only support range partition");
        }
        boolean isDynamicOrAuto = isDynamicPartition(olapTable) || isAutoPartition(olapTable);
        if (!isDynamicOrAuto) {
            return Pair.of(false, "only support dynamic/auto partition");
        }
        return Pair.of(true, "");
    }

    /**
     * Compare whether the partitioning rules of two tables are consistent
     *
     * @param originalTable partition table of materialized view
     * @param relatedTable Partition refresh table for materialized views
     * @return Inspection results and reasons
     * @throws AnalysisException The preconditions are not met
     */
    public static Pair<Boolean, String> compareOriginalTableAndRelatedTable(OlapTable originalTable,
            OlapTable relatedTable) throws AnalysisException {
        if (isDynamicPartition(originalTable)) {
            return compareDynamicPartition(originalTable, relatedTable);
        } else if (isAutoPartition(originalTable)) {
            return compareAutoPartition(originalTable, relatedTable);
        } else {
            throw new AnalysisException("only support dynamic/auto partition");
        }
    }

    /**
     * Determine which related table partitioning rules are consistent with the original table
     *
     * @param originalTable partition table of materialized view
     * @param relatedTables Partition refresh table for materialized views
     * @return Inspection results and reasons
     * @throws AnalysisException The preconditions are not met
     */
    public static List<Pair<Boolean, String>> compareOriginalTableAndRelatedTables(OlapTable originalTable,
            List<OlapTable> relatedTables) throws AnalysisException {
        List<Pair<Boolean, String>> res = Lists.newArrayListWithCapacity(relatedTables.size());
        for (OlapTable relatedTable : relatedTables) {
            res.add(compareOriginalTableAndRelatedTable(originalTable, relatedTable));
        }
        return res;
    }

    @VisibleForTesting
    public static Pair<Boolean, String> compareDynamicPartition(OlapTable originalTable,
            OlapTable relatedTable) throws AnalysisException {
        if (!isDynamicPartition(relatedTable)) {
            return Pair.of(false, "relatedTable is not dynamic partition.");
        }
        DynamicPartitionProperty originalDynamicProperty = originalTable.getTableProperty()
                .getDynamicPartitionProperty();
        DynamicPartitionProperty relatedDynamicProperty = relatedTable.getTableProperty().getDynamicPartitionProperty();
        if (originalDynamicProperty == null || relatedDynamicProperty == null) {
            throw new AnalysisException("dynamicProperty is null");
        }
        if (originalDynamicProperty.getTimeZone() != relatedDynamicProperty.getTimeZone()) {
            return Pair.of(false, "timeZone not equal.");
        }
        if (originalDynamicProperty.getTimeUnit() != relatedDynamicProperty.getTimeUnit()) {
            return Pair.of(false, "timeUnit not equal.");
        }
        if (!originalDynamicProperty.getStartOfMonth().equals(relatedDynamicProperty.getStartOfMonth())) {
            return Pair.of(false, "startOfMonth not equal.");
        }
        if (!originalDynamicProperty.getStartOfWeek().equals(relatedDynamicProperty.getStartOfWeek())) {
            return Pair.of(false, "startOfWeek not equal.");
        }
        return Pair.of(true, "");
    }

    @VisibleForTesting
    public static Pair<Boolean, String> compareAutoPartition(OlapTable originalTable,
            OlapTable relatedTable) throws AnalysisException {
        if (!isDynamicPartition(relatedTable)) {
            return Pair.of(false, "relatedTable is not dynamic partition.");
        }
        FunctionIntervalInfo originalFunctionIntervalInfo = PartitionExprUtil.getFunctionIntervalInfo(
                originalTable.getPartitionInfo().getPartitionExprs(), originalTable.getPartitionType());
        FunctionIntervalInfo relatedFunctionIntervalInfo = PartitionExprUtil.getFunctionIntervalInfo(
                relatedTable.getPartitionInfo().getPartitionExprs(), relatedTable.getPartitionType());
        boolean equals = Objects.equals(originalFunctionIntervalInfo, relatedFunctionIntervalInfo);
        if (!equals) {
            return Pair.of(false, "functionIntervalInfo not equal.");
        }
        return Pair.of(true, "");
    }

    private static boolean isDynamicPartition(OlapTable olapTable) {
        return DynamicPartitionUtil.isDynamicPartitionTable(olapTable);
    }

    private static boolean isAutoPartition(OlapTable olapTable) {
        return olapTable.getPartitionInfo().enableAutomaticPartition();
    }
}
