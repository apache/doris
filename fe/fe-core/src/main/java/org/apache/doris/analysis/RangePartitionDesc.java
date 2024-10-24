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

import org.apache.doris.analysis.PartitionKeyDesc.PartitionKeyValueType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// to describe the key range partition's information in create table stmt
public class RangePartitionDesc extends PartitionDesc {

    public RangePartitionDesc(List<String> partitionColNames,
                              List<AllPartitionDesc> allPartitionDescs) throws AnalysisException {
        super(partitionColNames, allPartitionDescs);
        type = org.apache.doris.catalog.PartitionType.RANGE;
        this.partitionExprs = new ArrayList<>(partitionColNames.stream()
            .map(col -> new SlotRef(null, col))
            .collect(Collectors.toList()));
        this.isAutoCreatePartitions = false;
    }

    public RangePartitionDesc(ArrayList<Expr> exprs, List<String> partitionColNames,
            List<AllPartitionDesc> allPartitionDescs) throws AnalysisException {
        this.partitionExprs = exprs;
        this.partitionColNames = partitionColNames;
        this.singlePartitionDescs = handleAllPartitionDesc(allPartitionDescs);
        this.type = org.apache.doris.catalog.PartitionType.RANGE;
        this.isAutoCreatePartitions = true;
    }

    // for parse auto partition
    public static RangePartitionDesc createRangePartitionDesc(ArrayList<Expr> exprs,
            List<AllPartitionDesc> allPartitionDescs) throws AnalysisException {
        List<String> colNames = getColNamesFromExpr(exprs, false, true);
        return new RangePartitionDesc(exprs, colNames, allPartitionDescs);
    }

    @Override
    public void checkPartitionKeyValueType(PartitionKeyDesc partitionKeyDesc) throws AnalysisException {
        if (partitionKeyDesc.getPartitionType() != PartitionKeyValueType.FIXED
                && partitionKeyDesc.getPartitionType() != PartitionKeyValueType.LESS_THAN) {
            throw new AnalysisException("You can only use fixed or less than values to create range partitions");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("PARTITION BY RANGE(");
        int idx = 0;
        for (String column : partitionColNames) {
            if (idx != 0) {
                sb.append(", ");
            }
            sb.append("`").append(column).append("`");
            idx++;
        }
        sb.append(")\n(\n");

        for (int i = 0; i < singlePartitionDescs.size(); i++) {
            if (i != 0) {
                sb.append(",\n");
            }
            sb.append(singlePartitionDescs.get(i).toSql());
        }
        sb.append("\n)");
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public PartitionInfo toPartitionInfo(List<Column> schema, Map<String, Long> partitionNameToId, boolean isTemp)
            throws DdlException {
        List<Column> partitionColumns = new ArrayList<>();

        // check and get partition column
        for (String colName : partitionColNames) {
            boolean find = false;
            for (Column column : schema) {
                if (column.getName().equalsIgnoreCase(colName)) {
                    if (column.getType().isComplexType()) {
                        throw new DdlException("Complex type column can't be partition column: "
                                + column.getType().toString());
                    }
                    try {
                        RangePartitionInfo.checkPartitionColumn(column);
                    } catch (AnalysisException e) {
                        throw new DdlException(e.getMessage());
                    }

                    partitionColumns.add(column);
                    find = true;
                    break;
                }
            }
            if (!find) {
                throw new DdlException("Partition column[" + colName + "] does not found");
            }
        }

        /*
         * validate key range
         * eg.
         * VALUE LESS THAN (10, 100, 1000)
         * VALUE LESS THAN (50, 500)
         * VALUE LESS THAN (80)
         *
         * key range is:
         * ( {MIN, MIN, MIN},     {10,  100, 1000} )
         * [ {10,  100, 1000},    {50,  500, MIN } )
         * [ {50,  500, MIN },    {80,  MIN, MIN } )
         */
        RangePartitionInfo rangePartitionInfo = new RangePartitionInfo(this.isAutoCreatePartitions, this.partitionExprs,
                partitionColumns);
        for (SinglePartitionDesc desc : singlePartitionDescs) {
            long partitionId = partitionNameToId.get(desc.getPartitionName());
            rangePartitionInfo.handleNewSinglePartitionDesc(desc, partitionId, isTemp);
        }
        return rangePartitionInfo;
    }
}
