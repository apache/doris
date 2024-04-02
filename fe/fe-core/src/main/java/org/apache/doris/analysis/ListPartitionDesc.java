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
import org.apache.doris.catalog.ListPartitionInfo;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// to describe the key list partition's information in create table stmt
public class ListPartitionDesc extends PartitionDesc {

    public ListPartitionDesc(List<String> partitionColNames,
                             List<AllPartitionDesc> allPartitionDescs) throws AnalysisException {
        super(partitionColNames, allPartitionDescs);
        type = PartitionType.LIST;
        this.isAutoCreatePartitions = false;
        this.partitionExprs = new ArrayList<>(partitionColNames.stream()
            .map(col -> new SlotRef(null, col))
            .collect(Collectors.toList()));
    }

    public ListPartitionDesc(ArrayList<Expr> exprs, List<String> partitionColNames,
            List<AllPartitionDesc> allPartitionDescs) throws AnalysisException {
        if (exprs != null) {
            this.partitionExprs = exprs;
        }
        this.partitionColNames = partitionColNames;
        this.singlePartitionDescs = handleAllPartitionDesc(allPartitionDescs);
        this.type = PartitionType.LIST;
        this.isAutoCreatePartitions = true;
    }

    public static ListPartitionDesc createListPartitionDesc(ArrayList<Expr> exprs,
            List<AllPartitionDesc> allPartitionDescs) throws AnalysisException {
        List<String> colNames = getColNamesFromExpr(exprs, true, true);
        return new ListPartitionDesc(exprs, colNames, allPartitionDescs);
    }

    @Override
    public void checkPartitionKeyValueType(PartitionKeyDesc partitionKeyDesc) throws AnalysisException {
        if (partitionKeyDesc.getPartitionType() != PartitionKeyValueType.IN) {
            throw new AnalysisException("You can only use in values to create list partitions");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("PARTITION BY LIST(");
        int idx = 0;
        for (Expr e : partitionExprs) {
            if (idx > 0) {
                sb.append(", ");
            }
            idx++;
            sb.append(e.toSql());
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
                    try {
                        ListPartitionInfo.checkPartitionColumn(column);
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

        ListPartitionInfo listPartitionInfo = new ListPartitionInfo(this.isAutoCreatePartitions, this.partitionExprs,
                partitionColumns);
        for (SinglePartitionDesc desc : singlePartitionDescs) {
            long partitionId = partitionNameToId.get(desc.getPartitionName());
            listPartitionInfo.handleNewSinglePartitionDesc(desc, partitionId, isTemp);
        }
        return listPartitionInfo;
    }
}
