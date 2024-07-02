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

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.NotImplementedException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PartitionDesc {
    protected List<String> partitionColNames;
    protected List<SinglePartitionDesc> singlePartitionDescs;
    protected ArrayList<Expr> partitionExprs; //eg: auto partition by range date_trunc(column, 'day')
    protected boolean isAutoCreatePartitions;
    protected PartitionType type;
    public static final ImmutableSet<String> RANGE_PARTITION_FUNCTIONS = new ImmutableSortedSet.Builder<String>(
            String.CASE_INSENSITIVE_ORDER).add("date_trunc").add("date_ceil").add("date_floor").add("second_floor")
            .add("minute_floor").add("hour_floor").add("day_floor").add("month_floor").add("year_floor")
            .add("second_ceil").add("minute_ceil").add("hour_ceil").add("day_ceil").add("month_ceil").add("year_ceil")
            .build();

    public PartitionDesc() {}

    public PartitionDesc(List<String> partitionColNames,
                         List<AllPartitionDesc> allPartitionDescs) throws AnalysisException {
        this.partitionColNames = partitionColNames;
        this.singlePartitionDescs = handleAllPartitionDesc(allPartitionDescs);
    }

    public List<SinglePartitionDesc> handleAllPartitionDesc(List<AllPartitionDesc> allPartitionDescs)
            throws AnalysisException {
        boolean isMultiPartition = false;
        List<SinglePartitionDesc> tmpList = Lists.newArrayList();
        if (allPartitionDescs != null) {
            for (AllPartitionDesc allPartitionDesc : allPartitionDescs) {
                if (allPartitionDesc instanceof SinglePartitionDesc) {
                    tmpList.add((SinglePartitionDesc) allPartitionDesc);
                } else if (allPartitionDesc instanceof MultiPartitionDesc) {
                    isMultiPartition = true;
                    List<SinglePartitionDesc> singlePartitionDescList
                            = ((MultiPartitionDesc) allPartitionDesc).getSinglePartitionDescList();
                    tmpList.addAll(singlePartitionDescList);
                }
            }
        }
        if (isMultiPartition && partitionColNames.size() != 1) {
            throw new AnalysisException("multi partition column size except 1 but provided "
                    + partitionColNames.size() + ".");
        }
        return tmpList;
    }

    public List<SinglePartitionDesc> getSinglePartitionDescs() {
        return this.singlePartitionDescs;
    }

    public SinglePartitionDesc getSinglePartitionDescByName(String partitionName) {
        for (SinglePartitionDesc singlePartitionDesc : this.singlePartitionDescs) {
            if (singlePartitionDesc.getPartitionName().equals(partitionName)) {
                return singlePartitionDesc;
            }
        }
        return null;
    }

    public List<String> getPartitionColNames() {
        return partitionColNames;
    }

    // 1. partition by list (column) : now support one slotRef
    // 2. partition by range(column/function(column)) : support slotRef and some
    // special function eg: date_trunc, date_floor/ceil
    public static List<String> getColNamesFromExpr(ArrayList<Expr> exprs, boolean isListPartition,
            boolean isAutoPartition)
            throws AnalysisException {
        List<String> colNames = new ArrayList<>();
        for (Expr expr : exprs) {
            if ((expr instanceof FunctionCallExpr) && (isListPartition == false)) {
                FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
                List<Expr> paramsExpr = functionCallExpr.getParams().exprs();
                String name = functionCallExpr.getFnName().getFunction();
                if (RANGE_PARTITION_FUNCTIONS.contains(name)) {
                    for (Expr param : paramsExpr) {
                        if (param instanceof SlotRef) {
                            if (colNames.isEmpty()) {
                                colNames.add(((SlotRef) param).getColumnName());
                            } else {
                                throw new AnalysisException(
                                        "auto create partition only support one slotRef in function expr. "
                                                + expr.toSql());
                            }
                        }
                    }
                } else {
                    throw new AnalysisException(
                            "auto create partition only support function call expr is date_trunc/date_floor/date_ceil. "
                                    + expr.toSql());
                }
            } else if (expr instanceof SlotRef) {
                if (isAutoPartition && !colNames.isEmpty() && !isListPartition) {
                    throw new AnalysisException(
                            "auto create partition only support one slotRef in expr of RANGE partition. "
                                    + expr.toSql());
                }
                colNames.add(((SlotRef) expr).getColumnName());
            } else {
                if (!isListPartition) {
                    throw new AnalysisException(
                            "auto create partition only support slotRef and date_trunc/date_floor/date_ceil"
                                    + "function in range partitions. " + expr.toSql());
                } else {
                    throw new AnalysisException(
                            "auto create partition only support slotRef in list partitions. "
                                    + expr.toSql());
                }
            }
        }
        if (colNames.isEmpty()) {
            throw new AnalysisException(
                    "auto create partition have not find any partition columns. "
                            + exprs.get(0).toSql());
        }
        return colNames;
    }

    public void analyze(List<ColumnDef> columnDefs, Map<String, String> otherProperties) throws AnalysisException {
        if (partitionColNames == null || partitionColNames.isEmpty()) {
            throw new AnalysisException("No partition columns.");
        }

        int createTablePartitionMaxNum = ConnectContext.get().getSessionVariable().getCreateTablePartitionMaxNum();
        if (singlePartitionDescs.size() > createTablePartitionMaxNum) {
            throw new AnalysisException(String.format(
                    "The number of partitions to be created is [%s], exceeding the maximum value of [%s]. "
                            + "Creating too many partitions can be time-consuming. If necessary, "
                            + "You can set the session variable 'create_table_partition_max_num' to a larger value.",
                    singlePartitionDescs.size(), createTablePartitionMaxNum));
        }

        // `analyzeUniqueKeyMergeOnWrite` would modify `properties`, which will be used later,
        // so we just clone a properties map here.
        boolean enableUniqueKeyMergeOnWrite = false;
        if (otherProperties != null) {
            enableUniqueKeyMergeOnWrite =
                PropertyAnalyzer.analyzeUniqueKeyMergeOnWrite(Maps.newHashMap(otherProperties));
        }
        Set<String> partColNames = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (String partitionCol : partitionColNames) {
            if (!partColNames.add(partitionCol)) {
                throw new AnalysisException("Duplicated partition column " + partitionCol);
            }

            boolean found = false;
            for (ColumnDef columnDef : columnDefs) {
                if (columnDef.getName().equals(partitionCol)) {
                    if (!columnDef.isKey()) {
                        if (columnDef.getAggregateType() != AggregateType.NONE) {
                            throw new AnalysisException("The partition column could not be aggregated column");
                        }
                        if (enableUniqueKeyMergeOnWrite) {
                            throw new AnalysisException("Merge-on-Write table's partition column must be KEY column");
                        }
                    }
                    if (columnDef.getType().isFloatingPointType()) {
                        throw new AnalysisException("Floating point type column can not be partition column");
                    }
                    if (columnDef.getType().isScalarType(PrimitiveType.STRING)) {
                        throw new AnalysisException("String Type should not be used in partition column["
                                + columnDef.getName() + "].");
                    }
                    if (columnDef.getType().isComplexType()) {
                        throw new AnalysisException("Complex type column can't be partition column: "
                                + columnDef.getType().toString());
                    }
                    if (!ConnectContext.get().getSessionVariable().isAllowPartitionColumnNullable()
                            && columnDef.isAllowNull()) {
                        throw new AnalysisException(
                                "The partition column must be NOT NULL with allow_partition_column_nullable OFF");
                    }
                    if (this instanceof RangePartitionDesc && isAutoCreatePartitions) {
                        if (columnDef.isAllowNull()) {
                            throw new AnalysisException("AUTO RANGE PARTITION doesn't support NULL column");
                        }
                        if (partitionExprs != null) {
                            for (Expr expr : partitionExprs) {
                                if (!(expr instanceof FunctionCallExpr) || !RANGE_PARTITION_FUNCTIONS
                                        .contains(((FunctionCallExpr) expr).getFnName().getFunction())) {
                                    throw new AnalysisException(
                                            "auto create partition only support slotRef and "
                                                    + "date_trunc/date_floor/date_ceil function in range partitions. "
                                                    + expr.toSql());
                                }
                            }
                            if (partitionExprs.get(0) instanceof FunctionCallExpr) {
                                if (!columnDef.getType().isDateType()) {
                                    throw new AnalysisException(
                                            "Auto range partition needs Date/DateV2/"
                                                    + "Datetime/DatetimeV2 column as partition column but got"
                                                    + partitionExprs.get(0).toSql());
                                }
                            }
                        }
                    }
                    found = true;
                    break;
                }
            }

            if (!found) {
                throw new AnalysisException("Partition column[" + partitionCol + "] does not exist in column list.");
            }
        }

        Set<String> nameSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (SinglePartitionDesc desc : singlePartitionDescs) {
            if (!nameSet.add(desc.getPartitionName())) {
                throw new AnalysisException("Duplicated partition name: " + desc.getPartitionName());
            }
            // in create table stmt, we use given properties
            // copy one. because ProperAnalyzer will remove entry after analyze
            Map<String, String> givenProperties = null;
            if (otherProperties != null) {
                givenProperties = Maps.newHashMap(otherProperties);
            }
            // check partitionType
            checkPartitionKeyValueType(desc.getPartitionKeyDesc());
            // analyze singlePartitionDesc
            desc.analyze(columnDefs.size(), givenProperties);
        }
    }

    public void checkPartitionKeyValueType(PartitionKeyDesc partitionKeyDesc) throws AnalysisException {

    }

    public PartitionType getType() {
        return type;
    }

    public ArrayList<Expr> getPartitionExprs() {
        return partitionExprs;
    }

    public boolean isAutoCreatePartitions() {
        return isAutoCreatePartitions;
    }

    public String toSql() {
        throw new NotImplementedException("toSql not implemented");
    }

    public PartitionInfo toPartitionInfo(List<Column> schema, Map<String, Long> partitionNameToId, boolean isTemp)
            throws DdlException, AnalysisException {
        throw new NotImplementedException("toPartitionInfo not implemented");
    }

    public boolean inIdentifierPartitions(String colName) {
        return partitionColNames != null && partitionColNames.contains(colName);
    }
}
