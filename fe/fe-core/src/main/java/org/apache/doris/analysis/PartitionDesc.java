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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionTableInfo;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.NotImplementedException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
                if (isAutoPartition && !isListPartition) {
                    throw new AnalysisException(
                            "auto create partition only support date_trunc function of RANGE partition. "
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

    /**
     *  Convert to PartitionTableInfo
     */
    public PartitionTableInfo convertToPartitionTableInfo() {
        List<PartitionDefinition> partitionDefinitions = null;
        List<Expression> partitionFields = null;

        if (!type.name().equalsIgnoreCase(PartitionType.UNPARTITIONED.name())) {
            // only auto partition support partition expr
            if (!isAutoCreatePartitions) {
                if (partitionExprs.stream().anyMatch(expr -> expr instanceof FunctionCallExpr)) {
                    throw new org.apache.doris.nereids.exceptions.AnalysisException("Non-auto partition "
                        + "table not support partition expr!");
                }
            }

            try {
                partitionDefinitions = singlePartitionDescs.stream()
                    .map(SinglePartitionDesc::translateToPartitionDefinition)
                    .collect(Collectors.toList());

                partitionFields = partitionExprs.stream()
                    .map(expr -> {
                        if (expr instanceof SlotRef) {
                            SlotRef slot = (SlotRef) expr;
                            return new UnboundSlot(slot.getColumnName());
                        } else if (expr instanceof FunctionCallExpr) {
                            FunctionCallExpr function = (FunctionCallExpr) expr;
                            List<Expression> expressions = function.getFnParams().exprs().stream()
                                    .map(exp -> {
                                        if (exp instanceof SlotRef) {
                                            SlotRef slot = (SlotRef) exp;
                                            return new UnboundSlot(slot.getColumnName());
                                        } else if (exp instanceof StringLiteral) {
                                            return Literal.of((((StringLiteral) exp).getStringValue()));
                                        } else {
                                            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                                                "unsupported argument " + exp.toString());
                                        }
                                    })
                                    .collect(Collectors.toList());

                            return new UnboundFunction(
                                function.getFnName().getFunction(),
                                expressions);
                        } else {
                            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                                "unsupported auto partition expr " + expr.toString());
                        }
                    })
                    .collect(Collectors.toList());
            } catch (Exception e) {
                throw new org.apache.doris.nereids.exceptions.AnalysisException(e.getMessage(), e.getCause());
            }
        }

        return new PartitionTableInfo(
            isAutoCreatePartitions,
            type.name(),
            partitionDefinitions.isEmpty() ? null : partitionDefinitions,
            partitionFields);
    }
}
