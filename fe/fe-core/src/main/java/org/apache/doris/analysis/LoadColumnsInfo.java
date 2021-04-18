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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Pair;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * LoadColumnsInfo saves all columns' mapping expression
 */
public class LoadColumnsInfo implements ParseNode {
    private final List<String> columnNames;
    private final List<Expr> columnMappingList;

    // the following maps are parsed from 'columnMappingList'
    // col name -> (func name -> func args)
    private Map<String, Pair<String, List<String>>> columnToFunction;
    private Map<String, Expr> parsedExprMap;

    public LoadColumnsInfo(List<String> columnNames, List<Expr> columnMappingList) {
        this.columnNames = columnNames;
        this.columnMappingList = columnMappingList;
    }

    public Map<String, Expr> getParsedExprMap() {
        return parsedExprMap;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        checkColumnNames();
        checkColumnMapping();
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("COLUMNS ( ");
        sb.append(Joiner.on(",").join(columnNames));
        sb.append(")");

        if (columnMappingList != null || columnMappingList.size() != 0) {
            sb.append(" SET (");
            sb.append(Joiner.on(",").join(columnMappingList.parallelStream()
                                                  .map(entity -> entity.toSql()).collect(Collectors.toList())));
            sb.append(")");
        }
        return sb.toString();
    }

    private void checkColumnNames() throws AnalysisException {
        if (columnNames == null || columnNames.isEmpty()) {
            return;
        }
        Set<String> columnSet = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (String col : columnNames) {
            if (!columnSet.add(col)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DUP_FIELDNAME, col);
            }
        }
    }

    private void checkColumnMapping() throws AnalysisException {
        if (columnMappingList == null || columnMappingList.isEmpty()) {
            return;
        }

        columnToFunction = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        parsedExprMap = Maps.newHashMap();
        for (Expr expr : columnMappingList) {
            if (!(expr instanceof BinaryPredicate)) {
                throw new AnalysisException("Mapping function should only be binary predicate: " + expr.toSql());
            }

            BinaryPredicate predicate = (BinaryPredicate) expr;
            if (predicate.getOp() != BinaryPredicate.Operator.EQ) {
                throw new AnalysisException("Mapping function should only be binary predicate with EQ operator: "
                        + predicate.getOp());
            }

            Expr child0 = predicate.getChild(0);
            if (!(child0 instanceof SlotRef)) {
                throw new AnalysisException("Mapping function's left child should be a column name: " + child0.toSql());
            }

            String column = ((SlotRef) child0).getColumnName();
            if (columnToFunction.containsKey(column)) {
                throw new AnalysisException("Duplicate mapping for column: " + column);
            }

            Expr child1 = predicate.getChild(1);
            if (!(child1 instanceof FunctionCallExpr)) {
                throw new AnalysisException("Mapping function's right child should be a function: " + child1.toSql());
            }

            if (!child1.supportSerializable()) {
                throw new AnalysisException("Expr do not support serializable." + child1.toSql());
            }

            parsedExprMap.put(column, child1);

            FunctionCallExpr functionCallExpr = (FunctionCallExpr) child1;
            String functionName = functionCallExpr.getFnName().getFunction();
            List<Expr> paramExprs = functionCallExpr.getParams().exprs();
            List<String> args = Lists.newArrayList();
            for (Expr paramExpr : paramExprs) {
                if (paramExpr instanceof SlotRef) {
                    SlotRef slot = (SlotRef) paramExpr;
                    args.add(slot.getColumnName());
                } else if (paramExpr instanceof StringLiteral) {
                    StringLiteral literal = (StringLiteral) paramExpr;
                    args.add(literal.getValue());
                } else if (paramExpr instanceof NullLiteral) {
                    args.add(null);
                } else {
                    throw new AnalysisException("Mapping function args error, arg: " + paramExpr.toSql());
                }
            }

            Pair<String, List<String>> functionPair = new Pair<String, List<String>>(functionName, args);
            columnToFunction.put(column, functionPair);
        }
    }
}
