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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.PartitionExprUtil;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeExtractAndTransform;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

import java.util.Collections;
import java.util.List;

public class MTMVExprUtil {
    public static String getRollUpIdentity(MTMVPartitionInfo mvPartitionInfo,
            PartitionKeyDesc partitionKeyDesc, MTMVRelatedTableIf relatedTable) throws AnalysisException {
        Expr expr = mvPartitionInfo.getExpr();
        if (!(expr instanceof FunctionCallExpr)) {
            throw new AnalysisException("now mtmv partition only support FunctionCallExpr");
        }
        FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
        String fnName = functionCallExpr.getFnName().getFunction().toLowerCase();
        if ("date_trunc".equals(fnName)) {
            return getRollUpIdentityByDateTrunc(partitionKeyDesc, functionCallExpr,
                    getPartitionColumnType(relatedTable, mvPartitionInfo.getRelatedCol()));
        }
        throw new AnalysisException("now support function name: " + fnName);
    }

    public static PartitionKeyDesc generateRollUpPartitionKeyDesc(MTMVPartitionInfo mvPartitionInfo,
            PartitionKeyDesc partitionKeyDesc, MTMVRelatedTableIf relatedTable) throws AnalysisException {
        Expr expr = mvPartitionInfo.getExpr();
        if (!(expr instanceof FunctionCallExpr)) {
            throw new AnalysisException("now mtmv partition only support FunctionCallExpr");
        }
        FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
        String fnName = functionCallExpr.getFnName().getFunction().toLowerCase();
        if ("date_trunc".equals(fnName)) {
            return generateRollUpPartitionKeyDescByDateTrunc(partitionKeyDesc, functionCallExpr,
                    getPartitionColumnType(relatedTable, mvPartitionInfo.getRelatedCol()));
        }
        throw new AnalysisException("now support function name: " + fnName);
    }

    private static Type getPartitionColumnType(MTMVRelatedTableIf relatedTable, String col) throws AnalysisException {
        List<Column> partitionColumns = relatedTable.getPartitionColumns();
        for (Column column : partitionColumns) {
            if (column.getName().equals(col)) {
                return column.getType();
            }
        }
        throw new AnalysisException("can not getPartitionColumnType by:" + col);
    }

    private static PartitionKeyDesc generateRollUpPartitionKeyDescByDateTrunc(
            PartitionKeyDesc partitionKeyDesc, FunctionCallExpr functionCallExpr, Type partitionColumnType)
            throws AnalysisException {
        List<Expr> paramsExprs = functionCallExpr.getParams().exprs();
        // TODO: 2024/3/5 check it in createMTMVInfo
        // TODO: 2024/3/5 check is date type
        if (paramsExprs.size() != 2) {
            throw new AnalysisException("date_trunc params exprs size should be 2.");
        }
        Expr param = paramsExprs.get(1);
        if (!(param instanceof StringLiteral)) {
            throw new AnalysisException("date_trunc param of time unit is not string literal.");
        }
        // check timeunit
        String timeUnit = param.getStringValue().toLowerCase();
        // mtmv only support one partition column
        String lowerValue = partitionKeyDesc.getLowerValues().get(0).getStringValue();
        DateTimeLiteral beginTime = dateTrunc(lowerValue, timeUnit);
        DateTimeLiteral endTime = dateAdd(beginTime, timeUnit);
        String upperValue = partitionKeyDesc.getUpperValues().get(0).getStringValue();
        checkUpperValue(upperValue, beginTime, endTime);
        return createPartitionKeyDescWithRange(beginTime, endTime, partitionColumnType);
    }

    private static String getRollUpIdentityByDateTrunc(PartitionKeyDesc partitionKeyDesc,
            FunctionCallExpr functionCallExpr, Type partitionColumnType)
            throws AnalysisException {
        List<Expr> paramsExprs = functionCallExpr.getParams().exprs();
        // TODO: 2024/3/5 check it in createMTMVInfo
        // TODO: 2024/3/5 check is date type
        if (paramsExprs.size() != 2) {
            throw new AnalysisException("date_trunc params exprs size should be 2.");
        }
        Expr param = paramsExprs.get(1);
        if (!(param instanceof StringLiteral)) {
            throw new AnalysisException("date_trunc param of time unit is not string literal.");
        }
        // check timeunit
        String timeUnit = param.getStringValue().toLowerCase();
        // mtmv only support one partition column
        String firstValue = partitionKeyDesc.getInValues().get(0).get(0).getStringValue();
        DateTimeLiteral firstTime = dateTrunc(firstValue, timeUnit);
        // checkOtherValue();
        return firstTime.toString();
    }

    public static PartitionKeyDesc createPartitionKeyDescWithRange(DateTimeLiteral beginTime,
            DateTimeLiteral endTime, Type partitionColumnType) throws AnalysisException {
        String beginTimeStr;
        String endTimeStr;
        // maybe need check the range in FE also, like getAddPartitionClause.
        if (partitionColumnType.isDate() || partitionColumnType.isDateV2()) {
            beginTimeStr = String.format(PartitionExprUtil.DATE_FORMATTER, beginTime.getYear(), beginTime.getMonth(),
                    beginTime.getDay());
            endTimeStr = String.format(PartitionExprUtil.DATE_FORMATTER, endTime.getYear(), endTime.getMonth(),
                    endTime.getDay());
        } else if (partitionColumnType.isDatetime() || partitionColumnType.isDatetimeV2()) {
            beginTimeStr = String.format(PartitionExprUtil.DATETIME_FORMATTER,
                    beginTime.getYear(), beginTime.getMonth(), beginTime.getDay(),
                    beginTime.getHour(), beginTime.getMinute(), beginTime.getSecond());
            endTimeStr = String.format(PartitionExprUtil.DATETIME_FORMATTER,
                    endTime.getYear(), endTime.getMonth(), endTime.getDay(),
                    endTime.getHour(), endTime.getMinute(), endTime.getSecond());
        } else {
            throw new AnalysisException(
                    "MTMV swnot support partition with column type : " + partitionColumnType.toString());
        }
        PartitionValue lowerValue = new PartitionValue(beginTimeStr);
        PartitionValue upperValue = new PartitionValue(endTimeStr);
        return PartitionKeyDesc.createFixed(
                Collections.singletonList(lowerValue),
                Collections.singletonList(upperValue));
    }

    private static void checkUpperValue(String upperValue, DateLiteral beginTime, DateLiteral endTime) {
        // TODO: 2024/3/5 check
    }

    private static DateTimeLiteral dateTrunc(String value, String timeUnit) throws AnalysisException {
        Expression expression = DateTimeExtractAndTransform
                .dateTrunc(new DateTimeLiteral(value), new VarcharLiteral(timeUnit));
        if (!(expression instanceof DateTimeLiteral)) {
            throw new AnalysisException("dateTrunc() should return DateLiteral, expression: " + expression);
        }
        return (DateTimeLiteral) expression;
    }

    public static DateTimeLiteral dateAdd(DateTimeLiteral value, String timeUnit)
            throws AnalysisException {
        Expression result;
        switch (timeUnit) {
            case "year":
                result = value.plusYears(1L);
                break;
            case "month":
                result = value.plusMonths(1L);
                break;
            case "day":
                result = value.plusDays(1L);
                break;
            default:
                throw new AnalysisException("MTMV partition roll up not support timeUnit: " + timeUnit);
        }
        if (!(result instanceof DateTimeLiteral)) {
            throw new AnalysisException("sub() should return  DateTimeLiteral, result: " + result);
        }
        return (DateTimeLiteral) result;
    }
}

