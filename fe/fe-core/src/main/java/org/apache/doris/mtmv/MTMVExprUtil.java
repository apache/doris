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
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeExtractAndTransform;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

import java.util.Collections;
import java.util.List;

public class MTMVExprUtil {

    public static PartitionKeyDesc rollUpRange(MTMVPartitionInfo mvPartitionInfo, PartitionKeyDesc partitionKeyDesc)
            throws AnalysisException {
        if (mvPartitionInfo.getPartitionType() != MTMVPartitionType.EXPR) {
            throw new AnalysisException("mv partition type is not EXPR.");
        }
        return generateRollUpPartitionKeyDesc(mvPartitionInfo, partitionKeyDesc);
    }

    public static PartitionKeyDesc generateRollUpPartitionKeyDesc(MTMVPartitionInfo mvPartitionInfo,
            PartitionKeyDesc partitionKeyDesc) throws AnalysisException {
        Expr expr = mvPartitionInfo.getExpr();
        if (!(expr instanceof FunctionCallExpr)) {
            throw new AnalysisException("now range partition only support FunctionCallExpr");
        }
        FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
        String fnName = functionCallExpr.getFnName().getFunction().toLowerCase();
        if ("date_trunc".equals(fnName)) {
            return generateRollUpPartitionKeyDescByDateTrunc(partitionKeyDesc, functionCallExpr);
        }
        throw new AnalysisException("now support function name: " + fnName);
    }

    private static PartitionKeyDesc generateRollUpPartitionKeyDescByDateTrunc(
            PartitionKeyDesc partitionKeyDesc, FunctionCallExpr functionCallExpr)
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
        DateLiteral beginTime = dateTrunc(lowerValue, timeUnit);
        DateLiteral endTime = dateAdd(beginTime, timeUnit);
        String upperValue = partitionKeyDesc.getUpperValues().get(0).getStringValue();
        checkUpperValue(upperValue, beginTime, endTime);
        return createPartitionKeyDescWithRange(beginTime, endTime);
    }

    public static PartitionKeyDesc createPartitionKeyDescWithRange(DateLiteral beginTime,
            DateLiteral endDateTime) throws AnalysisException {
        PartitionValue lowerValue = new PartitionValue(beginTime.getStringValue());
        PartitionValue upperValue = new PartitionValue(endDateTime.getStringValue());
        return PartitionKeyDesc.createFixed(
                Collections.singletonList(lowerValue),
                Collections.singletonList(upperValue));
    }

    private static void checkUpperValue(String upperValue, DateLiteral beginTime, DateLiteral endTime) {
        // TODO: 2024/3/5 check
    }

    private static DateLiteral dateTrunc(String value, String timeUnit) throws AnalysisException {
        Expression expression = DateTimeExtractAndTransform
                .dateTrunc(new DateLiteral(value), new VarcharLiteral(timeUnit));
        if (!(expression instanceof DateLiteral)) {
            throw new AnalysisException("dateTrunc() should return DateLiteral, expression: " + expression);
        }
        return (DateLiteral) expression;
    }

    public static DateLiteral dateAdd(DateLiteral value, String timeUnit)
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
        if (!(result instanceof DateLiteral)) {
            throw new AnalysisException("sub() should return  DateTimeLiteral, result: " + result);
        }
        return (DateLiteral) result;
    }
}

