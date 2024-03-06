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
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeExtractAndTransform;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

import java.util.Collections;
import java.util.List;

public class MTMVPartitionExprDateTrunc implements MTMVPartitionExprService {
    private String timeUnit;

    public MTMVPartitionExprDateTrunc(FunctionCallExpr functionCallExpr) throws AnalysisException {
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
        // TODO: 2024/3/6 check timeunit
        this.timeUnit = param.getStringValue().toLowerCase();
    }

    @Override
    public String getRollUpIdentity(PartitionKeyDesc partitionKeyDesc)
            throws AnalysisException {
        // mtmv only support one partition column
        String firstValue = partitionKeyDesc.getInValues().get(0).get(0).getStringValue();
        DateTimeLiteral firstTime = dateTrunc(firstValue);
        // checkOtherValue();
        return firstTime.toString();
    }

    @Override
    public PartitionKeyDesc generateRollUpPartitionKeyDesc(PartitionKeyDesc partitionKeyDesc,
            MTMVPartitionInfo mvPartitionInfo) throws AnalysisException {
        Type partitionColumnType = MTMVPartitionUtil
                .getPartitionColumnType(mvPartitionInfo.getRelatedTable(), mvPartitionInfo.getRelatedCol());
        // mtmv only support one partition column
        String lowerValue = partitionKeyDesc.getLowerValues().get(0).getStringValue();
        DateTimeLiteral beginTime = dateTrunc(lowerValue);
        DateTimeLiteral endTime = dateAdd(beginTime);
        // TODO: 2024/3/6 check upper value
        return createPartitionKeyDescWithRange(beginTime, endTime, partitionColumnType);
    }

    private DateTimeLiteral dateTrunc(String value) throws AnalysisException {
        Expression expression = DateTimeExtractAndTransform
                .dateTrunc(new DateTimeLiteral(value), new VarcharLiteral(timeUnit));
        if (!(expression instanceof DateTimeLiteral)) {
            throw new AnalysisException("dateTrunc() should return DateLiteral, expression: " + expression);
        }
        return (DateTimeLiteral) expression;
    }

    private DateTimeLiteral dateAdd(DateTimeLiteral value)
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

    private PartitionKeyDesc createPartitionKeyDescWithRange(DateTimeLiteral beginTime,
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
}
