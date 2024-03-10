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
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeExtractAndTransform;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class MTMVPartitionExprDateTrunc implements MTMVPartitionExprService {
    private static Set<String> timeUnits = ImmutableSet.of("year", "month", "day");
    private String timeUnit;

    public MTMVPartitionExprDateTrunc(FunctionCallExpr functionCallExpr) throws AnalysisException {
        List<Expr> paramsExprs = functionCallExpr.getParams().exprs();
        if (paramsExprs.size() != 2) {
            throw new AnalysisException("date_trunc params exprs size should be 2.");
        }
        Expr param = paramsExprs.get(1);
        if (!(param instanceof StringLiteral)) {
            throw new AnalysisException("date_trunc param of time unit is not string literal.");
        }
        this.timeUnit = param.getStringValue().toLowerCase();
    }

    @Override
    public void analyze(MTMVPartitionInfo mvPartitionInfo) throws AnalysisException {
        if (!timeUnits.contains(this.timeUnit)) {
            throw new AnalysisException(
                    String.format("timeUnit not support: %s, only support: %s", this.timeUnit, timeUnits));
        }
        MTMVRelatedTableIf relatedTable = mvPartitionInfo.getRelatedTable();
        PartitionType partitionType = relatedTable.getPartitionType();
        if (partitionType == PartitionType.RANGE) {
            Type partitionColumnType = MTMVPartitionUtil
                    .getPartitionColumnType(mvPartitionInfo.getRelatedTable(), mvPartitionInfo.getRelatedCol());
            if (!partitionColumnType.isDateType()) {
                throw new AnalysisException(
                        "partitionColumnType should be date/datetime "
                                + "when PartitionType is range and expr is date_trunc");
            }
        }
    }

    @Override
    public String getRollUpIdentity(PartitionKeyDesc partitionKeyDesc, Map<String, String> mvProperties)
            throws AnalysisException {
        String res = null;
        Optional<String> dateFormat = getDateFormat(mvProperties);
        List<List<PartitionValue>> inValues = partitionKeyDesc.getInValues();
        for (int i = 0; i < inValues.size(); i++) {
            // mtmv only support one partition column
            PartitionValue partitionValue = inValues.get(i).get(0);
            if (partitionValue.isNullPartition()) {
                throw new AnalysisException("date trunc not support null partition value");
            }
            String identity = dateTrunc(partitionValue.getStringValue(), dateFormat).toString();
            if (i == 0) {
                res = identity;
            } else {
                if (!Objects.equals(res, identity)) {
                    throw new AnalysisException(
                            String.format("partition values not equal, res: %s, identity: %s", res,
                                    identity));
                }
            }
        }
        return res;
    }

    private Optional<String> getDateFormat(Map<String, String> mvProperties) {
        Optional<String> dateFormat =
                StringUtils.isEmpty(mvProperties.get(PropertyAnalyzer.PROPERTIES_PARTITION_DATE_FORMAT))
                        ? Optional.empty()
                        : Optional.of(mvProperties.get(PropertyAnalyzer.PROPERTIES_PARTITION_DATE_FORMAT));
        return dateFormat;
    }

    @Override
    public PartitionKeyDesc generateRollUpPartitionKeyDesc(PartitionKeyDesc partitionKeyDesc,
            MTMVPartitionInfo mvPartitionInfo) throws AnalysisException {
        Type partitionColumnType = MTMVPartitionUtil
                .getPartitionColumnType(mvPartitionInfo.getRelatedTable(), mvPartitionInfo.getRelatedCol());
        // mtmv only support one partition column
        // String upperValue = partitionKeyDesc.getUpperValues().get(0).getStringValue();
        org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral beginTime = dateTrunc(
                partitionKeyDesc.getLowerValues().get(0).getStringValue(),
                Optional.empty());

        PartitionValue lowerValue = new PartitionValue(timeToStr(beginTime, partitionColumnType));
        return PartitionKeyDesc.createFixed(
                Collections.singletonList(lowerValue),
                Collections.singletonList(
                        getUpperValue(partitionKeyDesc.getUpperValues().get(0), beginTime, partitionColumnType)));
    }

    private PartitionValue getUpperValue(PartitionValue upperValue,
            org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral beginTruncTime,
            Type partitionColumnType)
            throws AnalysisException {
        if (upperValue.isMax()) {
            throw new AnalysisException("date trunc not support MAXVALUE partition");
        }
        // begin time and end time dateTrunc should has same result
        org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral endTruncTime = dateTrunc(
                upperValue.getStringValue(),
                Optional.empty());
        if (!Objects.equals(beginTruncTime, endTruncTime)) {
            throw new AnalysisException(
                    String.format("partition values not equal, beginTruncTime: %s, endTruncTime: %s", beginTruncTime,
                            endTruncTime));
        }
        org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral endTime = dateAdd(beginTruncTime);
        return new PartitionValue(timeToStr(endTime, partitionColumnType));
    }

    private org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral dateTrunc(String value,
            Optional<String> dateFormat) throws AnalysisException {
        org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral dateTimeLiteral = strToDate(value,
                dateFormat);
        org.apache.doris.nereids.trees.expressions.Expression expression = DateTimeExtractAndTransform
                .dateTrunc(dateTimeLiteral,
                        new org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral(timeUnit));
        if (!(expression instanceof org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral)) {
            throw new AnalysisException("dateTrunc() should return DateLiteral, expression: " + expression);
        }
        return (org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral) expression;
    }

    private org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral strToDate(String value,
            Optional<String> dateFormat) throws AnalysisException {
        try {
            return new org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral(value);
        } catch (Exception e) {
            if (!dateFormat.isPresent()) {
                throw e;
            }
            org.apache.doris.nereids.trees.expressions.Expression strToDate = DateTimeExtractAndTransform
                    .strToDate(new org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral(value),
                            new org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral(dateFormat.get()));
            if (!(strToDate instanceof org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral)) {
                throw new AnalysisException(
                        String.format("strToDate failed, stringValue: %s, dateFormat: %s", value,
                                dateFormat));
            }
            return (org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral) strToDate;
        }
    }

    private org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral dateAdd(
            org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral value)
            throws AnalysisException {
        org.apache.doris.nereids.trees.expressions.Expression result;
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
        if (!(result instanceof org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral)) {
            throw new AnalysisException("sub() should return  DateTimeLiteral, result: " + result);
        }
        return (org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral) result;
    }

    private String timeToStr(org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral literal,
            Type partitionColumnType) throws AnalysisException {
        if (partitionColumnType.isDate() || partitionColumnType.isDateV2()) {
            return String.format(PartitionExprUtil.DATE_FORMATTER, literal.getYear(), literal.getMonth(),
                    literal.getDay());
        } else if (partitionColumnType.isDatetime() || partitionColumnType.isDatetimeV2()) {
            return String.format(PartitionExprUtil.DATETIME_FORMATTER,
                    literal.getYear(), literal.getMonth(), literal.getDay(),
                    literal.getHour(), literal.getMinute(), literal.getSecond());
        } else {
            throw new AnalysisException(
                    "MTMV swnot support partition with column type : " + partitionColumnType.toString());
        }
    }
}
