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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeArithmetic;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeExtractAndTransform;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class MTMVPartitionExprDateTrunc implements MTMVPartitionExprService {
    private static Set<String> timeUnits = ImmutableSet.of("year", "quarter", "week", "month", "day", "hour");
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
        } else {
            throw new AnalysisException("date_trunc only support range partition");
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
            String identity = dateTrunc(partitionValue.getStringValue(), dateFormat, false).toString();
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
        Preconditions.checkState(partitionKeyDesc.getLowerValues().size() == 1,
                "only support one partition column");
        DateTimeV2Literal beginTime = dateTrunc(
                partitionKeyDesc.getLowerValues().get(0).getStringValue(),
                Optional.empty(), false);

        PartitionValue lowerValue = new PartitionValue(dateTimeToStr(beginTime, partitionColumnType));
        PartitionValue upperValue = getUpperValue(partitionKeyDesc.getUpperValues().get(0), beginTime,
                partitionColumnType);
        return PartitionKeyDesc.createFixed(
                Collections.singletonList(lowerValue),
                Collections.singletonList(upperValue));
    }

    private PartitionValue getUpperValue(PartitionValue upperValue, DateTimeV2Literal beginTruncTime,
            Type partitionColumnType) throws AnalysisException {
        if (upperValue.isMax()) {
            throw new AnalysisException("date trunc not support MAXVALUE partition");
        }
        // begin time and end time dateTrunc should has same result
        DateTimeV2Literal endTruncTime = dateTrunc(upperValue.getStringValue(), Optional.empty(), true);
        if (!Objects.equals(beginTruncTime, endTruncTime)) {
            throw new AnalysisException(
                    String.format("partition values not equal, beginTruncTime: %s, endTruncTime: %s", beginTruncTime,
                            endTruncTime));
        }
        DateTimeV2Literal endTime = dateIncrement(beginTruncTime);
        return new PartitionValue(dateTimeToStr(endTime, partitionColumnType));
    }

    private DateTimeV2Literal dateTrunc(String value,
            Optional<String> dateFormat, boolean isUpper) throws AnalysisException {
        DateTimeV2Literal dateTimeLiteral = strToDate(value, dateFormat);
        // for (2020-01-31,2020-02-01),if not -1, lower value and upper value will not same after rollup
        if (isUpper) {
            dateTimeLiteral = (DateTimeV2Literal) DateTimeArithmetic.secondsSub(dateTimeLiteral, new IntegerLiteral(1));
        }
        Expression expression = DateTimeExtractAndTransform.dateTrunc(dateTimeLiteral, new VarcharLiteral(timeUnit));
        if (!(expression instanceof DateTimeV2Literal)) {
            throw new AnalysisException("dateTrunc() should return DateLiteral, expression: " + expression);
        }
        return (DateTimeV2Literal) expression;
    }

    private DateTimeV2Literal strToDate(String value,
            Optional<String> dateFormat) throws AnalysisException {
        try {
            return new DateTimeV2Literal(value);
        } catch (Exception e) {
            if (!dateFormat.isPresent()) {
                throw e;
            }
            Expression strToDate = DateTimeExtractAndTransform
                    .strToDate(new VarcharLiteral(value),
                            new VarcharLiteral(dateFormat.get()));
            if (strToDate instanceof DateV2Literal) {
                DateV2Literal dateV2Literal = (DateV2Literal) strToDate;
                return new DateTimeV2Literal(dateV2Literal.getYear(), dateV2Literal.getMonth(), dateV2Literal.getDay(),
                        0, 0, 0);
            } else if (strToDate instanceof DateTimeV2Literal) {
                return (DateTimeV2Literal) strToDate;
            } else {
                throw new AnalysisException(
                        String.format("strToDate failed, stringValue: %s, dateFormat: %s", value,
                                dateFormat));
            }
        }
    }

    private DateTimeV2Literal dateIncrement(DateTimeV2Literal value) throws AnalysisException {
        Expression result;
        switch (timeUnit) {
            case "year":
                result = value.plusYears(1L);
                break;
            case "quarter":
                result = value.plusMonths(3L);
                break;
            case "month":
                result = value.plusMonths(1L);
                break;
            case "week":
                result = value.plusWeeks(1L);
                break;
            case "day":
                result = value.plusDays(1L);
                break;
            case "hour":
                result = value.plusHours(1L);
                break;
            default:
                throw new AnalysisException(
                        "async materialized view partition roll up not support timeUnit: " + timeUnit);
        }
        if (!(result instanceof DateTimeV2Literal)) {
            throw new AnalysisException("sub() should return  DateTimeLiteral, result: " + result);
        }
        return (DateTimeV2Literal) result;
    }

    private String dateTimeToStr(DateTimeV2Literal literal,
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
                    "MTMV not support partition with column type : " + partitionColumnType);
        }
    }
}
