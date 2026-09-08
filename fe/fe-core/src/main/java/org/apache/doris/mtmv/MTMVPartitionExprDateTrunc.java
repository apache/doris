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
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeArithmetic;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeExtractAndTransform;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.TimeStampNsLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimestampTzLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.expressions.literal.format.DateTimeChecker;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
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
        List<BaseColInfo> pctInfos = mvPartitionInfo.getPctInfos();
        for (BaseColInfo pctInfo : pctInfos) {
            MTMVRelatedTableIf pctTable = MTMVUtil.getRelatedTable(pctInfo.getTableInfo());
            PartitionType partitionType = pctTable.getPartitionType(MvccUtil.getSnapshotFromContext(pctTable));
            if (partitionType == PartitionType.RANGE) {
                Type partitionColumnType = MTMVPartitionUtil
                        .getPartitionColumnType(pctTable, pctInfo.getColName());
                if (!partitionColumnType.isDateType()) {
                    throw new AnalysisException(
                            "partitionColumnType should be date/datetime "
                                    + "when PartitionType is range and expr is date_trunc");
                }
            } else {
                throw new AnalysisException("date_trunc only support range partition");
            }
        }
    }

    @Override
    public String toSql(MTMVPartitionInfo mvPartitionInfo) {
        return String.format("date_trunc(`%s`, '%s')", mvPartitionInfo.getPartitionCol(), timeUnit);
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
            MTMVPartitionInfo mvPartitionInfo, MTMVRelatedTableIf pctTable) throws AnalysisException {
        Type partitionColumnType = MTMVPartitionUtil
                .getPartitionColumnType(pctTable, mvPartitionInfo.getPartitionColByPctTable(pctTable));
        // mtmv only support one partition column
        Preconditions.checkState(partitionKeyDesc.getLowerValues().size() == 1,
                "only support one partition column");
        if (partitionColumnType.isTimeStampNs()) {
            return generateTimeStampNsRollUpPartitionKeyDesc(partitionKeyDesc);
        }
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

    private PartitionKeyDesc generateTimeStampNsRollUpPartitionKeyDesc(PartitionKeyDesc partitionKeyDesc)
            throws AnalysisException {
        TimeStampNsLiteral lower = new TimeStampNsLiteral(
                partitionKeyDesc.getLowerValues().get(0).getStringValue());
        LocalDateTime beginTruncTime = dateTrunc(lower.toJavaDateType());

        PartitionValue upperValue = partitionKeyDesc.getUpperValues().get(0);
        LocalDateTime upperRepresentative = upperValue.isMax()
                ? TimeStampNsLiteral.getMaxValue().toJavaDateType()
                : new TimeStampNsLiteral(upperValue.getStringValue()).toJavaDateType().minusNanos(1);
        LocalDateTime endTruncTime = dateTrunc(upperRepresentative);
        if (!beginTruncTime.equals(endTruncTime)) {
            throw new AnalysisException(
                    String.format("partition values not equal, beginTruncTime: %s, endTruncTime: %s",
                            beginTruncTime, endTruncTime));
        }

        PartitionValue lowerValue = timestampNsToPartitionValue(beginTruncTime);
        PartitionValue rollUpUpperValue = timestampNsToPartitionValue(dateIncrement(beginTruncTime));
        return PartitionKeyDesc.createFixed(
                Collections.singletonList(lowerValue),
                Collections.singletonList(rollUpUpperValue));
    }

    private PartitionValue timestampNsToPartitionValue(LocalDateTime value) {
        LocalDateTime minValue = TimeStampNsLiteral.getMinValue().toJavaDateType();
        LocalDateTime maxValue = TimeStampNsLiteral.getMaxValue().toJavaDateType();
        // The exclusive end of the last natural bucket is beyond the type maximum.
        // Keep it as infinity because clamping it to the legal maximum would exclude that value.
        if (value.isAfter(maxValue)) {
            return PartitionValue.MAX_VALUE;
        }
        // The first natural bucket starts before the signed epoch-nanosecond minimum.
        LocalDateTime clampedValue = value.isBefore(minValue) ? minValue : value;
        return new PartitionValue(TimeStampNsLiteral.fromJavaDateType(clampedValue).getStringValue());
    }

    private LocalDateTime dateTrunc(LocalDateTime value) throws AnalysisException {
        switch (timeUnit) {
            case "year":
                return LocalDateTime.of(value.getYear(), 1, 1, 0, 0);
            case "quarter":
                return LocalDateTime.of(value.getYear(), (value.getMonthValue() - 1) / 3 * 3 + 1, 1, 0, 0);
            case "month":
                return LocalDateTime.of(value.getYear(), value.getMonthValue(), 1, 0, 0);
            case "week":
                return value.minusDays(value.getDayOfWeek().getValue() - 1L).truncatedTo(ChronoUnit.DAYS);
            case "day":
                return value.truncatedTo(ChronoUnit.DAYS);
            case "hour":
                return value.truncatedTo(ChronoUnit.HOURS);
            default:
                throw new AnalysisException(
                        "async materialized view partition roll up not support timeUnit: " + timeUnit);
        }
    }

    private LocalDateTime dateIncrement(LocalDateTime value) throws AnalysisException {
        switch (timeUnit) {
            case "year":
                return value.plusYears(1L);
            case "quarter":
                return value.plusMonths(3L);
            case "month":
                return value.plusMonths(1L);
            case "week":
                return value.plusWeeks(1L);
            case "day":
                return value.plusDays(1L);
            case "hour":
                return value.plusHours(1L);
            default:
                throw new AnalysisException(
                        "async materialized view partition roll up not support timeUnit: " + timeUnit);
        }
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
            dateTimeLiteral = (DateTimeV2Literal) DateTimeArithmetic.secondsSub(dateTimeLiteral, new BigIntLiteral(1));
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
            if (DateTimeChecker.hasTimeZone(value)) {
                // For TIMESTAMPTZ values, parse preserving UTC semantics.
                // DateTimeV2Literal would convert to session timezone, which would
                // produce incorrect MV partition boundaries when session tz != UTC.
                TimestampTzLiteral tzLiteral = new TimestampTzLiteral(value);
                return new DateTimeV2Literal(tzLiteral.getYear(), tzLiteral.getMonth(), tzLiteral.getDay(),
                        tzLiteral.getHour(), tzLiteral.getMinute(), tzLiteral.getSecond());
            }
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
        } else if (partitionColumnType.isTimeStampTz()) {
            // The internal DateTimeV2Literal values are always in UTC after truncation.
            // Emit an explicit +00:00 suffix so that downstream consumers
            // (PartitionKey.createPartitionKey -> TimestampTzLiteral.fromSessionTimeZone)
            // interpret the value as UTC rather than session-local time.
            return String.format(PartitionExprUtil.DATETIME_FORMATTER,
                    literal.getYear(), literal.getMonth(), literal.getDay(),
                    literal.getHour(), literal.getMinute(), literal.getSecond()) + "+00:00";
        } else {
            throw new AnalysisException(
                    "MTMV not support partition with column type : " + partitionColumnType);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MTMVPartitionExprDateTrunc that = (MTMVPartitionExprDateTrunc) o;
        return Objects.equals(timeUnit, that.timeUnit);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(timeUnit);
    }
}
