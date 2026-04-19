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

import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.PartitionExprUtil;
import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TimestampArithmeticExpr;
import org.apache.doris.analysis.ToSqlParams;
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
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Rollup service for expressions:
 * date_trunc(date_add/date_sub(partition_col, interval N hour), 'unit')
 */
public class MTMVPartitionExprDateTruncDateAddSub implements MTMVPartitionExprService {
    private static final Set<String> TIME_UNITS = ImmutableSet.of(
            "year", "quarter", "week", "month", "day", "hour");
    private final String timeUnit;
    private final long offsetHours;

    public MTMVPartitionExprDateTruncDateAddSub(FunctionCallExpr functionCallExpr) throws AnalysisException {
        List<Expr> paramsExprs = functionCallExpr.getParams().exprs();
        if (paramsExprs.size() != 2) {
            throw new AnalysisException("date_trunc params exprs size should be 2.");
        }
        Expr dateTruncUnit = paramsExprs.get(1);
        if (!(dateTruncUnit instanceof StringLiteral)) {
            throw new AnalysisException("date_trunc param of time unit is not string literal.");
        }
        this.timeUnit = dateTruncUnit.getStringValue().toLowerCase();

        Expr dateTruncArg = paramsExprs.get(0);
        while (dateTruncArg instanceof CastExpr) {
            dateTruncArg = dateTruncArg.getChild(0);
        }
        if (!(dateTruncArg instanceof TimestampArithmeticExpr)) {
            throw new AnalysisException("date_trunc first argument should be date_add/date_sub in mtmv partition");
        }
        TimestampArithmeticExpr timestampArithmeticExpr = (TimestampArithmeticExpr) dateTruncArg;
        if (timestampArithmeticExpr.getTimeUnit() != TimestampArithmeticExpr.TimeUnit.HOUR) {
            throw new AnalysisException("only HOUR unit is supported in date_add/date_sub for mtmv partition");
        }
        if (!(timestampArithmeticExpr.getChild(1) instanceof LiteralExpr)) {
            throw new AnalysisException("date_add/date_sub offset should be a literal");
        }
        long offset = parseOffsetHours((LiteralExpr) timestampArithmeticExpr.getChild(1));
        String functionName = timestampArithmeticExpr.getFuncName().toLowerCase();
        if ("date_sub".equals(functionName)) {
            this.offsetHours = -offset;
        } else if ("date_add".equals(functionName)) {
            this.offsetHours = offset;
        } else {
            throw new AnalysisException("only date_add/date_sub is supported in mtmv partition");
        }
    }

    @Override
    public String getRollUpIdentity(PartitionKeyDesc partitionKeyDesc, Map<String, String> mvProperties)
            throws AnalysisException {
        DateTimeV2Literal first = null;
        Optional<String> dateFormat = getDateFormat(mvProperties);
        List<List<PartitionValue>> inValues = partitionKeyDesc.getInValues();
        for (int i = 0; i < inValues.size(); i++) {
            PartitionValue partitionValue = inValues.get(i).get(0);
            if (partitionValue.isNullPartition()) {
                throw new AnalysisException("date_trunc + date_add/date_sub not support null partition value");
            }
            DateTimeV2Literal identity = dateTruncByOffset(partitionValue.getStringValue(), dateFormat);
            if (i == 0) {
                first = identity;
            } else if (!isSameTime(first, identity)) {
                throw new AnalysisException(
                        String.format("partition values not equal, first: %s, identity: %s", first, identity));
            }
        }
        Preconditions.checkState(first != null, "partition values is empty");
        return first.toString();
    }

    @Override
    public PartitionKeyDesc generateRollUpPartitionKeyDesc(PartitionKeyDesc partitionKeyDesc,
            MTMVPartitionInfo mvPartitionInfo, MTMVRelatedTableIf pctTable) throws AnalysisException {
        List<PartitionKeyDesc> descs = generateRollUpPartitionKeyDescs(partitionKeyDesc, mvPartitionInfo, pctTable);
        Preconditions.checkState(descs.size() == 1,
                "expected single roll-up partition desc, but got: %s", descs);
        return descs.get(0);
    }

    @Override
    public List<PartitionKeyDesc> generateRollUpPartitionKeyDescs(PartitionKeyDesc partitionKeyDesc,
            MTMVPartitionInfo mvPartitionInfo, MTMVRelatedTableIf pctTable) throws AnalysisException {
        Type partitionColumnType = MTMVPartitionUtil
                .getPartitionColumnType(pctTable, mvPartitionInfo.getPartitionColByPctTable(pctTable));
        Preconditions.checkState(partitionKeyDesc.getLowerValues().size() == 1,
                "only support one partition column");

        DateTimeV2Literal beginBucket = dateTruncByOffset(
                partitionKeyDesc.getLowerValues().get(0).getStringValue(), Optional.empty());

        // The upper bound of a range partition is exclusive.
        // Compute the offset-applied upper value BEFORE truncation so we can detect
        // whether it falls exactly on a time-unit boundary.
        //
        // Case A – offset-aligned boundary (e.g. upper = 21:00:00, offset +3h):
        //   upperWithOffset = 00:00:00 of next day  ==  endBucket  → exact boundary hit
        //   → the last actual data value (epsilon below upper) maps to the PREVIOUS bucket,
        //     so endBucket must NOT be included.
        //
        // Case B – UTC-midnight boundary (e.g. upper = 00:00:00, offset +3h):
        //   upperWithOffset = 03:00:00  !=  endBucket (00:00:00)  → mid-bucket hit
        //   → actual data near the upper bound still maps to endBucket,
        //     so endBucket MUST be included.
        DateTimeV2Literal upperRaw = strToDate(
                partitionKeyDesc.getUpperValues().get(0).getStringValue(), Optional.empty());
        DateTimeV2Literal upperWithOffset = dateOffset(upperRaw);
        DateTimeV2Literal endBucket = applyDateTrunc(upperWithOffset);
        boolean includeEndBucket = !isSameTime(upperWithOffset, endBucket);

        List<PartitionKeyDesc> res = new ArrayList<>();
        DateTimeV2Literal currentBucket = beginBucket;
        while (includeEndBucket
                ? !currentBucket.toJavaDateType().isAfter(endBucket.toJavaDateType())
                : currentBucket.toJavaDateType().isBefore(endBucket.toJavaDateType())) {
            PartitionValue lowerValue = new PartitionValue(dateTimeToStr(currentBucket, partitionColumnType));
            DateTimeV2Literal nextBucket = dateIncrement(currentBucket);
            PartitionValue upperValue = new PartitionValue(dateTimeToStr(nextBucket, partitionColumnType));
            res.add(PartitionKeyDesc.createFixed(
                    Collections.singletonList(lowerValue),
                    Collections.singletonList(upperValue)));
            currentBucket = nextBucket;
        }
        return res;
    }

    @Override
    public void analyze(MTMVPartitionInfo mvPartitionInfo) throws AnalysisException {
        if (!TIME_UNITS.contains(this.timeUnit)) {
            throw new AnalysisException(
                    String.format("timeUnit not support: %s, only support: %s", this.timeUnit, TIME_UNITS));
        }
        List<BaseColInfo> pctInfos = mvPartitionInfo.getPctInfos();
        for (BaseColInfo pctInfo : pctInfos) {
            MTMVRelatedTableIf pctTable = MTMVUtil.getRelatedTable(pctInfo.getTableInfo());
            PartitionType partitionType = pctTable.getPartitionType(MvccUtil.getSnapshotFromContext(pctTable));
            if (partitionType == PartitionType.RANGE) {
                Type partitionColumnType = MTMVPartitionUtil.getPartitionColumnType(pctTable, pctInfo.getColName());
                if (!(partitionColumnType.isDatetime() || partitionColumnType.isDatetimeV2())) {
                    throw new AnalysisException(
                            "partitionColumnType should be datetime/datetimev2 "
                                    + "when PartitionType is range and expr is date_trunc + date_add/date_sub");
                }
            } else {
                throw new AnalysisException("date_trunc + date_add/date_sub only support range partition");
            }
        }
    }

    @Override
    public String toSql(MTMVPartitionInfo mvPartitionInfo) {
        Preconditions.checkState(mvPartitionInfo.getExpr() != null, "mtmv partition expr is null");
        return mvPartitionInfo.getExpr().accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITHOUT_TABLE);
    }

    private DateTimeV2Literal dateTruncByOffset(String value, Optional<String> dateFormat)
            throws AnalysisException {
        DateTimeV2Literal dateTimeLiteral = strToDate(value, dateFormat);
        dateTimeLiteral = dateOffset(dateTimeLiteral);
        return applyDateTrunc(dateTimeLiteral);
    }

    private DateTimeV2Literal applyDateTrunc(DateTimeV2Literal value) throws AnalysisException {
        Expression expression = DateTimeExtractAndTransform.dateTrunc(value, new VarcharLiteral(timeUnit));
        if (!(expression instanceof DateTimeV2Literal)) {
            throw new AnalysisException("dateTrunc() should return DateLiteral, expression: " + expression);
        }
        return (DateTimeV2Literal) expression;
    }

    private static boolean isSameTime(DateTimeV2Literal left, DateTimeV2Literal right) {
        return left.toJavaDateType().equals(right.toJavaDateType());
    }

    private DateTimeV2Literal dateOffset(DateTimeV2Literal value) throws AnalysisException {
        long offsetSeconds = offsetHours * 3600L;
        Expression result = offsetSeconds >= 0
                ? DateTimeArithmetic.secondsAdd(value, new BigIntLiteral(offsetSeconds))
                : DateTimeArithmetic.secondsSub(value, new BigIntLiteral(-offsetSeconds));
        if (!(result instanceof DateTimeV2Literal)) {
            throw new AnalysisException("date offset should return DateTimeV2Literal, result: " + result);
        }
        return (DateTimeV2Literal) result;
    }

    private DateTimeV2Literal strToDate(String value, Optional<String> dateFormat) throws AnalysisException {
        try {
            return new DateTimeV2Literal(value);
        } catch (Exception e) {
            if (!dateFormat.isPresent()) {
                throw e;
            }
            Expression strToDate = DateTimeExtractAndTransform.strToDate(new VarcharLiteral(value),
                    new VarcharLiteral(dateFormat.get()));
            if (strToDate instanceof DateV2Literal) {
                DateV2Literal dateV2Literal = (DateV2Literal) strToDate;
                return new DateTimeV2Literal(dateV2Literal.getYear(), dateV2Literal.getMonth(), dateV2Literal.getDay(),
                        0, 0, 0);
            } else if (strToDate instanceof DateTimeV2Literal) {
                return (DateTimeV2Literal) strToDate;
            } else {
                throw new AnalysisException(
                        String.format("strToDate failed, stringValue: %s, dateFormat: %s", value, dateFormat));
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
            throw new AnalysisException("dateIncrement() should return DateTimeLiteral, result: " + result);
        }
        return (DateTimeV2Literal) result;
    }

    private String dateTimeToStr(DateTimeV2Literal literal, Type partitionColumnType) throws AnalysisException {
        if (partitionColumnType.isDate() || partitionColumnType.isDateV2()) {
            return String.format(PartitionExprUtil.DATE_FORMATTER, literal.getYear(), literal.getMonth(),
                    literal.getDay());
        } else if (partitionColumnType.isDatetime() || partitionColumnType.isDatetimeV2()
                || partitionColumnType.isTimeStampTz()) {
            return String.format(PartitionExprUtil.DATETIME_FORMATTER,
                    literal.getYear(), literal.getMonth(), literal.getDay(),
                    literal.getHour(), literal.getMinute(), literal.getSecond());
        } else {
            throw new AnalysisException(
                    "MTMV not support partition with column type : " + partitionColumnType);
        }
    }

    private Optional<String> getDateFormat(Map<String, String> mvProperties) {
        return StringUtils.isEmpty(mvProperties.get(PropertyAnalyzer.PROPERTIES_PARTITION_DATE_FORMAT))
                ? Optional.empty()
                : Optional.of(mvProperties.get(PropertyAnalyzer.PROPERTIES_PARTITION_DATE_FORMAT));
    }

    private long parseOffsetHours(LiteralExpr offsetExpr) throws AnalysisException {
        try {
            return Long.parseLong(offsetExpr.getStringValue());
        } catch (NumberFormatException e) {
            throw new AnalysisException("date_add/date_sub hour offset should be integer");
        }
    }

    public long getOffsetHours() {
        return offsetHours;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MTMVPartitionExprDateTruncDateAddSub that = (MTMVPartitionExprDateTruncDateAddSub) o;
        return offsetHours == that.offsetHours && Objects.equals(timeUnit, that.timeUnit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeUnit, offsetHours);
    }
}
