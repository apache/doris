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
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.AutoBucketCalculator;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.nereids.trees.plans.commands.info.AddPartitionOp;
import org.apache.doris.thrift.TNullableStringLiteral;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PartitionExprUtil {
    public static final String DATETIME_FORMATTER = "%04d-%02d-%02d %02d:%02d:%02d";
    public static final String DATE_FORMATTER = "%04d-%02d-%02d";
    public static final String DATETIME_NAME_FORMATTER = "%04d%02d%02d%02d%02d%02d";
    private static final Logger LOG = LogManager.getLogger(PartitionExprUtil.class);
    private static final PartitionExprUtil partitionExprUtil = new PartitionExprUtil();
    private static final int MAX_PARTITION_NAME_LENGTH = 50;

    /**
     * The effective storage type of a partition column when a function expression wraps it.
     * For AUTO LIST partitioning we store the function output as the partition key literal,
     * so the partition column type must match the function return type.
     * - date_trunc: DATETIME -> DATETIME (raw column type unchanged)
     * - from_unixtime: BIGINT -> VARCHAR(64) (function outputs a formatted date string)
     * Non-function expressions preserve their raw column type.
     */
    public static Type getEffectivePartitionColumnType(Expr expr, Type rawColType) throws AnalysisException {
        if (!(expr instanceof FunctionCallExpr)) {
            return rawColType;
        }
        String fn = ((FunctionCallExpr) expr).getFnName().getFunction().toLowerCase();
        if ("date_trunc".equals(fn)) {
            return rawColType;
        }
        if ("from_unixtime".equals(fn)) {
            return ScalarType.createVarcharType(64);
        }
        // Any other function is handled by the RANGE path (date_floor/date_ceil etc.);
        // it does NOT need column type rewrite because the RANGE branch reads column type
        // via createPartitionKeyDescWithRange and expects the raw column type.
        return rawColType;
    }

    /**
     * Reject partition function calls whose granularity is second or minute, which would
     * create thousands of partitions per hour and blow up metadata. Accepted granularities
     * are hour / day / month / year.
     *
     * date_trunc: rejects unit 'second' and 'minute'.
     * from_unixtime: rejects any format string that contains sub-hour components (%s, %i,
     *   :ss, :mm, mm, ss). Format must be a StringLiteral; wildcards / dynamic formats
     *   are rejected. When the format has no time component at all we treat it as day/month
     *   granularity and accept.
     */
    public static void assertPartitionFormatAcceptable(Expr expr) throws AnalysisException {
        if (!(expr instanceof FunctionCallExpr)) {
            return;
        }
        FunctionCallExpr fce = (FunctionCallExpr) expr;
        String fn = fce.getFnName().getFunction().toLowerCase();
        List<Expr> params = fce.getParams().exprs();

        if ("date_trunc".equals(fn)) {
            // date_trunc's signature (col, unit_literal) is already enforced by Doris function
            // resolution: 1-arg calls fail as "illegal" and non-literal unit is rejected.
            // We only need to guard the granularity value here.
            String unit = ((StringLiteral) params.get(1)).getStringValue().toLowerCase();
            if ("second".equals(unit) || "minute".equals(unit)) {
                throw new AnalysisException(
                        "date_trunc granularity '" + unit + "' is not allowed for auto partition "
                                + "(would create up to 86400 partitions per day). "
                                + "Use 'hour', 'day', 'month' or 'year' instead.");
            }
            return;
        }

        if ("from_unixtime".equals(fn)) {
            if (params.size() < 2) {
                throw new AnalysisException(
                        "from_unixtime in auto partition requires an explicit format literal "
                                + "(e.g. 'yyyy-MM-dd' or '%Y-%m-%d').");
            }
            Expr fmtExpr = params.get(1);
            if (!(fmtExpr instanceof StringLiteral)) {
                throw new AnalysisException(
                        "from_unixtime in auto partition requires a string literal format.");
            }
            String fmt = ((StringLiteral) fmtExpr).getStringValue();
            String norm = fmt.toLowerCase();
            // MySQL-style seconds/minutes markers
            if (norm.contains("%s") || norm.contains("%i")) {
                throw new AnalysisException(
                        "from_unixtime format '" + fmt + "' contains sub-hour granularity "
                                + "(%s / %i); would create too many partitions. "
                                + "Use hour or coarser granularity.");
            }
            // Java-style seconds/minutes: look for ss or mm as time components
            // Only check when a time component 'HH' / 'hh' is present, because
            // 'mm' as month can appear standalone in date-only formats like 'yyyy-MM'
            boolean hasHour = norm.contains("hh");
            if (hasHour && (norm.contains(":ss") || norm.contains(":mm") || norm.endsWith("ss"))) {
                throw new AnalysisException(
                        "from_unixtime format '" + fmt + "' contains sub-hour granularity "
                                + "(minute/second); would create too many partitions. "
                                + "Use hour or coarser granularity.");
            }
        }
    }

    /**
     * Walk the expression AST and return the first SlotRef encountered, or null if none.
     * Used to locate which schema column a partition expression references
     * (e.g. date_trunc(dt, 'day') -> SlotRef(dt)).
     */
    public static SlotRef findFirstSlotRef(Expr expr) {
        if (expr == null) {
            return null;
        }
        if (expr instanceof SlotRef) {
            return (SlotRef) expr;
        }
        for (Expr child : expr.getChildren()) {
            SlotRef found = findFirstSlotRef(child);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    public static FunctionIntervalInfo getFunctionIntervalInfo(ArrayList<Expr> partitionExprs,
            PartitionType partitionType) throws AnalysisException {
        if (partitionType != PartitionType.RANGE) {
            return null;
        }
        if (partitionExprs.size() != 1) {
            throw new AnalysisException("now only support one expr in range partition");
        }

        Expr e = partitionExprs.get(0);
        if (!(e instanceof FunctionCallExpr)) {
            throw new AnalysisException("now range partition only support FunctionCallExpr");
        }
        FunctionCallExpr functionCallExpr = (FunctionCallExpr) e;
        String fnName = functionCallExpr.getFnName().getFunction().toLowerCase();
        String timeUnit;
        long interval;
        if ("date_trunc".equalsIgnoreCase(fnName)) {
            List<Expr> paramsExprs = functionCallExpr.getParams().exprs();
            if (paramsExprs.size() != 2) {
                throw new AnalysisException("date_trunc params exprs size should be 2.");
            }
            Expr param = paramsExprs.get(1);
            if (!(param instanceof StringLiteral)) {
                throw new AnalysisException("date_trunc param of time unit is not string literal.");
            }
            timeUnit = ((StringLiteral) param).getStringValue().toLowerCase();
            interval = 1L;
        } else if (PartitionDesc.RANGE_PARTITION_FUNCTIONS.contains(fnName)) {
            List<Expr> paramsExprs = functionCallExpr.getParams().exprs();
            if (paramsExprs.size() != 3) {
                throw new AnalysisException("date_floor/date_ceil params exprs size should be 3.");
            }
            Expr param = paramsExprs.get(1);
            if (!(param instanceof IntLiteral)) {
                throw new AnalysisException("date_floor/date_ceil param of interval must be int literal.");
            }
            //date_floor(event_day,interval 5 day) ---> day_floor(`event_day`, 5, '0001-01-01 00:00:00')
            String[] splits = fnName.split("_");
            timeUnit = splits[0]; //day
            interval = ((IntLiteral) param).getLongValue(); //5
        } else {
            throw new AnalysisException("now range partition only support date_trunc/date_floor/date_ceil.");
        }
        return partitionExprUtil.new FunctionIntervalInfo(fnName, timeUnit, interval);
    }

    public static DateLiteral getRangeEnd(DateLiteral beginTime, FunctionIntervalInfo intervalInfo)
            throws AnalysisException {
        String timeUnit = intervalInfo.timeUnit;
        long interval = intervalInfo.interval;
        switch (timeUnit) {
            case "year":
                return beginTime.plusYears(interval);
            case "quarter":
                return beginTime.plusMonths(interval * 3);
            case "month":
                return beginTime.plusMonths(interval);
            case "week":
                return beginTime.plusDays(interval * 7);
            case "day":
                return beginTime.plusDays(interval);
            case "hour":
                return beginTime.plusHours(interval);
            case "minute":
                return beginTime.plusMinutes(interval);
            case "second":
                return beginTime.plusSeconds(interval);
            default:
                break;
        }
        return null;
    }

    public static Map<String, AddPartitionOp> getAddPartitionClauseFromPartitionValues(
            OlapTable olapTable, ArrayList<List<TNullableStringLiteral>> partitionValues, PartitionInfo partitionInfo)
            throws AnalysisException {
        Map<String, AddPartitionOp> result = Maps.newHashMap();
        ArrayList<Expr> partitionExprs = partitionInfo.getPartitionExprs();
        PartitionType partitionType = partitionInfo.getType();
        List<Column> partitionColumn = partitionInfo.getPartitionColumns();
        boolean hasStringType = partitionColumn.stream().anyMatch(column -> column.getType().isStringType());
        FunctionIntervalInfo intervalInfo = getFunctionIntervalInfo(partitionExprs, partitionType);
        Set<String> filterPartitionValues = new HashSet<String>();

        for (List<TNullableStringLiteral> partitionValueList : partitionValues) {
            PartitionKeyDesc partitionKeyDesc = null;
            String partitionName = "p";
            ArrayList<String> curPartitionValues = new ArrayList<>();
            for (TNullableStringLiteral tStringLiteral : partitionValueList) {
                if (tStringLiteral.is_null) {
                    if (partitionType == PartitionType.RANGE) {
                        throw new AnalysisException("Can't create partition for NULL Range");
                    }
                    curPartitionValues.add(null);
                } else {
                    curPartitionValues.add(tStringLiteral.value);
                }
            }
            // Concatenate each string with its length. X means null
            String filterStr = curPartitionValues.stream()
                    .map(s -> (s == null) ? "X" : (s + s.length()))
                    .reduce("", (s1, s2) -> s1 + s2);
            if (filterPartitionValues.contains(filterStr)) {
                continue;
            }
            filterPartitionValues.add(filterStr);
            if (partitionType == PartitionType.RANGE) {
                String beginTime = curPartitionValues.get(0); // have check range type size must be 1
                Type partitionColumnType = partitionColumn.get(0).getType();
                DateLiteral beginDateTime = DateLiteralUtils.createDateLiteral(beginTime, partitionColumnType);
                partitionName += String.format(DATETIME_NAME_FORMATTER,
                    beginDateTime.getYear(), beginDateTime.getMonth(), beginDateTime.getDay(),
                    beginDateTime.getHour(), beginDateTime.getMinute(), beginDateTime.getSecond());
                DateLiteral endDateTime = getRangeEnd(beginDateTime, intervalInfo);
                partitionKeyDesc = createPartitionKeyDescWithRange(beginDateTime, endDateTime, partitionColumnType);
            } else if (partitionType == PartitionType.LIST) {
                List<List<PartitionValue>> listValues = new ArrayList<>();
                List<PartitionValue> inValues = new ArrayList<>();
                for (String value : curPartitionValues) {
                    if (value == null) {
                        inValues.add(new PartitionValue("", true));
                    } else {
                        inValues.add(new PartitionValue(value));
                    }
                }
                listValues.add(inValues);
                partitionKeyDesc = PartitionKeyDesc.createIn(listValues);
                // For multi-column LIST, build a readable partition name by joining
                // sanitized per-value tokens with underscores. Collision uniqueness is
                // already guaranteed by filterStr (which appends per-value length).
                StringBuilder suffix = new StringBuilder();
                for (String v : curPartitionValues) {
                    suffix.append("_").append(v == null ? "null" : getFormatPartitionValue(v));
                }
                partitionName += suffix.toString();
                if (hasStringType) {
                    if (partitionName.length() > MAX_PARTITION_NAME_LENGTH) {
                        partitionName = partitionName.substring(0, MAX_PARTITION_NAME_LENGTH)
                            + "_" + Integer.toHexString(partitionName.hashCode());
                    }
                }
            } else {
                throw new AnalysisException("now only support range and list partition");
            }

            Map<String, String> partitionProperties = Maps.newHashMap();
            DistributionInfo defaultDistributionInfo = olapTable.getDefaultDistributionInfo();
            DistributionDesc distributionDesc = defaultDistributionInfo.toDistributionDesc();
            if (olapTable.isAutoBucket() && partitionType == PartitionType.RANGE) {
                // Use unified auto bucket calculator
                AutoBucketCalculator.AutoBucketContext context = new AutoBucketCalculator.AutoBucketContext(
                        olapTable, partitionName, partitionName, false,
                        defaultDistributionInfo.getBucketNum());

                int bucketsNum = AutoBucketCalculator.calculateAutoBucketsWithBoundsCheck(context);

                // Only update distribution if calculation was successful (bucketsNum != default)
                if (bucketsNum != defaultDistributionInfo.getBucketNum()) {
                    if (defaultDistributionInfo.getType() == DistributionInfo.DistributionInfoType.HASH) {
                        HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) defaultDistributionInfo;
                        List<String> distColumnNames = new ArrayList<>();
                        for (Column distributionColumn : hashDistributionInfo.getDistributionColumns()) {
                            distColumnNames.add(distributionColumn.getName());
                        }
                        distributionDesc = new HashDistributionDesc(bucketsNum, distColumnNames);
                    } else {
                        distributionDesc = new RandomDistributionDesc(bucketsNum);
                    }
                }
            }

            SinglePartitionDesc singleRangePartitionDesc = new SinglePartitionDesc(true, partitionName,
                    partitionKeyDesc, partitionProperties);
            // iff table's storage medium is not equal default storage medium,
            // should add storage medium in partition properties
            if (!DataProperty.DEFAULT_STORAGE_MEDIUM.equals(olapTable.getStorageMedium())) {
                partitionProperties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM,
                        olapTable.getStorageMedium().name());
            }
            AddPartitionOp addPartitionClause = new AddPartitionOp(
                    singleRangePartitionDesc.translateToPartitionDefinition(),
                    distributionDesc.toDistributionDescriptor(), partitionProperties, false);
            result.put(partitionName, addPartitionClause);
        }
        return result;
    }

    private static PartitionKeyDesc createPartitionKeyDescWithRange(DateLiteral beginDateTime,
            DateLiteral endDateTime, Type partitionColumnType) throws AnalysisException {
        PartitionValue lowerValue = getPartitionFromDate(partitionColumnType, beginDateTime);
        PartitionValue upperValue = getPartitionFromDate(partitionColumnType, endDateTime);
        return PartitionKeyDesc.createFixed(
                Collections.singletonList(lowerValue),
                Collections.singletonList(upperValue));
    }

    private static PartitionValue getPartitionFromDate(Type partitionColumnType, DateLiteral dateLiteral)
            throws AnalysisException {
        // check out of range.
        try {
            // if lower than range, parse will error. so if hits here, the only possiblility
            // is rounding to beyond the limit
            dateLiteral.checkValueValid();
        } catch (AnalysisException e) {
            return PartitionValue.MAX_VALUE;
        }

        String timeString;
        if (partitionColumnType.isDate() || partitionColumnType.isDateV2()) {
            timeString = String.format(DATE_FORMATTER, dateLiteral.getYear(), dateLiteral.getMonth(),
                    dateLiteral.getDay());
        } else if (partitionColumnType.isDatetime() || partitionColumnType.isDatetimeV2()
                || partitionColumnType.isTimeStampTz()) {
            timeString = String.format(DATETIME_FORMATTER,
                    dateLiteral.getYear(), dateLiteral.getMonth(), dateLiteral.getDay(),
                    dateLiteral.getHour(), dateLiteral.getMinute(), dateLiteral.getSecond());
        } else {
            throw new AnalysisException(
                    "not support range partition with column type : " + partitionColumnType.toString());
        }

        return new PartitionValue(timeString);
    }

    private static String getFormatPartitionValue(String value) {
        StringBuilder sb = new StringBuilder();
        // When the value is negative
        if (value.length() > 0 && value.charAt(0) == '-') {
            sb.append("_");
        }
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if ((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9')) {
                sb.append(ch);
            } else {
                int unicodeValue = value.codePointAt(i);
                String unicodeString = Integer.toHexString(unicodeValue);
                sb.append(unicodeString);
            }
        }
        return sb.toString();
    }

    public class FunctionIntervalInfo {
        public String fnName;
        public String timeUnit;
        public long interval;

        public FunctionIntervalInfo(String fnName, String timeUnit, long interval) {
            this.fnName = fnName;
            this.timeUnit = timeUnit;
            this.interval = interval;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FunctionIntervalInfo that = (FunctionIntervalInfo) o;
            return interval == that.interval && Objects.equal(fnName, that.fnName)
                    && Objects.equal(timeUnit, that.timeUnit);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(fnName, timeUnit, interval);
        }
    }
}
