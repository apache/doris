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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.thrift.TNullableStringLiteral;

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
        return partitionExprUtil.new FunctionIntervalInfo(timeUnit, interval);
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

    public static Map<String, AddPartitionClause> getAddPartitionClauseFromPartitionValues(OlapTable olapTable,
            ArrayList<List<TNullableStringLiteral>> partitionValues, PartitionInfo partitionInfo)
            throws AnalysisException {
        Map<String, AddPartitionClause> result = Maps.newHashMap();
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
                DateLiteral beginDateTime = new DateLiteral(beginTime, partitionColumnType);
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
                partitionName += getFormatPartitionValue(filterStr);
                if (hasStringType) {
                    if (partitionName.length() > 50) {
                        throw new AnalysisException("Partition name's length is over limit of 50. abort to create.");
                    }
                }
            } else {
                throw new AnalysisException("now only support range and list partition");
            }

            Map<String, String> partitionProperties = Maps.newHashMap();
            DistributionDesc distributionDesc = olapTable.getDefaultDistributionInfo().toDistributionDesc();

            SinglePartitionDesc singleRangePartitionDesc = new SinglePartitionDesc(true, partitionName,
                    partitionKeyDesc, partitionProperties);
            // iff table's storage medium is not equal default storage medium,
            // should add storage medium in partition properties
            if (!DataProperty.DEFAULT_STORAGE_MEDIUM.equals(olapTable.getStorageMedium())) {
                partitionProperties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM,
                        olapTable.getStorageMedium().name());
            }
            AddPartitionClause addPartitionClause = new AddPartitionClause(singleRangePartitionDesc,
                    distributionDesc, partitionProperties, false);
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
        } else if (partitionColumnType.isDatetime() || partitionColumnType.isDatetimeV2()) {
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
        public String timeUnit;
        public long interval;

        public FunctionIntervalInfo(String timeUnit, long interval) {
            this.timeUnit = timeUnit;
            this.interval = interval;
        }
    }
}
