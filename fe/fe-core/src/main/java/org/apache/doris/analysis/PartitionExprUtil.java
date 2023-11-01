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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TStringLiteral;

import com.github.javaparser.quality.Preconditions;
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
            case "month":
                return beginTime.plusMonths(interval);
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

    // In one calling, because we have partition values filter, the same partition
    // value won't make duplicate AddPartitionClause.
    // But if there's same partition values in two calling of this. we may have the
    // different partition name because we have timestamp suffix here.
    // Should check existence of partitions in this table. so need at least readlock
    // first.
    // @return <newName, newPartitionClause>
    // @return existPartitionIds will save exist partition's id.
    public static Map<String, AddPartitionClause> getNonExistPartitionAddClause(OlapTable olapTable,
            ArrayList<TStringLiteral> partitionValues, PartitionInfo partitionInfo, ArrayList<Long> existPartitionIds)
            throws AnalysisException {
        Preconditions.checkArgument(!partitionInfo.isMultiColumnPartition(),
                "now dont support multi key columns in auto-partition.");

        Map<String, AddPartitionClause> result = Maps.newHashMap();
        ArrayList<Expr> partitionExprs = partitionInfo.getPartitionExprs();
        PartitionType partitionType = partitionInfo.getType();
        List<Column> partiitonColumn = partitionInfo.getPartitionColumns();
        Type partitionColumnType = partiitonColumn.get(0).getType();
        FunctionIntervalInfo intervalInfo = getFunctionIntervalInfo(partitionExprs, partitionType);
        Set<String> filterPartitionValues = new HashSet<String>();

        for (TStringLiteral partitionValue : partitionValues) {
            PartitionKeyDesc partitionKeyDesc = null;
            String partitionName = "p";
            String value = partitionValue.value;
            if (filterPartitionValues.contains(value)) {
                continue;
            }
            filterPartitionValues.add(value);

            // check if this key value has been covered by some partition.
            Long id = partitionInfo.contains(partitionValue, partitionType);
            if (id != null) { // found
                existPartitionIds.add(id);
                continue;
            }

            if (partitionType == PartitionType.RANGE) {
                String beginTime = value;
                DateLiteral beginDateTime = new DateLiteral(beginTime, partitionColumnType);
                partitionName += String.format(DATETIME_NAME_FORMATTER,
                        beginDateTime.getYear(), beginDateTime.getMonth(), beginDateTime.getDay(),
                        beginDateTime.getHour(), beginDateTime.getMinute(), beginDateTime.getSecond());
                DateLiteral endDateTime = getRangeEnd(beginDateTime, intervalInfo);
                partitionKeyDesc = createPartitionKeyDescWithRange(beginDateTime, endDateTime, partitionColumnType);
            } else if (partitionType == PartitionType.LIST) {
                List<List<PartitionValue>> listValues = new ArrayList<>();
                String pointValue = value;
                PartitionValue lowerValue = new PartitionValue(pointValue);
                listValues.add(Collections.singletonList(lowerValue));
                partitionKeyDesc = PartitionKeyDesc.createIn(
                        listValues);
                // the partition's name can't contain some special characters. so some string
                // values(like a*b and ab) will get same partition name. to distingush them, we
                // have to add a timestamp.
                partitionName += getFormatPartitionValue(lowerValue.getStringValue());
                if (partitionColumnType.isStringType()) {
                    partitionName += "_" + System.currentTimeMillis();
                }
            } else {
                throw new AnalysisException("auto-partition only support range and list partition");
            }

            Map<String, String> partitionProperties = Maps.newHashMap();
            DistributionDesc distributionDesc = olapTable.getDefaultDistributionInfo().toDistributionDesc();

            SinglePartitionDesc partitionDesc = new SinglePartitionDesc(true, partitionName,
                    partitionKeyDesc, partitionProperties);

            AddPartitionClause addPartitionClause = new AddPartitionClause(partitionDesc,
                    distributionDesc, partitionProperties, false);
            result.put(partitionName, addPartitionClause);
        }
        return result;
    }

    public static PartitionKeyDesc createPartitionKeyDescWithRange(DateLiteral beginDateTime,
            DateLiteral endDateTime, Type partitionColumnType) throws AnalysisException {
        String beginTime;
        String endTime;
        // maybe need check the range in FE also, like getAddPartitionClause.
        if (partitionColumnType.isDate() || partitionColumnType.isDateV2()) {
            beginTime = String.format(DATE_FORMATTER, beginDateTime.getYear(), beginDateTime.getMonth(),
                    beginDateTime.getDay());
            endTime = String.format(DATE_FORMATTER, endDateTime.getYear(), endDateTime.getMonth(),
                    endDateTime.getDay());
        } else if (partitionColumnType.isDatetime() || partitionColumnType.isDatetimeV2()) {
            beginTime = String.format(DATETIME_FORMATTER,
                    beginDateTime.getYear(), beginDateTime.getMonth(), beginDateTime.getDay(),
                    beginDateTime.getHour(), beginDateTime.getMinute(), beginDateTime.getSecond());
            endTime = String.format(DATETIME_FORMATTER,
                    endDateTime.getYear(), endDateTime.getMonth(), endDateTime.getDay(),
                    endDateTime.getHour(), endDateTime.getMinute(), endDateTime.getSecond());
        } else {
            throw new AnalysisException(
                    "not support range partition with column type : " + partitionColumnType.toString());
        }
        PartitionValue lowerValue = new PartitionValue(beginTime);
        PartitionValue upperValue = new PartitionValue(endTime);
        return PartitionKeyDesc.createFixed(
                Collections.singletonList(lowerValue),
                Collections.singletonList(upperValue));
    }

    public static String getFormatPartitionValue(String value) {
        StringBuilder sb = new StringBuilder();
        // When the value is negative
        if (value.length() > 0 && value.charAt(0) == '-') {
            sb.append("_");
        }
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if ((ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9')) {
                sb.append(ch);
            } else if (ch == '-' || ch == ':' || ch == ' ' || ch == '*') {
                // Main user remove characters in time
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
