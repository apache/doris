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
        String fnName = functionCallExpr.getFnName().getFunction();
        String timeUnit;
        int interval;
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
            interval = 1;
        } else {
            throw new AnalysisException("now range partition only support date_trunc.");
        }
        return partitionExprUtil.new FunctionIntervalInfo(timeUnit, interval);
    }

    public static DateLiteral getRangeEnd(DateLiteral beginTime, FunctionIntervalInfo intervalInfo)
            throws AnalysisException {
        String timeUnit = intervalInfo.timeUnit;
        int interval = intervalInfo.interval;
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

    public static Map<String, AddPartitionClause> getAddPartitionClauseFromPartitionValues(OlapTable olapTable,
            ArrayList<TStringLiteral> partitionValues, PartitionInfo partitionInfo)
            throws AnalysisException {
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
            if (partitionType == PartitionType.RANGE) {
                String beginTime = value;
                DateLiteral beginDateTime = new DateLiteral(beginTime, Type.DATETIMEV2);
                partitionName += String.format(DATETIME_NAME_FORMATTER,
                        beginDateTime.getYear(), beginDateTime.getMonth(), beginDateTime.getDay(),
                        beginDateTime.getHour(), beginDateTime.getMinute(), beginDateTime.getSecond());
                DateLiteral endDateTime = getRangeEnd(beginDateTime, intervalInfo);
                partitionKeyDesc = createPartitionKeyDescWithRange(beginDateTime, endDateTime, partitionColumnType);
            } else if (partitionType == PartitionType.LIST) {
                List<List<PartitionValue>> listValues = new ArrayList<>();
                // TODO: need to support any type
                String pointValue = value;
                PartitionValue lowerValue = new PartitionValue(pointValue);
                listValues.add(Collections.singletonList(lowerValue));
                partitionKeyDesc = PartitionKeyDesc.createIn(
                        listValues);
                partitionName += lowerValue.getStringValue();
            } else {
                throw new AnalysisException("now only support range and list partition");
            }

            Map<String, String> partitionProperties = Maps.newHashMap();
            DistributionDesc distributionDesc = olapTable.getDefaultDistributionInfo().toDistributionDesc();

            SinglePartitionDesc singleRangePartitionDesc = new SinglePartitionDesc(true, partitionName,
                    partitionKeyDesc, partitionProperties);

            AddPartitionClause addPartitionClause = new AddPartitionClause(singleRangePartitionDesc,
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

    public class FunctionIntervalInfo {
        public String timeUnit;
        public int interval;

        public FunctionIntervalInfo(String timeUnit, int interval) {
            this.timeUnit = timeUnit;
            this.interval = interval;
        }
    }
}
