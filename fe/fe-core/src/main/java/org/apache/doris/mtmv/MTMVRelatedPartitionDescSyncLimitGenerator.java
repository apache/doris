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

import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeAcquire;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeArithmetic;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeExtractAndTransform;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

/**
 * Only focus on partial partitions of related tables
 */
public class MTMVRelatedPartitionDescSyncLimitGenerator implements MTMVRelatedPartitionDescGeneratorService {

    @Override
    public void apply(MTMVPartitionInfo mvPartitionInfo, Map<String, String> mvProperties,
            RelatedPartitionDescResult lastResult) throws AnalysisException {
        Map<String, PartitionItem> partitionItems = lastResult.getItems();
        MTMVPartitionSyncConfig config = generateMTMVPartitionSyncConfigByProperties(mvProperties);
        if (config.getSyncLimit() <= 0) {
            return;
        }
        long nowTruncSubSec = getNowTruncSubSec(config.getTimeUnit(), config.getSyncLimit());
        Optional<String> dateFormat = config.getDateFormat();
        Map<String, PartitionItem> res = Maps.newHashMap();
        int relatedColPos = mvPartitionInfo.getRelatedColPos();
        for (Entry<String, PartitionItem> entry : partitionItems.entrySet()) {
            if (entry.getValue().isGreaterThanSpecifiedTime(relatedColPos, dateFormat, nowTruncSubSec)) {
                res.put(entry.getKey(), entry.getValue());
            }
        }
        lastResult.setItems(res);
    }

    /**
     * Generate MTMVPartitionSyncConfig based on mvProperties
     *
     * @param mvProperties
     * @return
     */
    public MTMVPartitionSyncConfig generateMTMVPartitionSyncConfigByProperties(
            Map<String, String> mvProperties) {
        int syncLimit = StringUtils.isEmpty(mvProperties.get(PropertyAnalyzer.PROPERTIES_PARTITION_SYNC_LIMIT)) ? -1
                : Integer.parseInt(mvProperties.get(PropertyAnalyzer.PROPERTIES_PARTITION_SYNC_LIMIT));
        MTMVPartitionSyncTimeUnit timeUnit =
                StringUtils.isEmpty(mvProperties.get(PropertyAnalyzer.PROPERTIES_PARTITION_TIME_UNIT))
                        ? MTMVPartitionSyncTimeUnit.DAY : MTMVPartitionSyncTimeUnit
                        .valueOf(mvProperties.get(PropertyAnalyzer.PROPERTIES_PARTITION_TIME_UNIT).toUpperCase());
        Optional<String> dateFormat =
                StringUtils.isEmpty(mvProperties.get(PropertyAnalyzer.PROPERTIES_PARTITION_DATE_FORMAT))
                        ? Optional.empty()
                        : Optional.of(mvProperties.get(PropertyAnalyzer.PROPERTIES_PARTITION_DATE_FORMAT));
        return new MTMVPartitionSyncConfig(syncLimit, timeUnit, dateFormat);
    }

    /**
     * Obtain the minimum second from `syncLimit` `timeUnit` ago
     *
     * @param timeUnit
     * @param syncLimit
     * @return
     * @throws AnalysisException
     */
    public long getNowTruncSubSec(MTMVPartitionSyncTimeUnit timeUnit, int syncLimit)
            throws AnalysisException {
        if (syncLimit < 1) {
            throw new AnalysisException("Unexpected syncLimit, syncLimit: " + syncLimit);
        }
        // get current time
        Expression now = DateTimeAcquire.now();
        if (!(now instanceof DateTimeLiteral)) {
            throw new AnalysisException("now() should return DateTimeLiteral, now: " + now);
        }
        DateTimeLiteral nowLiteral = (DateTimeLiteral) now;
        // date trunc
        now = DateTimeExtractAndTransform
                .dateTrunc(nowLiteral, new VarcharLiteral(timeUnit.name()));
        if (!(now instanceof DateTimeLiteral)) {
            throw new AnalysisException("dateTrunc() should return DateTimeLiteral, now: " + now);
        }
        nowLiteral = (DateTimeLiteral) now;
        // date sub
        if (syncLimit > 1) {
            nowLiteral = dateSub(nowLiteral, timeUnit, syncLimit - 1);
        }
        return ((IntegerLiteral) DateTimeExtractAndTransform.unixTimestamp(nowLiteral)).getValue();
    }


    private DateTimeLiteral dateSub(
            org.apache.doris.nereids.trees.expressions.literal.DateLiteral date, MTMVPartitionSyncTimeUnit timeUnit,
            int num)
            throws AnalysisException {
        IntegerLiteral integerLiteral = new IntegerLiteral(num);
        Expression result;
        switch (timeUnit) {
            case DAY:
                result = DateTimeArithmetic.dateSub(date, integerLiteral);
                break;
            case YEAR:
                result = DateTimeArithmetic.yearsSub(date, integerLiteral);
                break;
            case MONTH:
                result = DateTimeArithmetic.monthsSub(date, integerLiteral);
                break;
            default:
                throw new AnalysisException(
                        "async materialized view partition limit not support timeUnit: " + timeUnit.name());
        }
        if (!(result instanceof DateTimeLiteral)) {
            throw new AnalysisException("sub() should return  DateTimeLiteral, result: " + result);
        }
        return (DateTimeLiteral) result;
    }
}
