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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeAcquire;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeArithmetic;
import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeExtractAndTransform;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

/**
 * Only focus on partial partitions of related tables
 */
public class MTMVRelatedPartitionDescSyncLimitGenerator implements MTMVRelatedPartitionDescGeneratorService {

    @Override
    public void apply(MTMVPartitionInfo mvPartitionInfo, Map<String, String> mvProperties,
            RelatedPartitionDescResult lastResult, List<Column> partitionColumns,
                      Map<List<String>, Set<String>> queryUsedPartitionMap) throws AnalysisException {
        Map<MTMVRelatedTableIf, Map<String, PartitionItem>> partitionItems = lastResult.getItems();
        MTMVPartitionSyncConfig config = generateMTMVPartitionSyncConfigByProperties(mvProperties);
        if (config.getSyncLimit() <= 0) {
            return;
        }
        long nowTruncSubSec = getNowTruncSubSec(config.getTimeUnit(), config.getSyncLimit());
        Optional<String> dateFormat = config.getDateFormat();
        Map<MTMVRelatedTableIf, Map<String, PartitionItem>> res = Maps.newHashMap();
        for (Entry<MTMVRelatedTableIf, Map<String, PartitionItem>> entry : partitionItems.entrySet()) {
            if (hasDefaultListPartition(entry.getValue())) {
                // The window is what makes a refresh read fewer base partitions than the MV partition's own
                // key range covers. It cannot be applied to a table whose list partitions have a default
                // one: that partition takes the rows no other partition of the table claims, and which of
                // those rows belong to an MV partition is decided by their own key rather than by the
                // partition they were placed in -- ADD PARTITION can claim a key without moving the rows the
                // default partition already holds under it, so which side of the window a row is on is not
                // something a predicate on the partition columns can say. A read that is to keep only part of
                // the MV partition's key range cannot be written for such a table, so the key range is kept
                // whole and the mapping names every partition it covers, which is what the refresh reads for
                // it. What is given up is the window for this table, not the correspondence between the read
                // and the record.
                res.put(entry.getKey(), entry.getValue());
                continue;
            }
            Map<String, PartitionItem> onePctRes = Maps.newHashMap();
            int relatedColPos = mvPartitionInfo.getPctColPos(entry.getKey());
            for (Entry<String, PartitionItem> onePctEntry : entry.getValue().entrySet()) {
                if (onePctEntry.getValue().isGreaterThanSpecifiedTime(relatedColPos, dateFormat, nowTruncSubSec)) {
                    onePctRes.put(onePctEntry.getKey(), onePctEntry.getValue());
                }
            }
            res.put(entry.getKey(), onePctRes);
        }

        lastResult.setItems(res);
    }

    /**
     * Whether these partitions include a list partition's default one, the partition that takes the rows no
     * other partition of the table claims. Read from the items rather than from the table: the caller
     * already holds every partition of it, and this is a fact about the item rather than about metadata
     * that could change under a lock not held here.
     */
    private static boolean hasDefaultListPartition(Map<String, PartitionItem> partitionItems) {
        for (PartitionItem item : partitionItems.values()) {
            if (item.isDefaultPartition()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Generate MTMVPartitionSyncConfig based on mvProperties
     *
     * @param mvProperties
     * @return
     */
    public static MTMVPartitionSyncConfig generateMTMVPartitionSyncConfigByProperties(
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
        if (!(now instanceof DateTimeV2Literal)) {
            throw new AnalysisException("now() should return DateTimeV2Literal, now: " + now);
        }
        DateTimeV2Literal nowLiteral = (DateTimeV2Literal) now;
        // date trunc
        now = DateTimeExtractAndTransform
                .dateTrunc(nowLiteral, new VarcharLiteral(timeUnit.name()));
        if (!(now instanceof DateTimeV2Literal)) {
            throw new AnalysisException("dateTrunc() should return DateTimeV2Literal, now: " + now);
        }
        nowLiteral = (DateTimeV2Literal) now;
        // date sub
        if (syncLimit > 1) {
            nowLiteral = dateSub(nowLiteral, timeUnit, syncLimit - 1);
        }
        return ZonedDateTime.of(nowLiteral.toJavaDateType(), TimeUtils.getDorisZoneId()).toEpochSecond();
    }

    private DateTimeV2Literal dateSub(DateTimeV2Literal date, MTMVPartitionSyncTimeUnit timeUnit, int num)
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
        if (!(result instanceof DateTimeV2Literal)) {
            throw new AnalysisException("sub() should return  DateTimeLiteral, result: " + result);
        }
        return (DateTimeV2Literal) result;
    }
}
