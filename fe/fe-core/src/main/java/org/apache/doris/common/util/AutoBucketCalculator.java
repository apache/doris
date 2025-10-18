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

package org.apache.doris.common.util;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.clone.DynamicPartitionScheduler;
import org.apache.doris.common.Pair;
import org.apache.doris.rpc.RpcException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Utility class for calculating auto bucket numbers for partitions.
 * This class provides a unified interface for auto bucket calculation
 * used by both DynamicPartitionScheduler and PartitionExprUtil.
 */
public class AutoBucketCalculator {
    private static final Logger LOG = LogManager.getLogger(AutoBucketCalculator.class);

    /**
     * Context for auto bucket calculation
     */
    public static class AutoBucketContext {
        private final OlapTable table;
        private final String partitionName;
        private final String nowPartitionName;
        private final boolean executeFirstTime;
        private final int defaultBuckets;

        public AutoBucketContext(OlapTable table, String partitionName, String nowPartitionName,
                                boolean executeFirstTime, int defaultBuckets) {
            this.table = table;
            this.partitionName = partitionName;
            this.nowPartitionName = nowPartitionName;
            this.executeFirstTime = executeFirstTime;
            this.defaultBuckets = defaultBuckets;
        }

        public OlapTable getTable() {
            return table;
        }

        public String getPartitionName() {
            return partitionName;
        }

        public String getNowPartitionName() {
            return nowPartitionName;
        }

        public boolean isExecuteFirstTime() {
            return executeFirstTime;
        }

        public int getDefaultBuckets() {
            return defaultBuckets;
        }
    }

    /**
     * Result of auto bucket calculation
     */
    public static class AutoBucketResult {
        private final int buckets;
        private final int previousBuckets;
        private final boolean success;
        private final String errorMessage;

        private AutoBucketResult(int buckets, int previousBuckets, boolean success, String errorMessage) {
            this.buckets = buckets;
            this.previousBuckets = previousBuckets;
            this.success = success;
            this.errorMessage = errorMessage;
        }

        public static AutoBucketResult success(int buckets, int previousBuckets) {
            return new AutoBucketResult(buckets, previousBuckets, true, null);
        }

        public static AutoBucketResult fallback(int defaultBuckets, String reason) {
            return new AutoBucketResult(defaultBuckets, 0, false, reason);
        }

        public int getBuckets() {
            return buckets;
        }

        public int getPreviousBuckets() {
            return previousBuckets;
        }

        public boolean isSuccess() {
            return success;
        }

        public String getErrorMessage() {
            return errorMessage;
        }
    }

    /**
     * Calculate auto bucket number for a partition.
     * This is the unified method that replaces the logic in both
     * DynamicPartitionScheduler.getBucketsNum and PartitionExprUtil.getAddPartitionClauseFromPartitionValues
     *
     * @param context the context for auto bucket calculation
     * @return AutoBucketResult containing bucket numbers and calculation status
     */
    public static AutoBucketResult calculateAutoBuckets(AutoBucketContext context) {
        OlapTable table = context.getTable();
        String partitionName = context.getPartitionName();
        String nowPartitionName = context.getNowPartitionName();
        boolean executeFirstTime = context.isExecuteFirstTime();
        int defaultBuckets = context.getDefaultBuckets();

        // if execute first time or not auto bucket, use default buckets
        if (!table.isAutoBucket() || executeFirstTime) {
            return AutoBucketResult.fallback(defaultBuckets,
                executeFirstTime ? "executeFirstTime" : "not auto bucket table");
        }

        // Get historical partitions
        List<Partition> partitions = DynamicPartitionScheduler.getHistoricalPartitions(table, nowPartitionName);

        // Get visible versions with error handling
        List<Long> visibleVersions;
        try {
            visibleVersions = Partition.getVisibleVersions(partitions);
        } catch (RpcException e) {
            LOG.warn("auto bucket get visible version fail, table: [{}-{}], partition: {}, use default buckets: {}",
                    table.getName(), table.getId(), partitionName, defaultBuckets, e);
            return AutoBucketResult.fallback(defaultBuckets, "RpcException: " + e.getMessage());
        }

        // Check if visible versions match partitions
        if (visibleVersions == null || partitions.size() != visibleVersions.size()) {
            LOG.warn(
                    "auto bucket visible versions mismatch, table: [{}-{}], partition: {}, "
                            + "partitions size: {}, visible versions size: {}, use default buckets: {}",
                    table.getName(), table.getId(), partitionName, partitions.size(),
                    visibleVersions != null ? visibleVersions.size() : 0, defaultBuckets);
            return AutoBucketResult.fallback(defaultBuckets, "visible versions mismatch");
        }

        // Filter partitions with data
        List<Partition> hasDataPartitions = DynamicPartitionScheduler.filterDataPartitions(partitions, visibleVersions);
        if (hasDataPartitions.isEmpty()) {
            LOG.info(
                    "auto bucket use default buckets due to all partitions no data, table: [{}-{}], "
                            + "partition: {}, buckets num: {}",
                    table.getName(), table.getId(), partitionName, defaultBuckets);
            return AutoBucketResult.fallback(defaultBuckets, "no data partitions");
        }

        // Calculate buckets based on historical data
        Pair<Integer, Integer> calc = DynamicPartitionScheduler.calculateBuckets(hasDataPartitions);
        int candidateBuckets = calc.first;
        int previousBuckets = calc.second;

        return AutoBucketResult.success(candidateBuckets, previousBuckets);
    }

    /**
     * Calculate auto bucket number and apply bounds checking.
     * This method combines calculateAutoBuckets with checkAndFixAutoBucketCalcNumIsValid.
     *
     * @param context the context for auto bucket calculation
     * @return final bucket number after bounds checking
     */
    public static int calculateAutoBucketsWithBoundsCheck(AutoBucketContext context) {
        AutoBucketResult result = calculateAutoBuckets(context);

        if (!result.isSuccess()) {
            return result.getBuckets(); // return default buckets
        }

        int candidateBuckets = result.getBuckets();
        int previousBuckets = result.getPreviousBuckets();

        // Apply bounds checking
        int adjustedBuckets = DynamicPartitionScheduler.checkAndFixAutoBucketCalcNumIsValid(
                candidateBuckets, previousBuckets, context.getTable().getName(), context.getPartitionName());

        return adjustedBuckets > 0 ? adjustedBuckets : candidateBuckets;
    }
}
