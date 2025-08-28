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

package org.apache.doris.statistics.hbo;

/**
 * Plan statistics match strategy.
 */
public class PlanStatisticsMatchStrategy {
    private final boolean needMatchPartition;
    private final boolean needMatchPartitionColumnPredicate;
    private final boolean needMatchOtherColumnPredicate;

    /**
     * Full matching: the same partition ids, the same predicates on partition column,
     *                the same other predicate with the same constant.
     * Mainly for accurate matching under RETRY.
     * By design, the accurate entry in the recentRunEntries will have only ONE entry.
     */
    public static final PlanStatisticsMatchStrategy FULL_MATCH = new PlanStatisticsMatchStrategy(
            true, true, true);

    /**
     * Partition and other predicate matching: the same partition ids, the same other predicate with the same constant.
     * Will NOT ensure the same predicates on partition column.
     */
    public static final PlanStatisticsMatchStrategy PARTITION_AND_OTHER_MATCH = new PlanStatisticsMatchStrategy(
            true, false, true);

    /**
     * Partition only matching: the same partition ids.
     * Will NOT ensure the same predicates on partition column and the same other predicates.
     * Will ONLY be used when the 'enable_hbo_nonstrict_matching_mode' session variable is open.
     */
    public static final PlanStatisticsMatchStrategy PARTITION_ONLY_MATCH = new PlanStatisticsMatchStrategy(
            true, false, false);

    /**
     * Other predicates only matching: the same partition ids.
     * Will ensure the same predicates on other predicates.
     * Will ONLY be used for non-partition table matching.
     */
    public static final PlanStatisticsMatchStrategy OTHER_ONLY_MATCH = new PlanStatisticsMatchStrategy(
            false, false, true);

    public PlanStatisticsMatchStrategy(boolean needMatchPartition,
            boolean needMatchPartitionColumnPredicate, boolean needMatchOtherColumnPredicate) {
        this.needMatchPartition = needMatchPartition;
        this.needMatchPartitionColumnPredicate = needMatchPartitionColumnPredicate;
        this.needMatchOtherColumnPredicate = needMatchOtherColumnPredicate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PlanStatisticsMatchStrategy other = (PlanStatisticsMatchStrategy) o;
        return needMatchPartition == other.needMatchPartition
                && needMatchPartitionColumnPredicate == other.needMatchPartitionColumnPredicate
                && needMatchOtherColumnPredicate == other.needMatchOtherColumnPredicate;
    }

    public boolean isNeedMatchOtherColumnPredicate() {
        return needMatchOtherColumnPredicate;
    }

    public boolean isNeedMatchPartition() {
        return needMatchPartition;
    }

    public boolean isNeedMatchPartitionColumnPredicate() {
        return needMatchPartitionColumnPredicate;
    }
}
