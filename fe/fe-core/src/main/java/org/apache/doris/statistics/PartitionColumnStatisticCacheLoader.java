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

package org.apache.doris.statistics;

import org.apache.doris.qe.InternalQueryExecutionException;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;

public class PartitionColumnStatisticCacheLoader extends
        BasicAsyncCacheLoader<PartitionColumnStatisticCacheKey, Optional<PartitionColumnStatistic>> {

    private static final Logger LOG = LogManager.getLogger(PartitionColumnStatisticCacheLoader.class);

    @Override
    protected Optional<PartitionColumnStatistic> doLoad(PartitionColumnStatisticCacheKey key) {
        Optional<PartitionColumnStatistic> partitionStatistic = Optional.empty();
        try {
            partitionStatistic = loadFromPartitionStatsTable(key);
        } catch (Throwable t) {
            LOG.warn("Failed to load stats for column [Catalog:{}, DB:{}, Table:{}, Part:{}, Column:{}],"
                    + "Reason: {}", key.catalogId, key.dbId, key.tableId, key.partId, key.colName, t.getMessage());
            if (LOG.isDebugEnabled()) {
                LOG.debug(t);
            }
        }
        if (partitionStatistic.isPresent()) {
            // For non-empty table, return UNKNOWN if we can't collect ndv value.
            // Because inaccurate ndv is very misleading.
            PartitionColumnStatistic stats = partitionStatistic.get();
            if (stats.count > 0 && stats.ndv.estimateCardinality() == 0 && stats.count != stats.numNulls) {
                partitionStatistic = Optional.of(PartitionColumnStatistic.UNKNOWN);
            }
        }
        return partitionStatistic;
    }

    private Optional<PartitionColumnStatistic> loadFromPartitionStatsTable(PartitionColumnStatisticCacheKey key) {
        List<ResultRow> partitionResults;
        try {
            String partName = "'" + StatisticsUtil.escapeSQL(key.partId) + "'";
            partitionResults = StatisticsRepository.loadPartitionColumnStats(
                key.catalogId, key.dbId, key.tableId, key.idxId, partName, key.colName);
        } catch (InternalQueryExecutionException e) {
            LOG.info("Failed to load stats for table {} column {}. Reason:{}",
                    key.tableId, key.colName, e.getMessage());
            return Optional.empty();
        }
        PartitionColumnStatistic partitionStatistic;
        try {
            partitionStatistic = StatisticsUtil.deserializeToPartitionStatistics(partitionResults);
        } catch (Exception e) {
            LOG.warn("Exception to deserialize partition statistics", e);
            return Optional.empty();
        }
        return Optional.ofNullable(partitionStatistic);
    }
}
