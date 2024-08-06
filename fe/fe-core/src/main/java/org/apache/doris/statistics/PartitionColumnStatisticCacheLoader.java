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

import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class PartitionColumnStatisticCacheLoader extends
        BasicAsyncCacheLoader<PartitionColumnStatisticCacheKey, Optional<PartitionColumnStatistic>> {

    private static final Logger LOG = LogManager.getLogger(PartitionColumnStatisticCacheLoader.class);

    @Override
    protected Optional<PartitionColumnStatistic> doLoad(PartitionColumnStatisticCacheKey key) {
        Optional<PartitionColumnStatistic> partitionStatistic;
        try {
            partitionStatistic = loadFromPartitionStatsTable(key);
        } catch (Throwable t) {
            LOG.warn("Failed to load stats for column [Catalog:{}, DB:{}, Table:{}, Part:{}, Column:{}],"
                    + "Reason: {}", key.catalogId, key.dbId, key.tableId, key.partId, key.colName, t.getMessage());
            if (LOG.isDebugEnabled()) {
                LOG.debug(t);
            }
            return null;
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

    private Optional<PartitionColumnStatistic> loadFromPartitionStatsTable(PartitionColumnStatisticCacheKey key)
            throws IOException {
        String partName = "'" + StatisticsUtil.escapeSQL(key.partId) + "'";
        List<ResultRow> partitionResults = StatisticsRepository.loadPartitionColumnStats(
                key.catalogId, key.dbId, key.tableId, key.idxId, partName, key.colName);
        return Optional.ofNullable(StatisticsUtil.deserializeToPartitionStatistics(partitionResults));
    }
}
