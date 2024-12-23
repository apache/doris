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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;

public class ColumnStatisticsCacheLoader extends BasicAsyncCacheLoader<StatisticsCacheKey, Optional<ColumnStatistic>> {

    private static final Logger LOG = LogManager.getLogger(ColumnStatisticsCacheLoader.class);

    @Override
    protected Optional<ColumnStatistic> doLoad(StatisticsCacheKey key) {
        Optional<ColumnStatistic> columnStatistic;
        try {
            // Load from statistics table.
            columnStatistic = loadFromStatsTable(key);
            if (!columnStatistic.isPresent()) {
                // Load from data source metadata
                TableIf table = StatisticsUtil.findTable(key.catalogId, key.dbId, key.tableId);
                columnStatistic = table.getColumnStatistic(key.colName);
            }
        } catch (Throwable t) {
            LOG.info("Failed to load stats for column [Catalog:{}, DB:{}, Table:{}, Column:{}], Reason: {}",
                    key.catalogId, key.dbId, key.tableId, key.colName, t.getMessage());
            if (LOG.isDebugEnabled()) {
                LOG.debug(t);
            }
            return null;
        }
        return columnStatistic;
    }

    private Optional<ColumnStatistic> loadFromStatsTable(StatisticsCacheKey key) {
        List<ResultRow> columnResults = StatisticsRepository.loadColStats(
                    key.catalogId, key.dbId, key.tableId, key.idxId, key.colName);
        ColumnStatistic columnStatistics = StatisticsUtil.deserializeToColumnStatistics(columnResults);
        if (columnStatistics == null) {
            return Optional.empty();
        } else {
            return Optional.of(columnStatistics);
        }
    }
}
