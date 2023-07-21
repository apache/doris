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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;

public class ColumnStatisticsCacheLoader extends StatisticsCacheLoader<Optional<ColumnStatistic>> {

    private static final Logger LOG = LogManager.getLogger(ColumnStatisticsCacheLoader.class);

    @Override
    protected Optional<ColumnStatistic> doLoad(StatisticsCacheKey key) {
        // Load from statistics table.
        Optional<ColumnStatistic> columnStatistic = loadFromStatsTable(key.tableId,
                key.idxId, key.colName);
        if (columnStatistic.isPresent()) {
            return columnStatistic;
        }
        // Load from data source metadata
        try {
            TableIf table = Env.getCurrentEnv().getCatalogMgr().getCatalog(key.catalogId)
                    .getDbOrMetaException(key.dbId).getTableOrMetaException(key.tableId);
            columnStatistic = table.getColumnStatistic(key.colName);
        } catch (Exception e) {
            LOG.warn(String.format("Exception to get column statistics by metadata. [Catalog:%d, DB:%d, Table:%d]",
                    key.catalogId, key.dbId, key.tableId), e);
        }
        return columnStatistic;
    }

    private Optional<ColumnStatistic> loadFromStatsTable(long tableId, long idxId, String colName) {
        List<ResultRow> columnResults = StatisticsRepository.loadColStats(tableId, idxId, colName);
        ColumnStatistic columnStatistics;
        try {
            columnStatistics = StatisticsUtil.deserializeToColumnStatistics(columnResults);
        } catch (Exception e) {
            LOG.warn("Exception to deserialize column statistics", e);
            return Optional.empty();
        }
        if (columnStatistics == null) {
            return Optional.empty();
        } else {
            return Optional.of(columnStatistics);
        }
    }
}
