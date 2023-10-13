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
import org.apache.doris.common.DdlException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;

public class TableStatisticsCacheLoader extends StatisticsCacheLoader<Optional<TableStatistic>> {

    private static final Logger LOG = LogManager.getLogger(TableStatisticsCacheLoader.class);

    @Override
    protected Optional<TableStatistic> doLoad(StatisticsCacheKey key) {
        try {
            TableStatistic tableStatistic = StatisticsRepository.fetchTableLevelStats(key.tableId);
            if (tableStatistic != TableStatistic.UNKNOWN) {
                return Optional.of(tableStatistic);
            }
        } catch (DdlException e) {
            LOG.debug("Fail to get table line number from table_statistics table. "
                    + "Will try to get from data source.", e);
        }
        // Get row count by call TableIf interface getRowCount
        // when statistic table doesn't contain a record for this table.
        try {
            TableIf table = Env.getCurrentEnv().getCatalogMgr().getCatalog(key.catalogId)
                    .getDbOrDdlException(key.dbId).getTableOrAnalysisException(key.tableId);
            long rowCount = table.getRowCount();
            long lastAnalyzeTimeInMs = System.currentTimeMillis();
            String updateTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(lastAnalyzeTimeInMs));
            return Optional.of(new TableStatistic(rowCount, lastAnalyzeTimeInMs, updateTime));
        } catch (Exception e) {
            LOG.warn(String.format("Fail to get row count for table %d", key.tableId), e);
        }
        return Optional.empty();
    }
}
