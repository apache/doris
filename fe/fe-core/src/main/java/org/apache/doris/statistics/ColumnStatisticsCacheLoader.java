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

import org.apache.doris.common.FeConstants;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;

public class ColumnStatisticsCacheLoader extends StatisticsCacheLoader<Optional<ColumnStatistic>> {

    private static final Logger LOG = LogManager.getLogger(ColumnStatisticsCacheLoader.class);

    private static final String QUERY_COLUMN_STATISTICS = "SELECT * FROM " + FeConstants.INTERNAL_DB_NAME
            + "." + StatisticConstants.STATISTIC_TBL_NAME + " WHERE "
            + "id = CONCAT('${tblId}', '-', ${idxId}, '-', '${colId}')";

    @Override
    protected Optional<ColumnStatistic> doLoad(StatisticsCacheKey key) {
        Map<String, String> params = new HashMap<>();
        params.put("tblId", String.valueOf(key.tableId));
        params.put("idxId", String.valueOf(key.idxId));
        params.put("colId", String.valueOf(key.colName));

        List<ColumnStatistic> columnStatistics;
        List<ResultRow> columnResult =
                StatisticsUtil.execStatisticQuery(new StringSubstitutor(params)
                        .replace(QUERY_COLUMN_STATISTICS));
        try {
            columnStatistics = StatisticsUtil.deserializeToColumnStatistics(columnResult);
        } catch (Exception e) {
            LOG.warn("Failed to deserialize column statistics", e);
            throw new CompletionException(e);
        }
        if (CollectionUtils.isEmpty(columnStatistics)) {
            return Optional.empty();
        } else {
            return Optional.of(columnStatistics.get(0));
        }
    }
}
