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

public class HistogramCacheLoader extends BasicAsyncCacheLoader<StatisticsCacheKey, Optional<Histogram>> {

    private static final Logger LOG = LogManager.getLogger(HistogramCacheLoader.class);

    private static final String QUERY_HISTOGRAM_STATISTICS = "SELECT * FROM " + FeConstants.INTERNAL_DB_NAME
            + "." + StatisticConstants.HISTOGRAM_TBL_NAME + " WHERE "
            + "id = CONCAT('${tblId}', '-', ${idxId}, '-', '${colId}')";

    @Override
    protected Optional<Histogram> doLoad(StatisticsCacheKey key) {
        List<Histogram> histogramStatistics;
        Map<String, String> params = new HashMap<>();
        params.put("tblId", String.valueOf(key.tableId));
        params.put("idxId", String.valueOf(key.idxId));
        params.put("colId", String.valueOf(key.colName));

        List<ResultRow> histogramResult =
                StatisticsUtil.execStatisticQuery(new StringSubstitutor(params)
                        .replace(QUERY_HISTOGRAM_STATISTICS));
        try {
            histogramStatistics = StatisticsUtil.deserializeToHistogramStatistics(histogramResult);
        } catch (Exception e) {
            LOG.warn("Failed to deserialize histogram statistics", e);
            throw new CompletionException(e);
        }
        if (!CollectionUtils.isEmpty(histogramStatistics)) {
            return Optional.of(histogramStatistics.get(0));
        }
        return Optional.empty();
    }
}
