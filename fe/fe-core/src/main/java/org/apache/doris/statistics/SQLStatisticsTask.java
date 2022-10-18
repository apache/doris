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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.InvalidFormatException;
import org.apache.doris.statistics.StatisticsTaskResult.TaskResult;
import org.apache.doris.statistics.StatsGranularity.Granularity;
import org.apache.doris.statistics.util.InternalQuery;
import org.apache.doris.statistics.util.InternalQueryResult;
import org.apache.doris.statistics.util.InternalQueryResult.ResultRow;
import org.apache.doris.statistics.util.InternalSqlTemplate;
import org.apache.doris.statistics.util.InternalSqlTemplate.QueryType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * A statistics task that collects statistics by executing query.
 * The results of the query will be returned as @StatisticsTaskResult.
 */
public class SQLStatisticsTask extends StatisticsTask {
    protected QueryType queryType = QueryType.FULL;

    protected String statement;

    public SQLStatisticsTask(long jobId, List<StatisticsDesc> statsDescs) {
        super(jobId, statsDescs);
    }

    @Override
    public StatisticsTaskResult call() throws Exception {
        checkStatisticsDesc();
        List<TaskResult> taskResults = Lists.newArrayList();

        for (StatisticsDesc statsDesc : statsDescs) {
            statement = constructQuery(statsDesc);
            TaskResult taskResult = executeQuery(statsDesc);
            taskResults.add(taskResult);
            LOG.info("Collected statistics successfully by SQL: {}", statement);
        }

        return new StatisticsTaskResult(taskResults);
    }

    protected String constructQuery(StatisticsDesc statsDesc) throws DdlException,
            InvalidFormatException {
        Map<String, String> params = getQueryParams(statsDesc);

        List<StatsType> statsTypes = statsDesc.getStatsTypes();
        StatsType type = statsTypes.get(0);

        StatsGranularity statsGranularity = statsDesc.getStatsGranularity();
        Granularity granularity = statsGranularity.getGranularity();
        boolean nonPartitioned = granularity != Granularity.PARTITION;

        switch (type) {
            case ROW_COUNT:
                return nonPartitioned ? InternalSqlTemplate.buildStatsRowCountSql(params, queryType)
                        : InternalSqlTemplate.buildStatsPartitionRowCountSql(params, queryType);
            case NUM_NULLS:
                return nonPartitioned ? InternalSqlTemplate.buildStatsNumNullsSql(params, queryType)
                        : InternalSqlTemplate.buildStatsPartitionNumNullsSql(params, queryType);
            case MAX_SIZE:
            case AVG_SIZE:
                return nonPartitioned ? InternalSqlTemplate.buildStatsMaxAvgSizeSql(params, queryType)
                        : InternalSqlTemplate.buildStatsPartitionMaxAvgSizeSql(params, queryType);
            case NDV:
            case MAX_VALUE:
            case MIN_VALUE:
                return nonPartitioned ? InternalSqlTemplate.buildStatsMinMaxNdvValueSql(params, queryType)
                        : InternalSqlTemplate.buildStatsPartitionMinMaxNdvValueSql(params, queryType);
            case DATA_SIZE:
            default:
                throw new DdlException("Unsupported statistics type: " + type);
        }
    }

    protected TaskResult executeQuery(StatisticsDesc statsDesc) throws Exception {
        StatsGranularity granularity = statsDesc.getStatsGranularity();
        List<StatsType> statsTypes = statsDesc.getStatsTypes();
        StatsCategory category = statsDesc.getStatsCategory();

        String dbName = Env.getCurrentInternalCatalog()
                .getDbOrDdlException(category.getDbId()).getFullName();
        InternalQuery query = new InternalQuery(dbName, statement);
        InternalQueryResult queryResult = query.query();
        List<ResultRow> resultRows = queryResult.getResultRows();

        if (resultRows != null && resultRows.size() == 1) {
            ResultRow resultRow = resultRows.get(0);
            List<String> columns = resultRow.getColumns();
            TaskResult result = createNewTaskResult(category, granularity);

            if (columns.size() == statsTypes.size()) {
                for (int i = 0; i < columns.size(); i++) {
                    StatsType statsType = StatsType.fromString(columns.get(i));
                    result.getStatsTypeToValue().put(statsType, resultRow.getString(i));
                }
                return result;
            }
        }

        // Statistics statements are executed singly and return only one row data
        throw new DdlException("Statistics query result is incorrect, statement: "
                + statement + " queryResult: " + queryResult);
    }

    protected Map<String, String> getQueryParams(StatisticsDesc statsDesc) throws DdlException {
        StatsCategory category = statsDesc.getStatsCategory();
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(category.getDbId());
        Table table = db.getTableOrDdlException(category.getTableId());

        Map<String, String> params = Maps.newHashMap();
        params.put(InternalSqlTemplate.TABLE, table.getName());
        params.put(InternalSqlTemplate.PARTITION, category.getPartitionName());
        params.put(InternalSqlTemplate.COLUMN, category.getColumnName());

        return params;
    }
}
