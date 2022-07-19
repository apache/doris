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

import org.apache.doris.analysis.SelectStmt;

import java.util.List;

/**
 * A statistics task that collects statistics by executing query.
 * The results of the query will be returned as @StatisticsTaskResult.
 */
public class SQLStatisticsTask extends StatisticsTask {
    private SelectStmt query;

    public SQLStatisticsTask(long jobId, List<StatisticsDesc> statsDescs) {
        super(jobId, statsDescs);
    }

    @Override
    public StatisticsTaskResult call() throws Exception {
        // TODO
        // step1: construct query by statsDescList
        constructQuery();
        // step2: execute query
        // the result should be sequence by @statsTypeList
        List<String> queryResultList = executeQuery(query);
        // step3: construct StatisticsTaskResult by query result
        constructTaskResult(queryResultList);
        return null;
    }

    protected void constructQuery() {
        // TODO
        // step1: construct FROM by @granularityDesc
        // step2: construct SELECT LIST by @statsTypeList
    }

    protected List<String> executeQuery(SelectStmt query) {
        // TODO (ML)
        return null;
    }

    protected StatisticsTaskResult constructTaskResult(List<String> queryResultList) {
        // TODO
        return null;
    }
}
