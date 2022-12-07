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

import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.Config;

import org.apache.commons.lang.NotImplementedException;

public class HMSAnalysisTask extends BaseAnalysisTask {

    protected HMSExternalTable table;

    public HMSAnalysisTask(AnalysisTaskScheduler analysisTaskScheduler, AnalysisTaskInfo info) {
        super(analysisTaskScheduler, info);
        table = (HMSExternalTable) tbl;
    }

    /**
     * Collect the column level stats for external table through metadata.
     */
    protected void getColumnStatsByMeta() throws Exception {
        throw new NotImplementedException();
    }

    /**
     * Collect the stats for external table through sql.
     * @return ColumnStatistics
     */
    protected void getColumnStatsBySql() {
        throw new NotImplementedException();
    }

    @Override
    public void execute() throws Exception {
        if (Config.collect_external_table_stats_by_sql) {
            getColumnStatsBySql();
        } else {
            getColumnStatsByMeta();
        }
    }
}
