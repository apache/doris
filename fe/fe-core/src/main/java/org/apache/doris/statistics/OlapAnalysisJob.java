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

import java.util.List;

public class OlapAnalysisJob {



    private List<String> columns;

    private static String collectPartionStatsSQLTemplate =
            " SELECT "
                    + "CONCAT(${tblId}, '-', ${idxId}, '-', '${colId}', '-', ${partId}) AS id, "
                    + "${catalogId} AS catalog_id, "
                    + "${dbId} AS db_id, "
                    + "${tblId} AS tbl_id, "
                    + "${idxId} AS idx_id, "
                    + "'${colId}' AS col_id, "
                    + "${partId} AS part_id, "
                    + "COUNT(1) AS row_count, "
                    + "NDV(`${colName}`) AS ndv, "
                    + "SUM(CASE WHEN `${colName}` IS NULL THEN 1 ELSE 0 END) AS null_count, "
                    + "MIN(`${colName}`) AS min, "
                    + "MAX(`${colName}`) AS max, "
                    + "${dataSizeFunction} AS data_size, "
                    + "NOW() ";


    protected void beforeExecution() {
    }

    public void execute() {
    }

    protected void afterExecution() {

    }

}
