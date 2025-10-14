/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("ai3") {
    String db = context.config.getDbNameByFile(new File(context.file.parent))
    if (isCloudMode()) {
        return
    }
    sql "use ${db}"
    sql 'set enable_nereids_planner=true'
    sql 'set enable_nereids_distribute_planner=false'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set exec_mem_limit=21G'
    sql 'set be_number_for_test=3'
    sql 'set parallel_pipeline_task_num=8; '
    sql 'set forbid_unknown_col_stats=true'
    sql 'set enable_nereids_timeout = false'
    sql 'set enable_runtime_filter_prune=false'
    sql 'set runtime_filter_type=8'
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"

    qt_shape """
    explain shape plan
    SELECT
    d.d_year,
    d.d_moy,
    SUM(ss.ss_sales_price) AS monthly_sales,
    SUM(SUM(ss.ss_sales_price)) OVER (PARTITION BY d.d_year) AS yearly_sales,
            SUM(ss.ss_sales_price)/SUM(SUM(ss.ss_sales_price)) OVER (PARTITION BY d.d_year)*100 AS pct_of_year,
        LAG(SUM(ss.ss_sales_price), 12) OVER (ORDER BY d.d_year, d.d_moy) AS prev_year_same_month,
        CASE
            WHEN LAG(SUM(ss.ss_sales_price), 12) OVER (ORDER BY d.d_year, d.d_moy) > 0
        THEN (SUM(ss.ss_sales_price)-LAG(SUM(ss.ss_sales_price), 12) OVER (ORDER BY d.d_year, d.d_moy))
                /LAG(SUM(ss.ss_sales_price), 12) OVER (ORDER BY d.d_year, d.d_moy)*100
            ELSE NULL
            END AS yoy_growth_pct
    FROM
        store_sales ss
            JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
    WHERE
        d.d_year BETWEEN 1998 AND 2002
    GROUP BY
        d.d_year, d.d_moy
    ORDER BY
        d.d_year, d.d_moy LIMIT 100;
        """
}