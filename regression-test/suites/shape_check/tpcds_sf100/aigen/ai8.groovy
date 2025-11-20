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

suite("ai8") {
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
    i.i_item_sk,
    i.i_item_id,
    SUM(ss.ss_quantity) AS total_sold,
    inv.inv_quantity_on_hand AS current_stock,
    CASE
        WHEN inv.inv_quantity_on_hand = 0 THEN NULL
        ELSE ROUND(SUM(ss.ss_quantity) / inv.inv_quantity_on_hand, 2)
        END AS turnover_rate
    FROM store_sales ss
            JOIN item i ON ss.ss_item_sk = i.i_item_sk
            JOIN inventory inv
                ON ss.ss_item_sk = inv.inv_item_sk AND ss.ss_store_sk = inv.inv_warehouse_sk
    GROUP BY i.i_item_sk, i.i_item_id, inv.inv_quantity_on_hand
    ORDER BY turnover_rate DESC
        LIMIT 100;
        """
}