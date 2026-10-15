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

suite("merge_union_output_mapping", "p0") {
    sql "DROP TABLE IF EXISTS merge_union_output_mapping"
    sql """CREATE TABLE merge_union_output_mapping (
        id INT, campaign VARCHAR(40), country VARCHAR(40), dt VARCHAR(40)
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES ('replication_num' = '1')"""
    sql "INSERT INTO merge_union_output_mapping VALUES (1, 'AD_A', 'FR', '2026-01-01')"
    sql "SET enable_sql_cache=false"
    order_qt_country """WITH sa AS (
    SELECT CAST(dt AS DATE) AS dt, campaign, country, campaign AS tag FROM merge_union_output_mapping
    UNION ALL
    SELECT CAST(dt AS DATE), campaign, country, campaign
    FROM (
        SELECT campaign, country, dt, id AS x0, id AS x1 FROM merge_union_output_mapping
        UNION ALL
        SELECT campaign, country, dt, id AS x0, id AS x1 FROM merge_union_output_mapping
    ) u
), c AS (
    SELECT dt, country, tag, 'SA' AS ad_type FROM sa
), e AS (
    SELECT 'FR' AS country
)
SELECT a.country
FROM c a
LEFT JOIN e ON CASE
    WHEN a.country = 'PL' AND a.ad_type = 'DSP' THEN 'PL_DSP'
    ELSE a.country
END = e.country
LEFT JOIN c b ON a.tag = b.tag
GROUP BY a.country"""
    order_qt_country_date """WITH sa AS (
    SELECT CAST(dt AS DATE) AS dt, campaign, country, campaign AS tag FROM merge_union_output_mapping
    UNION ALL
    SELECT CAST(dt AS DATE), campaign, country, campaign
    FROM (
        SELECT campaign, country, dt, id AS x0, id AS x1 FROM merge_union_output_mapping
        UNION ALL
        SELECT campaign, country, dt, id AS x0, id AS x1 FROM merge_union_output_mapping
    ) u
), c AS (
    SELECT dt, country, tag, 'SA' AS ad_type FROM sa
), e AS (
    SELECT 'FR' AS country
)
SELECT a.country, a.dt
FROM c a
LEFT JOIN e ON CASE
    WHEN a.country = 'PL' AND a.ad_type = 'DSP' THEN 'PL_DSP'
    ELSE a.country
END = e.country
LEFT JOIN c b ON a.tag = b.tag
GROUP BY a.country, a.dt"""
}
