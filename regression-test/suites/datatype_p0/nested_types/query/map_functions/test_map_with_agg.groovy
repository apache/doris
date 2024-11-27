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

suite("test_map_with_agg", "p0") {
    // create table
    sql """ DROP TABLE IF EXISTS t_map_count;"""
    sql """ CREATE TABLE IF NOT EXISTS t_map_count(id int(11), c_char VARCHAR(20), p1 varchar(200), p2 varchar(200), et VARCHAR(30), uid VARCHAR(100))
     ENGINE=OLAP DUPLICATE KEY(`id`, `c_char`, `p1`) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num' = '1');"""

    // insert data
    sql """ INSERT INTO t_map_count VALUES(1, 'express_search','consumer-un','17469174857s957ssf','page_l','36e44300d6c7433784852347b167ccdbd');"""
    sql """ INSERT INTO t_map_count VALUES(1, 'expr_seach','consumer-un','17469174857s957ssf','page_l','36e44300d6c7433784852347b167ccdbd');"""
    sql """ INSERT INTO t_map_count VALUES(1, 'expr_seach','consumer-un','17469174857s957ssf','page_l','36e44300d6c7433784852347b167ccdbd');"""
    sql """ INSERT INTO t_map_count VALUES(1, 'expr_seach','consumer-un','17469174857s957ssf','page_l','36e44300d6c7433784852347b167ccdbd');"""
    sql """ INSERT INTO t_map_count VALUES(1, 'express_search','consumer-un','17469174857s957ssf','page_l','36e44300d6c7433784852347b167ccdbd');"""
    sql """ INSERT INTO t_map_count VALUES(1, 'expr_seach','consumer-un','17469174857s957ssf','page_l','36e44300d6c7433784852347b167ccdbd');"""
    sql """ INSERT INTO t_map_count VALUES(1, 'express_search','consumer-un','17469174857s957ssf','page_l','36e44300d6c7433784852347b167ccdbd');"""
    sql """ INSERT INTO t_map_count VALUES(1, 'expr_seach','consumer-un','17469174857s957ssf','page_l','36e44300d6c7433784852347b167ccdbd');"""
    sql """ INSERT INTO t_map_count VALUES(1, 'express_search','consumer-un','17469174857s957ssf','page_l','36e44300d6c7433784852347b167ccdbd');"""
    sql """ INSERT INTO t_map_count VALUES(1, 'expr_seach','consumer-un','17469174857s957ssf','page_l','36e44300d6c7433784852347b167ccdbd');"""
    sql """ INSERT INTO t_map_count VALUES(1, 'express_search','consumer-un','17469174857s957ssf','page_l','36e44300d6c7433784852347b167ccdbd');"""
    sql """ INSERT INTO t_map_count VALUES(1, 'expr_seach','consumer-un','17469174857s957ssf','page_l','36e44300d6c7433784852347b167ccdbd');"""
    sql """ INSERT INTO t_map_count VALUES(1, 'express_search','consumer-un','17469174857s957ssf','page_l','36e44300d6c7433784852347b167ccdbd');"""
    sql """ INSERT INTO t_map_count VALUES(1, 'expr_seach','consumer-un','17469174857s957ssf','page_l','36e44300d6c7433784852347b167ccdbd');"""

    sql """ set parallel_pipeline_task_num=5; """

    // test in old planner
    sql """set enable_nereids_planner=false"""
    order_qt_old_sql """  SELECT id, c_char, map('exp_sea', 1) as m  FROM t_map_count  WHERE p1 = 'comr' AND p2 = 'ex'  GROUP BY 1,2
                          union all
                          SELECT id, c_char, map('exp_seac', count(CASE WHEN et = 'page_l' THEN uid END )) as m FROM t_map_count  WHERE  p1 = 'consumer-un' AND p2 = '17469174857s957ssf'  GROUP BY 1,2;"""


    // test in nereids planner
    sql """set enable_nereids_planner=true"""
    sql """ set enable_fallback_to_original_planner=false"""
    order_qt_nereid_sql """  SELECT id, c_char, map('exp_sea', 1) as m  FROM t_map_count  WHERE p1 = 'comr' AND p2 = 'ex'  GROUP BY 1,2
                             union all
                             SELECT id, c_char, map('exp_seac', count(CASE WHEN et = 'page_l' THEN uid END )) as m FROM t_map_count  WHERE  p1 = 'consumer-un' AND p2 = '17469174857s957ssf'  GROUP BY 1,2;"""
}
