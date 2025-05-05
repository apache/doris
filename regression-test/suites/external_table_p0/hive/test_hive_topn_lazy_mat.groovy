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

suite("test_hive_topn_lazy_mat", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }


    // Define test function inside suite
    def runTopNLazyMatTests = {
        def limitValues = [3, 8, 29]
        
        // Single table query tests, using nested loops to iterate through tables and limit values
        for (String table : ["orc_topn_lazy_mat_table", "parquet_topn_lazy_mat_table"]) {
            qt_1 """ select * from ${table} order by id limit 10; """
            qt_2 """ select * from ${table} order by id,file_id limit 10; """
            qt_3 """ select score, value, active,name  from ${table} order by id,file_id limit 10; """
            qt_4 """ select value,name,id,file_id  from ${table} order by name limit 10; """


            for (int limit : limitValues) {
                // Basic query
                qt_test_basic """ 
                    select * from ${table} 
                    where value > 0 
                    order by id, score desc 
                    limit ${limit};
                """

                // Partial columns query
                qt_test_partial """
                    select id, name, score 
                    from ${table} 
                    where active = true 
                    order by id, value desc 
                    limit ${limit};
                """

                // Multi-field sorting
                qt_test_multi_sort """
                    select id, name, value, score 
                    from ${table} 
                    order by id, score desc, value asc 
                    limit ${limit};
                """

                // Filter condition query
                qt_test_filter """
                    select id, name, value 
                    from ${table} 
                    where active = true and value > 100 
                    order by id, value desc 
                    limit ${limit};
                """

                // Subquery
                qt_test_subquery """
                    select t.id, t.name, t.total_score 
                    from (
                        select id, name, (value + score) as total_score 
                        from ${table} 
                        where active = true
                    ) t 
                    order by t.id, t.total_score desc 
                    limit ${limit};
                """

                // Aggregation query
                qt_test_agg """
                    select 
                        id,
                        max(score) as max_score,
                        avg(value) as avg_value
                    from ${table} 
                    group by id
                    order by id, max_score desc 
                    limit ${limit};
                """
            }
        }

        // Multi-table query tests (join related), also using limit loop
        for (int limit : limitValues) {
            // Join query
            qt_test_join1 """
                select o.id, o.name, o.value, p.score 
                from orc_topn_lazy_mat_table o 
                join parquet_topn_lazy_mat_table p 
                on o.id = p.id 
                where o.active = true 
                order by o.id, o.value desc 
                limit ${limit};
            """

            // Left join
            qt_test_join2 """
                select o.id, o.name, o.value, p.score 
                from orc_topn_lazy_mat_table o 
                left join parquet_topn_lazy_mat_table p 
                on o.id = p.id and o.file_id = p.file_id
                where o.score > 0 
                order by o.id, o.score desc, p.score desc 
                limit ${limit};
            """

            // Complex join query
            qt_test_complex """
                select 
                    o.id,
                    o.name,
                    o.value,
                    o.score,
                    p.value as p_value
                from orc_topn_lazy_mat_table o 
                left join parquet_topn_lazy_mat_table p 
                on o.id = p.id 
                where o.active = true 
                    and o.value > 0 
                    and o.score is not null
                order by o.id, o.score desc, p.value asc 
                limit ${limit};
            """
        }
    }



    for (String hivePrefix : ["hive2"]) {
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "ali_hive"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        // logger.info("switched to catalog " + catalog_name)
        
        sql """ use daidai; """



        sql """
        set enable_topn_lazy_materialization=true;
        set runtime_filter_mode=GLOBAL;
        set TOPN_FILTER_RATIO=0.5;
        set disable_join_reorder=true;
        set enable_runtime_filter_prune=false;
        """


        explain {
            sql "select * from  orc_topn_lazy_mat_table order by id limit 10; "
            contains("projectList:[id, name, value, active, score, file_id]")
            contains("column_descs_lists[[`name` text NULL, `value` double NULL, `active` boolean NULL, `score` double NULL, `file_id` int NULL]]")
            contains("locations: [[1, 2, 3, 4, 5]]")
            contains("table_idxs: [[1, 2, 3, 4, 5]]")
            contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__orc_topn_lazy_mat_table]")
        }
       
        explain {
            sql " select file_id,id from  orc_topn_lazy_mat_table  order by name limit 10; "
            contains("projectList:[file_id, id]")
            contains("column_descs_lists[[`id` int NULL, `file_id` int NULL]]")
            contains("locations: [[1, 2]]")
            contains("table_idxs: [[0, 5]]")
            contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__orc_topn_lazy_mat_table]")
        }

        explain {
            sql """ select a.name,length(a.name),a.value,b.*,a.* from  parquet_topn_lazy_mat_table as a    
            join  orc_topn_lazy_mat_table as b on a.id = b.id order by a.name    limit 10 """
            contains("projectList:[name, length(a.name), value, id, name, value, active, score, file_id, id, name, value, active, score, file_id]")
            contains("column_descs_lists[[`name` text NULL, `value` double NULL, `active` boolean NULL, `score` double NULL, `file_id` int NULL], [`value` double NULL, `active` boolean NULL, `score` double NULL, `file_id` int NULL]]")
            contains("locations: [[5, 6, 7, 8, 9], [10, 11, 12, 13]]")
            contains("table_idxs: [[1, 2, 3, 4, 5], [2, 3, 4, 5]]")
            contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__orc_topn_lazy_mat_table, __DORIS_GLOBAL_ROWID_COL__parquet_topn_lazy_mat_table]")
        }

        runTopNLazyMatTests()


        sql """ set enable_topn_lazy_materialization=false; """
        runTopNLazyMatTests()




    }
}
