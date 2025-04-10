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
    def runTopNLazyMatTests() {
        def limitValues = [1, 5, 10, 20, 30]
        
        // Single table query tests, using nested loops to iterate through tables and limit values
        for (String table : ["orc_topn_lazy_mat_table", "parquet_topn_lazy_mat_table"]) {
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



    for (String hivePrefix : ["hive2", "hive3"]) {
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "${hivePrefix}_test_hive_topn_lazy_mat"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""
        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        
        sql """ use global_lazy_mat_db """



        

        runTopNLazyMatTests()






        sql """drop catalog ${catalog_name};"""
    }
}
