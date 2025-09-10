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

suite("test_tvf_topn_lazy_mat","external,hive,tvf,external_docker") {
    String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")


    String enabled = context.config.otherConfigs.get("enableHiveTest")



    def hdfsUserName = "doris"
    def defaultFS = "hdfs://${externalEnvIp}:${hdfs_port}"
    def parquet_uri = "${defaultFS}" + "/user/doris/preinstalled_data/parquet_table/parquet_global_lazy_mat_table/file_id=1/example_1.parquet";
    def orc_uri1 = "${defaultFS}" + "/user/doris/preinstalled_data/orc_table/orc_global_lazy_mat_table/file_id=1/example_1.orc"
    def orc_uri2 = "${defaultFS}" + "/user/doris/preinstalled_data/orc_table/orc_global_lazy_mat_table/file_id=2/example_2.orc"

    def tvf_parquet_1 = """ HDFS(
                            "uri" = "${parquet_uri}",
                            "hadoop.username" = "doris",
                            "format" = "parquet") """
    
    def tvf_orc_1 = """ HDFS("uri" = "${orc_uri1}","hadoop.username" = "doris","format" = "ORC") """
    def tvf_orc_2 = """ HDFS("uri" = "${orc_uri2}","hadoop.username" = "doris", "format" = "orc") """
    

    def runTopNLazyMatTests = {
        def limitValues = [3, 7]
        
        // Single table query tests, using nested loops to iterate through tables and limit values
        for (String table : ["${tvf_orc_1}", "${tvf_parquet_1}"]) {
            qt_1 """ select * from ${table} order by id limit 3; """
            qt_2 """ select * from ${table} order by score limit 5; """
            qt_3 """ select score, value, active,name  from ${table} order by value limit 4; """
            qt_4 """ select value,name,id from ${table} order by name,score limit 2; """

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
    }

    def runjointest = {
        qt_test_join_1 """ 
            select * from ${tvf_parquet_1} as a join  ${tvf_orc_1} as b on a.id =  b.id  order by a.name limit 5;    
        """

        qt_test_join_2 """
            select a.id,b.name from ${tvf_parquet_1} as a join  ${tvf_orc_2} as b on a.id + 9 =  b.id  order by a.id limit 5;    
        """

        qt_test_join_3 """
            select a.* from ${tvf_orc_1} as a join  ${tvf_orc_2} as b on a.id + 9 =  b.id  order by b.name limit 5;    
        """

        qt_test_join_4 """
            select * from ${tvf_orc_1} as a join  ${tvf_orc_1} as b on a.id = b.value  order by a.id limit 4;    
        """
        
        qt_test_join_5 """
            select * from ${tvf_orc_1} as a join  ${tvf_orc_2} as b join ${tvf_parquet_1} as c  
                on a.value + 9  =  b.id and c.name = a.name  order by a.score limit 5;    
        """
        

    }
    sql """ set disable_join_reorder=true; """

    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        //id   | name  | value | active | score 
        sql """ set topn_lazy_materialization_threshold=1024; """
         
        explain {
            sql """ verbose select * from ${tvf_parquet_1} order by id limit 5; """ 
            contains("VMaterializeNode")
            contains("projectList:[id, name, value, active, score]")

            contains("column_descs_lists[[`name` text NULL, `value` double NULL, `active` boolean NULL, `score` double NULL]]")
            contains("locations: [[1, 2, 3, 4]]")
            contains("table_idxs: [[1, 2, 3, 4]]")
            contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__hdfs]")
            contains("isTopMaterializeNode: true")
            contains("SlotDescriptor{id=0, col=id, colUniqueId=-1, type=bigint, nullable=true")
            contains("SlotDescriptor{id=1, col=__DORIS_GLOBAL_ROWID_COL__hdfs, colUniqueId=-1, type=text, nullable=false,")
        }


        explain {
            sql """ verbose select name,value,score from  ${tvf_orc_1} order by id limit 5; """ 
            contains("VMaterializeNode")

            contains("projectList:[name, value, score]")
            contains("column_descs_lists[[`name` text NULL, `value` double NULL, `score` double NULL]]")
            contains("locations: [[1, 2, 3]]")
            contains("table_idxs: [[1, 2, 4]]")
            contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__hdfs]")
            contains("isTopMaterializeNode: true")


            contains("SlotDescriptor{id=0, col=id, colUniqueId=-1, type=int, nullable=true")
            contains("SlotDescriptor{id=1, col=__DORIS_GLOBAL_ROWID_COL__hdfs, colUniqueId=-1, type=text, nullable=false,")
        }


        explain {
            sql """verbose select * from ${tvf_parquet_1} as a join  ${tvf_orc_1} as b on a.id =  b.id  order by a.name limit 5; """
            contains("VMaterializeNode")
            contains("projectList:[id, name, value, active, score, id, name, value, active, score]")
            contains("column_descs_lists[[`value` double NULL, `active` boolean NULL, `score` double NULL], [`name` text NULL, `value` double NULL, `active` boolean NULL, `score` double NULL]]")
            contains("locations: [[3, 4, 5], [6, 7, 8, 9]]")
            contains("table_idxs: [[2, 3, 4], [1, 2, 3, 4]]")
            contains("row_ids: [__DORIS_GLOBAL_ROWID_COL__hdfs, __DORIS_GLOBAL_ROWID_COL__hdfs]")
            contains("isTopMaterializeNode: true")
        }

        runTopNLazyMatTests()
        runjointest()




        sql """ set topn_lazy_materialization_threshold=-1; """
        explain {
            sql """ verbose select * from ${tvf_parquet_1} order by id limit 5; """ 
            notContains ("VMaterializeNode")
        }


        explain {
            sql """ verbose select name,value,score from  ${tvf_orc_1} order by id limit 5; """ 
            notContains ("VMaterializeNode")
        }

        explain {
            sql """verbose select * from ${tvf_parquet_1} as a join  ${tvf_orc_1} as b on a.id =  b.id  order by a.name limit 5; """
            notContains ("VMaterializeNode")
        }



        runTopNLazyMatTests()
        runjointest()        
    }

}

