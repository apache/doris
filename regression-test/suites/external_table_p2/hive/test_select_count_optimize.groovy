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

suite("test_select_count_optimize", "p2,external,hive,external_remote,external_remote_hive") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "test_select_count_optimize"
        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hadoop.username' = 'hadoop',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """
        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)

        sql """set experimental_enable_nereids_planner=true;"""
        sql """set enable_fallback_to_original_planner=false;"""

        //parquet 
        qt_sql """ select * from tpch_1000_parquet.nation order by n_name,n_regionkey,n_nationkey,n_comment ; """

        qt_sql """ select count(*) from tpch_1000_parquet.nation; """

        qt_sql """ select count(1024) from tpch_1000_parquet.nation; """
        
        qt_sql """ select count(null) from tpch_1000_parquet.nation; """


        qt_sql """ select count(*) from tpch_1000_parquet.nation where n_regionkey = 0; """

        qt_sql """ select max(n_regionkey) from  tpch_1000_parquet.nation ;""" 
        
        qt_sql """ select min(n_regionkey) from  tpch_1000_parquet.nation ; """ 

        qt_sql """ select count(*) as a from tpch_1000_parquet.nation group by n_regionkey order by a; """ 
        
        qt_sql """ select count(*) from  tpch_1000_parquet.lineitem; """  

        qt_sql """ select count(*) from  tpch_1000_parquet.part; """  
        
        qt_sql """ select count(p_partkey) from tpch_1000_parquet.part; """  

        qt_sql """ select count(*) as sz from tpch_1000_parquet.part group by p_size order by  sz ;""" 

        qt_sql """ select count(*) from tpch_1000_parquet.part where p_size = 1; """ 

        qt_sql """ select count(*) from   user_profile.hive_hll_user_profile_wide_table_parquet; """;

        //orc 
        qt_sql """ select count(*) from   tpch_1000_orc.part where p_partkey=1; """ 
        
        qt_sql """ select max(p_partkey) from   tpch_1000_orc.part ; """ 

        qt_sql """ select count(p_comment) from tpch_1000_orc.part; """ 

        qt_sql """ select count(*) as a from tpch_1000_orc.part group by p_size order by a limit 3 ; """ 

        qt_sql """ select count(*) from user_profile.hive_hll_user_profile_wide_table_orc ; """ ; 

        //other 
        qt_sql """ select n_name from tpch_1000.nation order by n_name limit 2; """ 

        qt_sql """ select count(*) from tpch_1000.nation; """ 
        
        qt_sql """ select min(n_regionkey) from  tpch_1000.nation ; """ 

        qt_sql """ select count(*) from tpch_1000.nation where n_nationkey=5;"""

        qt_sql """ select count(*) as a from tpch_1000.nation group by n_regionkey order by a;""" 
        
        explain {
            
            sql "select count(*) from tpch_1000_parquet.nation;"

            contains "pushdown agg=COUNT"
        } 

        explain {
            
            sql "select count(1) from tpch_1000_parquet.nation;"

            contains "pushdown agg=COUNT"
        } 


        explain {
            
            sql "select count(2) from tpch_1000_parquet.nation;"

            contains "pushdown agg=COUNT"
        } 


        explain {
            
            sql "select count(n_name) from tpch_1000_parquet.nation;"

            notContains "pushdown agg=COUNT"
        }
        
        explain {
            
            sql "select count(n_name) from tpch_1000_parquet.nation  where n_nationkey = 1;"

            notContains "pushdown agg=COUNT"
        }

        explain {
            
            sql "select count(*) from tpch_1000_parquet.nation group by n_regionkey ;"

            notContains "pushdown agg=COUNT"
        }


        explain {

            sql " select count(*) from  multi_catalog.test_csv_format_error; "

            contains "pushdown agg=COUNT"
        } 

        explain {
            sql "select count(*) from  multi_catalog.hits_orc ; "

            contains "pushdown agg=COUNT"

        }


        explain {
            sql "select count(*) from  multi_catalog.hits_orc ; "

            contains "pushdown agg=COUNT"

        }


        explain {

            sql "select count(*) from multi_catalog.parquet_one_column;"

            contains "pushdown agg=COUNT"
        }

        explain {

            sql "select count(col1) from multi_catalog.parquet_one_column;"

            notContains "pushdown agg=COUNT"
        }
    
        explain {

            sql "select count(*) from multi_catalog.parquet_two_column;"

            contains "pushdown agg=COUNT"
        }
        explain {

            sql "select count(*) from multi_catalog.parquet_two_column where col1 = 1;"

            notContains "pushdown agg=COUNT"
        }


        explain {

            sql "select count(*) from  multi_catalog.logs2_orc;"

            contains "pushdown agg=COUNT"
        }
    }
}

