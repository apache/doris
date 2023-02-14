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

// Most of the cases are copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases
// and modified by Doris.

suite("nereids_emptyset_prune") {
    
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"

    sql "set enable_nereids_planner=true;"
    explain{
        sql """
            select *  
            from      
                (select * from region where false)T1      
                join      
                (select * from nation) T2 on r_regionkey=n_regionkey;
            """
        notContains "VHASH JOIN"
    }

    explain{
        sql """
            select *  
            from      
                (select * from region where false)T1      
                left semi join      
                (select * from nation) T2 on r_regionkey=n_regionkey;
            """
        notContains "JOIN"
    }

    explain{
        sql """
            select *  
            from      
                (select * from region where false)T1      
                join      
                (select * from nation) T2;
            """
        notContains "JOIN"
    }

    qt_scalar_sum """
         select sum(T2.n_nationkey)  
         from      
             (select * from region where false) T1      
             join      
             (select * from nation) T2 
             on r_regionkey=n_regionkey;
        """
    qt_agg_sum """
         select sum(T2.n_nationkey)  
         from      
              (select * from region where false) T1      
              join      
              (select * from nation) T2 
              on r_regionkey=n_regionkey 
        group by T1.r_name;
        """
}