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
suite("test_index_rqg_bug6", "test_index_rqg_bug"){
    def table = "test_index_rqg_bug6"

    sql "drop table if exists ${table}"

    sql """
      create table ${table} (
        pk int,
        col_int_undef_signed_index_inverted  int not null  ,
        col_varchar_1024__undef_signed_not_null varchar(1024) not  null, 
        INDEX col_int_undef_signed_index_inverted_idx (`col_int_undef_signed_index_inverted`) USING INVERTED,
        INDEX col_varchar_1024__undef_signed_not_null_idx (`col_varchar_1024__undef_signed_not_null`) USING INVERTED
        ) engine=olap
        DUPLICATE KEY(pk)
        distributed by hash(pk) buckets 1
        properties("replication_num" = "1");
    """

    sql """ insert into ${table} values (10, 0, 'ok'), (11, 0, 'oo'), (12, 1, 'ok')"""


    sql """ sync"""
    sql """ set enable_inverted_index_query = true """
    sql """ set inverted_index_skip_threshold = 0 """
    qt_sql """ 
            SELECT 
                count() 
            FROM 
                test_index_rqg_bug6 
            WHERE 
                IF(col_int_undef_signed_index_inverted = 0, 'true', 'false') = 'false' 
                AND (
                    col_varchar_1024__undef_signed_not_null LIKE 'ok'
                    OR col_int_undef_signed_index_inverted = 0
                );
    """

    qt_sql_2 """ 
            SELECT 
                count() 
            FROM 
                test_index_rqg_bug6 
            WHERE 
                col_varchar_1024__undef_signed_not_null LIKE 'ok'
                OR col_int_undef_signed_index_inverted = 0;
    """
}
