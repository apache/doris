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
suite("test_index_rqg_bug7", "test_index_rqg_bug"){
    def table = "test_index_rqg_bug7"

    sql "drop table if exists ${table}"

    sql """
      create table ${table} (
        pk int,
        col_int_undef_signed int  null  ,
        col_int_undef_signed_not_null_index_inverted int  not null  ,
        INDEX col_int_undef_signed_not_null_index_inverted_idx (`col_int_undef_signed_not_null_index_inverted`) USING INVERTED
        ) engine=olap
        DUPLICATE KEY(pk)
        distributed by hash(pk) buckets 10
        properties("replication_num" = "1");
    """

    sql """ insert into ${table} values (1, 7, 7), (2, 7, -2), (3, 4, -2)"""


    sql """ sync"""
    sql """ set enable_inverted_index_query = true """
    sql """ set inverted_index_skip_threshold = 0 """
    sql """ set enable_no_need_read_data_opt = true """
    qt_sql """ 
           select count(*) from ${table} where col_int_undef_signed_not_null_index_inverted = -2  AND ((CASE WHEN col_int_undef_signed_not_null_index_inverted = -2 THEN 1 ELSE NULL END = 1) OR col_int_undef_signed != 7);
    """
}
