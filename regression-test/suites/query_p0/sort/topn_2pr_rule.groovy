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

suite("topn_2pr_rule") {
    sql """set topn_opt_limit_threshold = 1024"""
    sql """set enable_two_phase_read_opt= true"""

    def create_table = { table_name, key_type="DUPLICATE" ->
        sql "DROP TABLE IF EXISTS ${table_name}"
        value_type = "v string"
        if ("${key_type}" == "AGGREGATE") {
            value_type = "v string REPLACE_IF_NOT_NULL NULL" 
        }
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                k bigint,
                ${value_type}
            )
            ${key_type} KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            properties("replication_num" = "1", "disable_auto_compaction" = "false");
        """
    }
    def verify = { table_name, key_type->
        if("${key_type}" == "DUPLICATE") {
            explain {
                sql("select * from ${table_name}  order by k limit 1;")
                contains "OPT TWO PHASE"
            } 
            explain {
                sql("select * from ${table_name}  where k = 1 order by k limit 1;")
                contains "OPT TWO PHASE"
            } 
            explain {
                sql("select * from ${table_name}  where k order by k + 1 limit 1;")
                notContains "OPT TWO PHASE"
            } 
        } else if("${key_type}" == "UNIQUE") {
             explain {
                sql("select * from ${table_name}  order by k limit 1;")
                notContains "OPT TWO PHASE"
            } 
        } else if("${key_type}" == "AGGREGATE") {
             explain {
                sql("select * from ${table_name}  order by k limit 1;")
                notContains "OPT TWO PHASE"
            } 
        }
    }

    def key_types = ["DUPLICATE", "UNIQUE", "AGGREGATE"]
    for (int i = 0; i < key_types.size(); i++) {
        def table_name = "topn_2pr_rule_${key_types[i]}" 
        create_table.call(table_name, key_types[i])
        sql """insert into ${table_name} values(1, "1")"""
        sql """insert into ${table_name} values(2, "2")"""
        sql """insert into ${table_name} values(3, "3")"""
        verify.call(table_name, key_types[i])
    }
}