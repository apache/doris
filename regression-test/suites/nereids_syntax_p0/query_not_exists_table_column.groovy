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


suite("query_not_exists_table_column") {
    test {
        sql("select * from fasdfasdf.kfafds")
        exception("Database [fasdfasdf] does not exist.(line 1, pos 14)")
    }

    def currentDb = (sql "select database()")[0][0]
    test {
        sql("select * from ${currentDb}.asdfasfdsaf")
        exception("Table [asdfasfdsaf] does not exist in database [${currentDb}].(line 1, pos 14)")
    }

    multi_sql """
        drop table if exists query_not_exists_table_column;
        create table if not exists query_not_exists_table_column(
            id int
        ) 
        distributed by hash(id)
        properties('replication_num'='1')"""

    test {
        sql("select kkk from ${currentDb}.query_not_exists_table_column")
        exception("Unknown column 'kkk' in 'table list' in PROJECT clause(line 1, pos 7)")
    }
}