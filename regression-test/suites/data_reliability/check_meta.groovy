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
suite("check_meta", "data_reliability,p3") {
    List<List<Object>> dbRes = sql "show databases"
    for (dbRow : dbRes) {
        def db = dbRow[0]
        if (db == "__internal_schema" || db == "information_schema") {
            continue
        }
        if (db.contains("external_table")) {
            continue
        }

        List<List<Object>> tableRes = sql """ show tables from ${db} """
        for (tableRow : tableRes) {
            def table = tableRow[0]
            def createTableSql
            try {
                createTableSql = sql "show create table ${db}.${table}"
            } catch (Exception e) {
                if (e.getMessage().contains("not support async materialized view")) {
                    try {
                        createTableSql = sql "show create materialized view ${db}.${table}"
                    } catch (Exception e2) {
                        if (e2.getMessage().contains("table not found")) {
                            continue
                        }
                    }
                } else {
                    logger.warn("Failed to show create materialized view ${db}.${table}: ${e.getMessage()}")
                    continue
                }
            }
            if (createTableSql[0][1].contains("CREATE VIEW")) {
                continue
            }
            logger.info("select count database: {}, table {}", db, table)

            def repeatedTimes = 6;  // replica num * 2
            for (int i = 0; i < repeatedTimes; i++) {
                sql """ select /*+ SET_VAR(enable_push_down_no_group_agg=false) */ count(*) from ${db}.`${table}` """
            }
        }
    }
}

