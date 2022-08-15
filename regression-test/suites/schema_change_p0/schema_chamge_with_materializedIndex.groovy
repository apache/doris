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

suite("schema_change_with_materizlized_index", "schema_change") {
    def tbName = "schema_change_with_materizlized_index"
    sql "drop table if exists ${tbName}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName} (
                k1 INT,
                value1 INT,
                value2 INT,
                value3 INT
            )
            DUPLICATE KEY (k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1 properties("replication_num" = "1", "light_schema_change" = "true");
        """
    sql "insert into ${tbName} values(1,1,1,1);"
    sql "insert into ${tbName} values(1,1,1,2);"
    sql "insert into ${tbName} values(2,2,2,2);"
    sql "insert into ${tbName} values(3,3,3,3);"
    sql "insert into ${tbName} values(4,4,4,4);"

    sql "insert into ${tbName} values(1,1,1,5);"
    sql "insert into ${tbName} values(1,1,1,6);"
    sql "insert into ${tbName} values(2,2,2,7);"
    sql "insert into ${tbName} values(3,3,3,8);"
    sql "insert into ${tbName} values(4,4,4,9);"

    qt_sql "select value1, value2, sum(value3) from ${tbName} group by value1, value2 order by value1;"
    sql "create materialized view mv as select value1, value2, sum(value3) from ${tbName} group by value1, value2;"
    def result = "null"
    while (!result.contains("FINISHED")){
        result = sql "SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${tbName}' ORDER BY CreateTime DESC LIMIT 1;"
        result = result.toString()
        logger.info("result: ${result}")
        if(result.contains("CANCELLED")){
            return
        }
        Thread.sleep(100)
    }
    qt_sql "select value1, value2, sum(value3) from ${tbName} group by value1, value2 order by value1;"

    qt_sql "desc ${tbName} all;";

    // when dropColumn is materizlized index, should do normal schema change
    sql "alter table ${tbName} drop column value2;"
    result = "null"
    while (!result.contains("FINISHED")){
        result = sql "SHOW ALTER TABLE COLUMN WHERE IndexName='${tbName}' ORDER BY CreateTime DESC LIMIT 1;"
        result = result.toString()
        logger.info("result: ${result}")
        if(result.contains("CANCELLED")) {
            log.info("rollup job is cancelled, result: ${result}".toString())
            return
        }
        Thread.sleep(100)
    }

    qt_sql "desc ${tbName} all;"; 
    qt_sql "select value1, sum(value3) from ${tbName} group by value1 order by value1;"

    sql "truncate table ${tbName};"
    sql "DROP MATERIALIZED VIEW mv on ${tbName};"
    sql "alter table ${tbName} add column value2 int after value1;"
    qt_sql "desc ${tbName} all;"; 

    sql "insert into ${tbName} values(1,1,1,1);"
    sql "insert into ${tbName} values(1,1,1,2);"
    sql "insert into ${tbName} values(2,2,2,2);"
    sql "insert into ${tbName} values(3,3,3,3);"
    sql "insert into ${tbName} values(4,4,4,4);"

    sql "insert into ${tbName} values(1,1,1,5);"
    sql "insert into ${tbName} values(1,1,1,6);"
    sql "insert into ${tbName} values(2,2,2,7);"
    sql "insert into ${tbName} values(3,3,3,8);"
    sql "insert into ${tbName} values(4,4,4,9);"

    qt_sql "select value1, sum(value2), sum(value3) from ${tbName} group by value1 order by value1;"
    sql "create materialized view mv2 as select value1, sum(value2), sum(value3) from ${tbName} group by value1;"
    result = "null"
    while (!result.contains("FINISHED")){
        result = sql "SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${tbName}' ORDER BY CreateTime DESC LIMIT 1;"
        result = result.toString()
        logger.info("result: ${result}")
        if(result.contains("CANCELLED")){
            return
        }
        Thread.sleep(100)
    }
    qt_sql "select value1, sum(value2), sum(value3) from ${tbName} group by value1 order by value1;"
    sql "alter table ${tbName} drop column value2;"
    qt_sql "desc ${tbName} all;"; 
    qt_sql "select value1, sum(value3) from ${tbName} group by value1 order by value1;"
}
