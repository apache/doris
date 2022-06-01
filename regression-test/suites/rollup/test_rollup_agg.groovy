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
suite("test_rollup_agg", "rollup") {
    def tbName = "test_rollup_agg"
    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName}(
                siteid INT(11) NOT NULL,
                citycode SMALLINT(6) NOT NULL,
                username VARCHAR(32) NOT NULL,
                pv BIGINT(20) SUM NOT NULL DEFAULT '0',
                uv BIGINT(20) SUM NOT NULL DEFAULT '0'
            )
            AGGREGATE KEY (siteid,citycode,username)
            DISTRIBUTED BY HASH(siteid) BUCKETS 5 properties("replication_num" = "1");
        """
    String res = "null"
    sql "ALTER TABLE ${tbName} ADD ROLLUP rollup_city(citycode, pv);"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE ROLLUP WHERE TableName='${tbName}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    res = "null"
    sql "ALTER TABLE ${tbName} ADD COLUMN vv BIGINT SUM NULL DEFAULT '0' TO rollup_city;"
    while (!res.contains("FINISHED")){
        res = sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbName}' ORDER BY CreateTime DESC LIMIT 1;"
        if(res.contains("CANCELLED")){
            print("job is cancelled")
            break
        }
        Thread.sleep(1000)
    }
    sql "SHOW ALTER TABLE ROLLUP WHERE TableName='${tbName}';"
    qt_sql "DESC ${tbName} ALL;"
    sql "insert into ${tbName} values(1, 1, 'test1', 100,100,100);"
    sql "insert into ${tbName} values(2, 1, 'test2', 100,100,100);"
    explain{
        sql("SELECT citycode,SUM(pv) FROM ${tbName} GROUP BY citycode")
        contains("rollup: rollup_city")
    }
    qt_sql "SELECT citycode,SUM(pv) FROM ${tbName} GROUP BY citycode"
    sql "ALTER TABLE ${tbName} DROP ROLLUP rollup_city"
    sql "DROP TABLE ${tbName}"
}
