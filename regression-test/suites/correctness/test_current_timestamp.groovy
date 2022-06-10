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

suite("test_current_timestamp", "correctness") {
    def tableName = "test_current_timestamp"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName}
        (
            id TINYINT,
            name CHAR(10) NOT NULL DEFAULT "zs",
            dt_0 DATETIME,
            dt_1 DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        COMMENT "test current_timestamp table"
        DISTRIBUTED BY HASH(id)
        PROPERTIES("replication_num" = "1");
    """
    
    // test insert into.
    sql " insert into ${tableName} (id,name,dt_0) values (1,'aa',current_timestamp()); "
    sql " insert into ${tableName} (id,name,dt_0) values (2,'bb',current_timestamp()); "
    sql " insert into ${tableName} (id,name,dt_0) values (3,'cc',current_timestamp()); "
    sql " insert into ${tableName} (id,name,dt_0) values (4,'dd',current_timestamp()); "

    qt_insert_into """ select count(*) from ${tableName} where to_date(dt_0) = to_date(dt_1); """

    // test stream load.
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'columns', 'id, name, dt_0 = current_timestamp()'

        file 'test_current_timestamp_streamload.csv'

        time 10000 // limit inflight 10s
    }
    qt_stream_load """ select count(*) from ${tableName} where id > 4 and to_date(dt_0) = to_date(dt_1); """
 }