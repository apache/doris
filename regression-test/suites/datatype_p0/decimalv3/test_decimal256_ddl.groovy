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

suite("test_decimal256_ddl") {
    def getJobState = { tableName ->
        def jobStateResult = sql """ SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
        return jobStateResult[0][9]
    }

    def waitSchemaChangeFinish = { table ->
        while (true) {
            String result = getJobState(table)
            if (result == "FINISHED") {
                break
            } else if (result == "CANCELLED") {
                sucess.set(false)
                logger.error("schema change was cancelled")
                assertTrue(false)
            } else {
                sleep(2000)
            }
        }
    }

    def table1 = "test_decimal256_ddl"

    sql "set enable_decimal256=true"

    sql "drop table if exists ${table1}"

    sql """
    CREATE TABLE IF NOT EXISTS `${table1}` (
      id int,
      dcl1 decimal(40, 5)
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`, `dcl1`)
    DISTRIBUTED BY HASH(`id`, `dcl1`) BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    )
    """

    sql """
        alter table ${table1} add column (dcl2 decimal(60,15));
    """

    waitSchemaChangeFinish(table1)


    sql """
        alter table ${table1} add column (dcl3 decimal(60,15) default '0');
    """

    waitSchemaChangeFinish(table1)


    sql """
        alter table ${table1} modify column dcl2 decimal(48, 5);
    """

    waitSchemaChangeFinish(table1)


    sql """
        alter table ${table1} modify column dcl3 decimal(48, 5) default '0';
    """

    waitSchemaChangeFinish(table1)


    sql """
        create materialized view ${table1}mv as select id, sum(dcl1), min(dcl2), max(dcl3) from ${table1} group by id;
    """


    sql """
        insert into ${table1} values
            (1,1.1,1.2,1.3),
            (1,11.1,11.2,11.3),
            (1,12.1,12.2,12.3),
            (2,2.1,2.2,2.3),  
            (2,21.1,21.2,21.3),  
            (2,22.1,22.2,22.3)  
    """

    sql """sync"""

    order_qt_select """
        select * from ${table1} order by id
    """

    order_qt_agg """
        select sum(dcl1), min(dcl2), max(dcl3) from ${table1} group by id
    """

    sql "drop table if exists ${table1} force"

    sql """
        create table ${table1} (id int, dcl1 decimal(60,3) sum, dcl2 decimal(70,0) min, dcl3 decimal(70,4) max)
        AGGREGATE KEY(`id`) 
        DISTRIBUTED BY hash(id)
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """

    sql """
        insert into ${table1} values
            (1,1.1,1.2,1.3),
            (1,11.1,11.2,11.3),
            (1,12.1,12.2,12.3),
            (2,2.1,2.2,2.3),  
            (2,21.1,21.2,21.3),  
            (2,22.1,22.2,22.3)  
    """

    sql """sync"""

    order_qt_select """
        select * from ${table1} order by id
    """

    order_qt_agg """
        select sum(dcl1), min(dcl2), max(dcl3) from ${table1} group by id
    """

    sql "drop table if exists ${table1} force"


}
