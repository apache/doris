
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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases
// and modified by Doris.
suite("insert") {
    def tables=["datatype", "mutable_datatype"]

    for (String table in tables) {
        sql """ DROP TABLE IF EXISTS $table """
    }

    for (String table in tables) {
        sql new File("""${context.file.parent}/ddl/${table}.sql""").text
    }

    streamLoad {
        // you can skip declare db, because a default db already specify in ${DORIS_HOME}/conf/regression-conf.groovy
        // db 'regression_test'
        table 'datatype'

        // default label is UUID:
        // set 'label' UUID.randomUUID().toString()

        // default column_separator is specify in doris fe config, usually is '\t'.
        // this line change to ','
        set 'column_separator', '|'

        // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
        // also, you can stream load a http stream, e.g. http://xxx/some.csv
        file 'datetype.csv'

        time 10000 // limit inflight 10s

        // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

        // if declared a check callback, the default check condition will ignore.
        // So you must check all condition
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
            assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
        }
    }

    sql """ insert into mutable_datatype select c_bigint, c_double, c_string, c_date, c_timestamp, c_boolean, c_short_decimal, c_long_decimal from datatype where c_double < 20 """
    sql """ insert into mutable_datatype select 1, c_double, 'abc', cast('2014-01-01' as date), c_timestamp, FALSE, '123.22', '123456789012345678.012345678' from datatype """
    sql """ insert into mutable_datatype select 1, cast(2.2 as double), 'abc', cast('2014-01-01' as date), cast('2015-01-01 03:15:16' as datetime), false, '123.22', '123456789012345678.012345678' from datatype """
    sql """ insert into mutable_datatype select 1, cast(2.1 as double), 'abc', cast('2014-01-01' as date), cast('2015-01-01 03:15:16' as datetime), FALSE, '123.22', '123456789012345678.012345678' """
    sql """ insert into mutable_datatype values (null, null, null, null, null, null, null, null) """
    sql """ insert into mutable_datatype select count(*), cast(1.1 as double), 'a', cast('2016-01-01' as date), cast('2015-01-01 03:15:16' as datetime), FALSE, '-123.22', '-123456789012345678.012345678' from datatype group by c_bigint """
    sql """ insert into mutable_datatype select 5 * c_bigint, c_double + 15, c_string, c_date, c_timestamp, c_boolean, cast((c_short_decimal / '2.00') as decimal(5,2)), cast((c_long_decimal % '10') as decimal(27,9)) from datatype """
    sql """ insert into mutable_datatype select * from datatype """
    sql """ insert into mutable_datatype select * from mutable_datatype """
    sql """ insert into mutable_datatype select * from datatype union all select * from datatype """
    sql """ insert into mutable_datatype select * from datatype order by 1 limit 2 """
    sql """ insert into mutable_datatype select * from datatype where c_bigint < 0 """
    sql """ insert into mutable_datatype values(1,cast(2.34567 as double),'a',cast('2014-01-01' as date), cast ('2015-01-01 03:15:16' as datetime), TRUE, '123.22', '123456789012345678.012345678') """
    sql """ insert into mutable_datatype values(1, cast(2.1 as double), 'abc', cast('2014-01-01' as date), cast('2015-01-01 03:15:16' as datetime), FALSE, '-123.22', '-123456789012345678.012345678') """
    sql """ insert into mutable_datatype values(5 * 10, cast(4.1 + 5 as double), 'abc', cast('2014-01-01' as date), cast('2015-01-01 03:15:16' as datetime), TRUE, '123.22', '123456789012345678.012345678') """

    sql "sync"
    qt_insert """ select * from mutable_datatype order by c_bigint, c_double, c_string, c_date, c_timestamp, c_boolean, c_short_decimal"""


    multi_sql """
        drop table if exists table_select_test1;
        CREATE TABLE table_select_test1 (
          `id` int    
        )
        distributed by hash(id)
        properties('replication_num'='1');

        insert into table_select_test1 values(2);

        drop table if exists table_test_insert1;
        create table table_test_insert1 (id int)
        partition by range(id)
        (
          partition p1 values[('1'), ('50')),
          partition p2 values[('50'), ('100'))
        )
        distributed by hash(id) buckets 100
        properties('replication_num'='1')
        
        insert into table_test_insert1 values(1), (50);
        
        insert into table_test_insert1
        with
          a as (select * from table_select_test1),
          b as (select * from a)
        select id from a;
        """
}
