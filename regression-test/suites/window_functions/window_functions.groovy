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
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/window_functions
// and modified by Doris.

def customerTableName="tpch_tiny_customer";
def lineitemTableName="tpch_tiny_lineitem";
def regionTableName="tpch_tiny_region";
def nationTableName="tpch_tiny_nation";
def partTableName="tpch_tiny_part";
def supplierTableName="tpch_tiny_supplier";

sql """ DROP TABLE IF EXISTS ${regionTableName} """
sql """
    CREATE TABLE IF NOT EXISTS ${regionTableName} (
        r_regionkey  INTEGER NOT NULL,
        r_name       CHAR(25) NOT NULL,
        r_comment    VARCHAR(152)
    )
    DUPLICATE KEY(r_regionkey)
    DISTRIBUTED BY HASH(r_regionkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    )
"""

sql """ DROP TABLE IF EXISTS ${nationTableName} """
sql """
    CREATE TABLE IF NOT EXISTS ${nationTableName} (
        nationkey  BIGINT NOT NULL,
        name       VARCHAR(25) NOT NULL,
        regionkey  BIGINT NOT NULL,
        comment    VARCHAR(152)
    )
    DUPLICATE KEY(nationkey)
    DISTRIBUTED BY HASH(nationkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    )
"""

sql """ DROP TABLE IF EXISTS ${partTableName} """
sql """
    CREATE TABLE IF NOT EXISTS ${partTableName} (
        p_partkey     INTEGER NOT NULL,
        p_name        VARCHAR(55) NOT NULL,
        p_mfgr        CHAR(25) NOT NULL,
        p_brand       CHAR(10) NOT NULL,
        p_type        VARCHAR(25) NOT NULL,
        p_size        INTEGER NOT NULL,
        p_container   CHAR(10) NOT NULL,
        p_retailprice DECIMAL(15,2) NOT NULL,
        p_comment     VARCHAR(23) NOT NULL
    )
    DUPLICATE KEY(p_partkey)
    DISTRIBUTED BY HASH(p_partkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    )
"""

sql """ DROP TABLE IF EXISTS ${supplierTableName} """
sql """
    CREATE TABLE IF NOT EXISTS ${supplierTableName} (
        s_suppkey     INTEGER NOT NULL,
        s_name        CHAR(25) NOT NULL,
        s_address     VARCHAR(40) NOT NULL,
        s_nationkey   INTEGER NOT NULL,
        s_phone       CHAR(15) NOT NULL,
        s_acctbal     DECIMAL(15,2) NOT NULL,
        s_comment     VARCHAR(101) NOT NULL
    )
    DUPLICATE KEY(s_suppkey)
    DISTRIBUTED BY HASH(s_suppkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    )
"""

sql """ DROP TABLE IF EXISTS ${lineitemTableName} """
sql """
    CREATE TABLE IF NOT EXISTS ${lineitemTableName} (
        orderkey   bigint,
        linenumber   integer,
        partkey   bigint,
        suppkey   bigint,
        quantity   double,
        extendedprice   double,
        discount   double,
        tax   double,
        returnflag   varchar(1),
        linestatus   varchar(1),
        shipdate   date,
        commitdate   date,
        receiptdate   date,
        shipinstruct   varchar(25),
        shipmode   varchar(10),
        comment   varchar(44)
        )
    DUPLICATE KEY(orderkey, linenumber)
    DISTRIBUTED BY HASH(orderkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    )
"""

streamLoad {
    // you can skip declare db, because a default db already specify in ${DORIS_HOME}/conf/regression-conf.groovy
    // db 'regression_test'
    table regionTableName

    // default label is UUID:
    // set 'label' UUID.randomUUID().toString()

    // default column_separator is specify in doris fe config, usually is '\t'.
    // this line change to ','
    set 'column_separator', ','
    // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
    // also, you can stream load a http stream, e.g. http://xxx/some.csv
    file 'sf0.01/region.csv'

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

streamLoad {
    // you can skip declare db, because a default db already specify in ${DORIS_HOME}/conf/regression-conf.groovy
    // db 'regression_test'
    table lineitemTableName

    // default label is UUID:
    // set 'label' UUID.randomUUID().toString()

    // default column_separator is specify in doris fe config, usually is '\t'.
    // this line change to ','
    set 'column_separator', ','
    // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
    // also, you can stream load a http stream, e.g. http://xxx/some.csv
    file 'sf0.01/lineitem.csv'

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

streamLoad {
    // you can skip declare db, because a default db already specify in ${DORIS_HOME}/conf/regression-conf.groovy
    // db 'regression_test'
    table nationTableName

    // default label is UUID:
    // set 'label' UUID.randomUUID().toString()

    // default column_separator is specify in doris fe config, usually is '\t'.
    // this line change to ','
    set 'column_separator', ','
    // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
    // also, you can stream load a http stream, e.g. http://xxx/some.csv
    file 'sf0.01/nation.csv'

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

// different with trino
qt_window_functions """
select orderkey, suppkey,
discount,
lead(discount,1,NULL) over (partition by suppkey order by orderkey desc) next_discount,
extendedprice,
lag(extendedprice,1,NULL) over (partition by discount order by extendedprice) previous_extendedprice
from ${lineitemTableName} where partkey = 272
"""

// different with trino
qt_window_functions """
select orderkey, suppkey, discount,
rank() over (partition by suppkey)
from ${lineitemTableName} where partkey = 272
"""

qt_window_functions """
SELECT nationkey, min(nationkey) OVER (PARTITION BY regionkey ORDER BY comment RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min FROM ${nationTableName}
"""
// qt_window_functions """
// select orderkey, discount, extendedprice,
// min(extendedprice) over (order by discount range current row) min_extendedprice,
// max(extendedprice) over (order by discount range current row) max_extendedprice
// from ${lineitemTableName} where partkey = 272
// """

qt_window_functions """
SELECT nationkey, min(nationkey) OVER (PARTITION BY regionkey ORDER BY comment RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS min FROM ${nationTableName}
"""

// qt_window_functions """
// select orderkey, discount,
// dense_rank() over (order by discount),
// rank() over (order by discount range between unbounded preceding and current row)
// from ${lineitemTableName} where partkey = 272
// """

// different with trino
// qt_window_functions """
// SELECT nationkey, min(nationkey) OVER (PARTITION BY regionkey ORDER BY nationkey ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) AS min FROM ${nationTableName}
// """

qt_window_functions """
select orderkey, suppkey,
extendedprice,
round(sum(extendedprice) over (partition by suppkey order by orderkey desc rows between unbounded preceding and current row), 5) total_extendedprice,
discount,
round(avg(discount) over (partition by suppkey order by orderkey asc rows between unbounded preceding and current row), 5)  avg_discount
from ${lineitemTableName} where partkey = 272
"""

qt_window_functions """
select orderkey, suppkey,
quantity,
round(sum(quantity) over (partition by suppkey order by orderkey rows between unbounded preceding and 1 preceding), 5) total_quantity,
extendedprice,
round(sum(extendedprice) over (partition by suppkey order by orderkey rows between current row and 1 following), 5)
total_extendedprice,
discount,
round(avg(discount) over (partition by suppkey order by orderkey rows between 3 following and unbounded following), 5)  avg_discount
from ${lineitemTableName} where partkey = 272
"""

qt_window_functions """
select orderkey, suppkey, extendedprice,
first_value(extendedprice) over (partition by suppkey order by extendedprice desc rows between unbounded preceding and unbounded following),
last_value(extendedprice) over (partition by suppkey order by extendedprice desc rows between unbounded preceding and unbounded following)
from ${lineitemTableName} where partkey = 272
"""

qt_window_functions """
select orderkey, suppkey, quantity,
round(sum(quantity) over (partition by suppkey order by orderkey rows between 2 following and 3 following), 5) total_quantity
from ${lineitemTableName} where partkey = 272
"""

qt_window_functions """
select orderkey, suppkey, quantity,
round(sum(quantity) over (partition by suppkey order by orderkey rows between 2 following and unbounded following), 5) total_quantity
from ${lineitemTableName} where partkey = 272
"""

qt_window_functions """
select orderkey, suppkey, quantity,
round(sum(quantity) over (partition by suppkey order by orderkey rows between 3 preceding and 2 preceding), 5) total_quantity
from ${lineitemTableName} where partkey = 272
"""

qt_window_functions """
select orderkey, suppkey, quantity,
round(sum(quantity) over (partition by suppkey order by orderkey rows between unbounded preceding and 2 preceding), 5) total_quantity
from ${lineitemTableName} where partkey = 272
"""

qt_window_functions """
select orderkey, suppkey,
quantity,
round(sum(quantity) over (partition by suppkey order by orderkey rows between unbounded preceding and current row), 5) total_quantity,
extendedprice,
round(sum(extendedprice) over (partition by suppkey order by orderkey rows between unbounded preceding and current row), 5)
total_extendedprice,
discount,
round(avg(discount) over (partition by suppkey order by orderkey rows between unbounded preceding and current row), 5)  avg_discount
from ${lineitemTableName} where partkey = 272
"""

qt_window_functions """
select orderkey, suppkey,
quantity,
round(sum(quantity) over (partition by suppkey order by orderkey rows between 3 preceding and 1 following), 5) total_quantity,
extendedprice,
round(sum(extendedprice) over (partition by suppkey order by orderkey rows between 1 preceding and 2 following), 5)
total_extendedprice,
discount,
round(avg(discount) over (partition by suppkey order by orderkey rows between current row and unbounded following), 5)  avg_discount
from ${lineitemTableName} where partkey = 272
"""

qt_window_functions """
select orderkey, suppkey, quantity,
round(sum(quantity) over (partition by suppkey order by orderkey rows unbounded preceding), 5) total_quantity
from ${lineitemTableName} where partkey = 272
"""

qt_window_functions """
SELECT nationkey, min(nationkey) OVER (PARTITION BY regionkey ORDER BY comment ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS min FROM ${nationTableName}
"""

// qt_window_functions """
// select
// suppkey, orderkey, partkey,
// round(sum(quantity) over (partition by suppkey order by orderkey rows between unbounded preceding and current row), 3) sum_quantity_A,
// round(sum(discount) over (partition by suppkey order by orderkey rows between unbounded preceding and current row), 3) sum_discount_A,
// ntile(4) over (partition by partkey order by orderkey rows between UNBOUNDED preceding and CURRENT ROW) ntile_quantity_B
// 
// from ${lineitemTableName} where (partkey = 272 or partkey = 273) and suppkey > 50
// """

qt_window_functions """
select
suppkey, orderkey, partkey,
round(sum(quantity) over (partition by suppkey order by orderkey rows between unbounded preceding and current row), 3) sum_quantity_A,
first_value(quantity) over (partition by suppkey order by orderkey rows between unbounded preceding and current row) first_value_quantity_A,
round(sum(discount) over (partition by suppkey order by orderkey rows between unbounded preceding and current row), 3) sum_discount_A

from ${lineitemTableName} where (partkey = 272 or partkey = 273) and suppkey > 50
"""

qt_window_functions """
select
suppkey, orderkey, partkey,
round(sum(quantity) over (partition by suppkey order by orderkey rows between unbounded preceding and current row), 3) sum_quantity_A,
round(sum(quantity) over (partition by orderkey order by shipdate rows between UNBOUNDED preceding and CURRENT ROW), 3) sum_quantity_B,
round(sum(discount) over (partition by suppkey order by orderkey rows between unbounded preceding and current row), 3) sum_discount_A

from ${lineitemTableName} where (partkey = 272 or partkey = 273) and suppkey > 50
"""

qt_window_functions """
select
suppkey, orderkey, partkey,
round(sum(quantity) over (partition by suppkey order by orderkey rows between unbounded preceding and current row), 3) sum_quantity_A,
lag(quantity, 1, 0.0) over (partition by partkey order by orderkey) lag_quantity_B,
round(sum(discount) over (partition by suppkey order by orderkey rows between unbounded preceding and current row), 3) sum_discount_A

from ${lineitemTableName} where (partkey = 272 or partkey = 273) and suppkey > 50
"""

// qt_window_functions """
// select
// suppkey, orderkey, partkey,
// nth_value(quantity, 4) over (partition by partkey order by orderkey rows between UNBOUNDED preceding and CURRENT ROW) nth_value_quantity_B,
// round(sum(quantity) over (partition by suppkey order by orderkey rows between unbounded preceding and current row), 3) sum_quantity_A,
// round(sum(discount) over (partition by suppkey order by orderkey rows between unbounded preceding and current row), 3) sum_discount_A

// from ${lineitemTableName} where (partkey = 272 or partkey = 273) and suppkey > 50
// """
