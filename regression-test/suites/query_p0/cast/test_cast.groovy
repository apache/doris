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

suite('test_cast', "arrow_flight_sql") {
    def date = "date '2020-01-01'"
    def datev2 = "datev2 '2020-01-01'"
    def datetime = "timestamp '2020-01-01 12:34:45'"
    test {
        sql "select cast(${date} as int), cast(${date} as bigint), cast(${date} as float), cast(${date} as double)"
        result([[20200101, 20200101l, ((float) 20200101), ((double) 20200101)]])
    }
    test {
        sql "select cast(${datev2} as int), cast(${datev2} as bigint), cast(${datev2} as float), cast(${datev2} as double)"
        result([[20200101, 20200101l, ((float) 20200101), ((double) 20200101)]])
    }
    test {
        sql "select cast(${datetime} as int), cast(${datetime} as bigint), cast(${datetime} as float), cast(${datetime} as double)"
        result([[869930357, 20200101123445l, ((float) 20200101123445l), ((double) 20200101123445l)]])
    }

    test {
        sql " select cast('9999e-1' as DECIMALV3(2, 1)) "
        result([[9.9]])
    }

    test {
        sql " select cast('100000' as DECIMALV3(2, 1)) "
        result([[9.9]])
    }

    test {
        sql " select cast('-9999e-1' as DECIMALV3(2, 1)) "
        result([[-9.9]])
    }

    // round
    //result([[123456789]])
    qt_sql_1 " select cast('123456789.0' as DECIMALV3(9, 0)) "

    // result([[999999999]])
    qt_sql_2 " select cast('999999999.0' as DECIMALV3(9, 0)) "

    // result([[123456790]])
    qt_sql_3 " select cast('123456789.9' as DECIMALV3(9, 0)) "

    // result([[926895541712428044]])
    qt_sql_4 " select cast('926895541712428044.1' as DECIMALV3(18,0)); "

    // result([[99999999999999999.9]])
    qt_sql_5 " select cast('926895541712428044.1' as DECIMAL(18,1)); "

    // leading-zeros
    qt_sql_decimalv3 """select CAST('0.29401599228723063' AS DECIMALV3)"""

    // not-leading-zeros
    qt_sql_decimalv3 """select CAST('1.29401599228723063' AS DECIMALV3) """

    // not-leading-zeros
    qt_sql_decimalv3 """ select CAST('10.29401599228723063' AS DECIMALV3) """

    // overflow with min value
    qt_sql_decimalv3 """ select cast('-100000' as DECIMALV3(2, 1)) """

    // overflow with max value
    qt_sql_decimalv3 """ select cast('0.2147483648e3' as DECIMALV3(2, 1))"""

    // overflow with min value
    qt_sql_decimalv3 """ select cast('0.2147483648e-3' as DECIMALV3(2, 1))"""

    // decimalv3 with abnormal decimal case ,
    qt_sql_decimalv3 """ select cast('1001-12-31 00:00:00' as DECIMALV3(27, 9))"""

    qt_sql_decimalv3 """ select cast('1001-12-31 00:00:00' as DECIMALV3(9, 0))"""

    qt_sql_decimalv3 """ select cast('1001-12-31 00:00:00' as DECIMALV3(2, 0))"""

    qt_sql_decimalv3 """ select cast('1001-12-31 00:00:00' as DECIMALV3(1, 0))"""

    def tbl = "test_cast"

    sql """ DROP TABLE IF EXISTS ${tbl}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${tbl} (
            `k0` int
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS 5 properties("replication_num" = "1")
        """
    sql """ INSERT INTO ${tbl} VALUES (101);"""

    test {
        sql "select * from ${tbl} where case when k0 = 101 then 1 else 0 end"
        result([[101]])
    }

    test {
        sql "select * from ${tbl} where case when k0 = 101 then 12 else 0 end"
        result([[101]])
    }

    test {
        sql "select * from ${tbl} where case when k0 = 101 then -12 else 0 end"
        result([[101]])
    }

    test {
        sql "select * from ${tbl} where case when k0 = 101 then 0 else 1 end"
        result([])
    }

    test {
        sql "select * from ${tbl} where case when k0 != 101 then 0 else 1 end"
        result([[101]])
    }

    test {
        sql "select * from ${tbl} where case when k0 = 101 then '1' else 0 end"
        result([[101]])
    }

    test {
        sql "select * from ${tbl} where case when k0 = 101 then '12' else 0 end"
        result([])
    }

    test {
        sql "select * from ${tbl} where case when k0 = 101 then 'false' else 0 end"
        result([])
    }

    test {
        sql "select * from ${tbl} where case when k0 = 101 then 'true' else 1 end"
        result([[101]])
    }

    sql "DROP TABLE IF EXISTS test_json"
    sql """
        CREATE TABLE IF NOT EXISTS test_json (
          id INT not null,
          j JSON not null
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES("replication_num" = "1");
    """

    sql """
        INSERT INTO test_json VALUES(26, '{"k1":"v1", "k2": 200}');
    """
    sql "sync"
    sql "Select cast(j as int) from test_json"
    sql "DROP TABLE IF EXISTS test_json"
}
