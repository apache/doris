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
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/aggregate
// and modified by Doris.

suite("aggregate_stddev_over_range") {
    sql """
        CREATE TABLE IF NOT EXISTS TEMPDATA(id INT, data INT) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1');
    """

    sql """
        INSERT INTO TEMPDATA values(1, 1);
    """

    sql """
        CREATE TABLE IF NOT EXISTS DOUBLEDATA(id INT, data DOUBLE ) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1');
    """

    sql """
       INSERT INTO DOUBLEDATA (id, data) VALUES 
        (0, 1),
        (1, 1.1),
        (2, -1),
        (3, 0),
        (4, 0.000000),
        (5, 0.000001),
        (6, 0.0000000000000001),
        (7, -0.7827311820927767),
        (8, -0.6997222019775602),
        (9, 0.6547571144385542),
        (10, -0.9249108129619033),
        (11, -0.27161961813236046),
        (12, -0.8509822016865269),
        (13, -0.025144193739938148),
        (14, -0.7160429045285339),
        (15, 0.8250354873082724),
        (16, 0.8756039443198336),
        (17, 45.123456),
        (18, -123.456789),
        (19, 179.999999),
        (20, 89.999999),
        (21, 1.7976931348623157e+308),
        (22, 2.2250738585072014e-308),
        (23, -1.7976931348623157e+308),
        (24, -2.2250738585072014e-308),
        (25, 1.3407807929942596e+154),
        (26, NULL);
    """

    sql """
        set parallel_pipeline_task_num = 1;
    """

    qt_sql """
        SELECT STDDEV(t.ARG0) FROM (SELECT TEMPDATA . data, TABLE0.ARG0 FROM TEMPDATA CROSS JOIN (SELECT data AS ARG0 FROM DOUBLEDATA) AS TABLE0) t;
    """

    sql """
         set parallel_pipeline_task_num = 48;
    """

    qt_sql """
        SELECT STDDEV(t.ARG0) FROM (SELECT TEMPDATA . data, TABLE0.ARG0 FROM TEMPDATA CROSS JOIN (SELECT data AS ARG0 FROM DOUBLEDATA) AS TABLE0) t;
    """

    sql """
         set parallel_pipeline_task_num = 8;
    """

    qt_sql """
        SELECT STDDEV(t.ARG0) FROM (SELECT TEMPDATA . data, TABLE0.ARG0 FROM TEMPDATA CROSS JOIN (SELECT data AS ARG0 FROM DOUBLEDATA) AS TABLE0) t;
    """

}
