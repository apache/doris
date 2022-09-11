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

suite("test_last_value_window") {
    def tableName = "state"
    def tableName1 = "empsalary"


    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ DROP TABLE IF EXISTS ${tableName1} """
    sql """
            CREATE TABLE ${tableName} (
            `myday` INT,
            `time_col` VARCHAR(40) NOT NULL,
            `state` INT
            ) ENGINE=OLAP
            DUPLICATE KEY(`myday`,time_col,state)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`myday`) BUCKETS 2
            PROPERTIES (
            "replication_num" = "1",
            "in_memory" = "false",
            "storage_format" = "V2"
            );
    """

    sql """
        CREATE TABLE ${tableName1}
        (
            `depname` varchar(20) NULL COMMENT "",
            `empno`  bigint NULL COMMENT "",
            `enroll_date` date NULL COMMENT "",
            `salary` int NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`depname`, `empno`, `enroll_date`)
        COMMENT ""
        DISTRIBUTED BY HASH(`depname`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """ INSERT INTO ${tableName} VALUES 
            (21,"04-21-11",1),
            (22,"04-22-10-21",0),
            (22,"04-22-10-21",1),
            (23,"04-23-10",1),
            (24,"02-24-10-21",1); """

    sql """
        INSERT INTO ${tableName1} (depname, empno, enroll_date, salary) VALUES
        ('develop', 10, '2007-08-01', 5200),
        ('sales', 1, '2006-10-01', 5000),
        ('personnel', 5, '2007-12-10', 3500),
        ('sales', 4, '2007-08-08', 4800),
        ('personnel', 2, '2006-12-23', 3900),
        ('develop', 7, '2008-01-01', 4200),
        ('develop', 9, '2008-01-01', 4500),
        ('sales', 3, '2007-08-01', 4800),
        ('develop', 8, '2006-10-01', 6000),
        ('develop', 11, '2007-08-15', 5200);
    """

    // not_vectorized
    sql """ set enable_vectorized_engine = false; """

    qt_select_default """ select *,last_value(state) over(partition by myday order by time_col) from ${tableName} order by myday, time_col, state; """

    qt_sql """
        select last_value(salary) over(order by salary range between UNBOUNDED preceding and UNBOUNDED following), lag(salary, 1, 0) over(order by salary) as l, salary from ${tableName1} order by l, salary;
    """

    sql "DROP TABLE IF EXISTS ${tableName};"
    sql "DROP TABLE IF EXISTS ${tableName1};"
}
