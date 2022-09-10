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

suite("test_min_max_window") {
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

    // vectorized
    sql """ set enable_vectorized_engine = true; """

    qt_select_default """ select *,min(state) over(partition by myday order by time_col rows between current row and 3 following) from ${tableName} order by myday, time_col, state; """
    qt_select_default """ select *,max(state) over(partition by myday order by time_col rows between current row and 3 following) from ${tableName} order by myday, time_col, state; """

    qt_sql """
        SELECT empno, depname, salary, bonus, depadj, MIN(bonus) OVER (ORDER BY empno), MAX(depadj) OVER () 
        FROM( SELECT *, CASE WHEN enroll_date < '2008-01-01' THEN 2008 - extract(YEAR FROM enroll_date) END * 500 AS bonus,         
        CASE WHEN AVG(salary) OVER (PARTITION BY depname) < salary THEN 200 END AS depadj FROM ${tableName1})s order by empno;
    """

    qt_sql """
        select max(enroll_date) over (order by enroll_date range between UNBOUNDED preceding and UNBOUNDED following), salary, enroll_date from ${tableName1} order by salary, enroll_date;
    """

    qt_sql """
        select max(enroll_date) over (order by salary range between UNBOUNDED preceding and UNBOUNDED following ), salary, enroll_date from ${tableName1} order by salary, enroll_date;
    """

    qt_sql """
        select max(enroll_date) over (order by enroll_date range between UNBOUNDED preceding and UNBOUNDED following), salary, enroll_date from ${tableName1} order by salary, enroll_date;
    """

    sql "DROP TABLE IF EXISTS ${tableName};"
    sql "DROP TABLE IF EXISTS ${tableName1};"
}
