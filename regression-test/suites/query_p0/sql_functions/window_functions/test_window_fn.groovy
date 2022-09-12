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
suite("test_window_fn") {
    def tbName1 = "empsalary"
    sql """ DROP TABLE IF EXISTS ${tbName1} """

    sql """
        CREATE TABLE ${tbName1}
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

    sql """
        INSERT INTO ${tbName1} (depname, empno, enroll_date, salary) VALUES
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

    // first_value
    qt_sql """
        select first_value(salary) over(order by salary range between UNBOUNDED preceding  and UNBOUNDED following), lead(salary, 1, 0) over(order by salary) as l, salary from ${tbName1} order by l, salary;
    """
    qt_sql """
        select first_value(salary) over(order by enroll_date range between unbounded preceding and UNBOUNDED following), last_value(salary) over(order by enroll_date range between unbounded preceding and UNBOUNDED following), salary, enroll_date from ${tbName1} order by salary, enroll_date;
    """

    // last_value
    qt_sql """
        select last_value(salary) over(order by salary range between UNBOUNDED preceding and UNBOUNDED following), lag(salary, 1, 0) over(order by salary) as l, salary from ${tbName1} order by l, salary;
    """

    // min_max
    qt_sql """
        SELECT empno, depname, salary, bonus, depadj, MIN(bonus) OVER (ORDER BY empno), MAX(depadj) OVER () 
        FROM( SELECT *, CASE WHEN enroll_date < '2008-01-01' THEN 2008 - extract(YEAR FROM enroll_date) END * 500 AS bonus,         
        CASE WHEN AVG(salary) OVER (PARTITION BY depname) < salary THEN 200 END AS depadj FROM ${tbName1})s order by empno;
    """
    qt_sql """
        select max(enroll_date) over (order by enroll_date range between UNBOUNDED preceding and UNBOUNDED following), salary, enroll_date from ${tbName1} order by salary, enroll_date;
    """
    qt_sql """
        select max(enroll_date) over (order by salary range between UNBOUNDED preceding and UNBOUNDED following ), salary, enroll_date from ${tbName1} order by salary, enroll_date;
    """
    qt_sql """
        select max(enroll_date) over (order by enroll_date range between UNBOUNDED preceding and UNBOUNDED following), salary, enroll_date from ${tbName1} order by salary, enroll_date;
    """

    // rank
    qt_sql  """ 
        SELECT depname, empno, salary, rank() OVER (PARTITION BY depname ORDER BY salary) FROM ${tbName1} order by depname
    """
    qt_sql """
        SELECT depname, empno, salary, rank() OVER (PARTITION BY depname ORDER BY salary, empno) 
        FROM ${tbName1} ORDER BY rank() OVER (PARTITION BY depname ORDER BY salary, empno);
    """
    qt_sql """
        SELECT sum(salary) as s, row_number() OVER (ORDER BY depname)  as r, sum(sum(salary)) OVER (ORDER BY depname DESC) as ss 
        FROM ${tbName1} GROUP BY depname order by s, r, ss;
    """
    qt_sql """
        SELECT sum(salary) OVER (PARTITION BY depname ORDER BY salary DESC) as s, rank() OVER (PARTITION BY depname ORDER BY salary DESC) as r 
        FROM ${tbName1} order by s, r;
    """
    qt_sql """
        SELECT * FROM ( select *, row_number() OVER (ORDER BY salary) as a from ${tbName1} ) as t where t.a < 10;
    """

    // sum_avg_count
    qt_sql """
        SELECT depname, empno, salary, sum(salary) OVER (PARTITION BY depname) FROM ${tbName1} order by depname,empno,salary;
    """
    qt_sql """
        SELECT sum(salary) OVER (ORDER BY salary) as s, count(1) OVER (ORDER BY salary) as c FROM ${tbName1} order by s, c;
    """
    qt_sql """
        select sum(salary) over (order by enroll_date range between UNBOUNDED preceding and UNBOUNDED following), salary, enroll_date from ${tbName1} order by salary, enroll_date;
    """
    qt_sql """
        select sum(salary) over (order by enroll_date desc range between UNBOUNDED preceding and UNBOUNDED following), salary, enroll_date from ${tbName1} order by salary, enroll_date;
    """
    qt_sql """
        select sum(salary) over (order by enroll_date desc range between UNBOUNDED preceding and current row) as s, salary, enroll_date from ${tbName1} order by s, salary;
    """
    qt_sql """
        select sum(salary) over (order by enroll_date, salary range between UNBOUNDED preceding and UNBOUNDED  following), salary, enroll_date from ${tbName1} order by salary, enroll_date;
    """
    qt_sql """
        select sum(salary) over (order by depname range between UNBOUNDED  preceding and UNBOUNDED following ), salary, enroll_date from ${tbName1} order by salary, enroll_date;
    """

    sql "DROP TABLE IF EXISTS ${tbName1};"

}

