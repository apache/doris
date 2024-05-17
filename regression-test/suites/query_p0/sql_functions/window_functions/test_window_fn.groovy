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
    // "arrow_flight_sql", groovy use flight sql connection to execute query `SUM(MAX(c1) OVER (PARTITION BY))` report error:
    // `AGGREGATE clause must not contain analytic expressions`, but no problem in Java execute it with `jdbc::arrow-flight-sql`.
    def tbName1 = "empsalary"
    def tbName2 = "tenk1"
    sql """ DROP TABLE IF EXISTS ${tbName1} """
    sql """ DROP TABLE IF EXISTS ${tbName2} """

    sql """
        CREATE TABLE IF NOT EXISTS ${tbName1}
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
        CREATE TABLE IF NOT EXISTS ${tbName2} (
        stringu1 varchar(20) NULL COMMENT "",
        stringu2 varchar(20) NULL COMMENT "",
        string4 varchar(20) NULL COMMENT "",
        unique1 int NULL COMMENT "",
        unique2 int NULL COMMENT "",
        two int NULL COMMENT "",
        four int NULL COMMENT "",
        ten int NULL COMMENT "",
        twenty int NULL COMMENT "",
        hundred int NULL COMMENT "",
        thousand int NULL COMMENT "",
        twothousand int NULL COMMENT "",
        fivethous int NULL COMMENT "",
        tenthous int NULL COMMENT "",
        odd int NULL COMMENT "",
        even int NULL COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`stringu1`, `stringu2`, `string4`)
        COMMENT ""
        DISTRIBUTED BY HASH(`stringu1`, `stringu2`, `string4`) BUCKETS 1
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

    // stream load data to table tenk
    streamLoad {
        // you can skip declare db, because a default db already specify in ${DORIS_HOME}/conf/regression-conf.groovy
        // db 'regression_test'
        table tbName2

        // default label is UUID:
        // set 'label' UUID.randomUUID().toString()

        // default column_separator is specify in doris fe config, usually is '\t'.
        // this line change to ','
        // set 'column_separator', '|'

        // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
        // also, you can stream load a http stream, e.g. http://xxx/some.csv
        file 'tenk.data'

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

    // first_value
    qt_sql """
        select first_value(salary) over(order by salary range between UNBOUNDED preceding  and UNBOUNDED following), lead(salary, 1, 0) over(order by salary) as l, salary from ${tbName1} order by 1, l, salary;
    """
    qt_sql """
        select
            first_value(salary) over(order by enroll_date range between unbounded preceding and UNBOUNDED following)
            , last_value(salary) over(order by enroll_date range between unbounded preceding and UNBOUNDED following)
            , salary, enroll_date
        from ${tbName1}
        order by 1, 2, salary, enroll_date;
    """
    qt_sql """
        SELECT first_value(ten) OVER (PARTITION BY four ORDER BY ten), ten, four FROM ${tbName2} WHERE unique2 < 10 order by 1, four, ten;
    """
    qt_sql """
        SELECT first_value(unique1) over (order by four range between current row and unbounded following),  
        last_value(unique1) over (order by four, unique1 desc range between current row and unbounded following), unique1, four
        FROM ${tbName2} WHERE unique1 < 10 order by 1, 2, unique1, four;
    """

    // last_value
    qt_sql """
        select last_value(salary) over(order by salary range between UNBOUNDED preceding and UNBOUNDED following), lag(salary, 1, 0) over(order by salary) as l, salary from ${tbName1} order by 1, l, salary;
    """
    qt_sql """
        SELECT last_value(ten) OVER (ORDER BY ten), ten, four FROM ${tbName2} WHERE unique2 < 10 order by 1, 2, 3;
    """
    qt_sql """
        SELECT last_value(ten) OVER (PARTITION BY four ORDER BY ten), ten, four FROM
        (SELECT * FROM ${tbName2} WHERE unique2 < 10 ORDER BY four, ten)s ORDER BY 1, 2, four, ten;
    """
    qt_sql """
        SELECT four, ten, sum(ten) over (partition by four order by ten), last_value(ten) over (partition by four order by ten) 
        FROM (select distinct ten, four from ${tbName2}) ss order by four, ten, 3, 4;
    """
    qt_sql """
        SELECT four, ten, sum(ten) over (partition by four order by ten range between unbounded preceding and current row), 
        last_value(ten) over (partition by four order by ten range between unbounded preceding and current row) 
        FROM (select distinct ten, four from ${tbName2}) ss order by four, ten, 3, 4;
    """
    qt_sql """
        SELECT four, ten, sum(ten) over (partition by four order by ten range between unbounded preceding and unbounded following), 
        last_value(ten) over (partition by four order by ten range between unbounded preceding and unbounded following) 
        FROM (select distinct ten, four from ${tbName2}) ss order by four, ten, 3, 4;
    """
    qt_sql """
        SELECT four, ten/4 as two, sum(ten/4) over (partition by four order by ten/4 range between unbounded preceding and current row), 
        last_value(ten/4) over (partition by four order by ten/4 range between unbounded preceding and current row) 
        FROM (select distinct ten, four from ${tbName2}) ss order by 1, 2, four, two;
    """
    qt_sql """
        SELECT four, ten/4 as two, sum(ten/4) over (partition by four order by ten/4 rows between unbounded preceding and current row), 
        last_value(ten/4) over (partition by four order by ten/4 rows between unbounded preceding and current row) 
        FROM (select distinct ten, four from ${tbName2}) ss order by four, two, 3, 4;
    """


    // min_max
    qt_sql """
        SELECT empno, depname, salary, bonus, depadj, MIN(bonus) OVER (ORDER BY empno), MAX(depadj) OVER () 
        FROM( SELECT *, CASE WHEN enroll_date < '2008-01-01' THEN 2008 - extract(YEAR FROM enroll_date) END * 500 AS bonus,         
        CASE WHEN AVG(salary) OVER (PARTITION BY depname) < salary THEN 200 END AS depadj FROM ${tbName1})s order by empno, depname, salary, bonus, depadj;
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
        FROM ${tbName1} ORDER BY rank() OVER (PARTITION BY depname ORDER BY salary, empno), depname;
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
    qt_sql """
        SELECT row_number() OVER (ORDER BY unique2) FROM ${tbName2} WHERE unique2 < 10;
    """
    qt_sql """
        SELECT rank() OVER (PARTITION BY four ORDER BY ten) AS rank_1, ten, four FROM ${tbName2} WHERE unique2 < 10 order by four, ten;
    """
    qt_sql """
        SELECT dense_rank() OVER (PARTITION BY four ORDER BY ten), ten, four FROM ${tbName2} WHERE unique2 < 10 order by four, ten;
    """
    qt_sql """
        SELECT percent_rank() OVER (PARTITION BY four ORDER BY ten), ten, four FROM ${tbName2} WHERE unique2 < 10 order by four, ten;
    """
    qt_sql """
        SELECT cume_dist() OVER (PARTITION BY four ORDER BY ten), ten, four FROM ${tbName2} WHERE unique2 < 10 order by four, ten;
    """
    qt_sql """
        select ten,   sum(unique1) + sum(unique2) as res,   rank() over (order by sum(unique1) + sum(unique2)) as rank from ${tbName2} group by ten order by ten;
    """


    // sum_avg_count
    qt_sql_sum_avg_count_1 """
        SELECT depname, empno, salary, sum(salary) OVER (PARTITION BY depname) FROM ${tbName1} order by depname,empno,salary;
    """
    qt_sql_sum_avg_count_2 """
        SELECT sum(salary) OVER (ORDER BY salary) as s, count(1) OVER (ORDER BY salary) as c FROM ${tbName1} order by s, c;
    """
    qt_sql_sum_avg_count_3 """
        select sum(salary) over (order by enroll_date range between UNBOUNDED preceding and UNBOUNDED following), salary, enroll_date from ${tbName1} order by salary, enroll_date;
    """
    qt_sql_sum_avg_count_4 """
        select sum(salary) over (order by enroll_date desc range between UNBOUNDED preceding and UNBOUNDED following), salary, enroll_date from ${tbName1} order by salary, enroll_date;
    """
    qt_sql_sum_avg_count_5 """
        select sum(salary) over (order by enroll_date desc range between UNBOUNDED preceding and current row) as s, salary, enroll_date from ${tbName1} order by s, salary;
    """
    qt_sql_sum_avg_count_6 """
        select sum(salary) over (order by enroll_date, salary range between UNBOUNDED preceding and UNBOUNDED  following), salary, enroll_date from ${tbName1} order by salary, enroll_date;
    """
    qt_sql_sum_avg_count_7 """
        select sum(salary) over (order by depname range between UNBOUNDED  preceding and UNBOUNDED following ), salary, enroll_date from ${tbName1} order by salary, enroll_date;
    """
    qt_sql_sum_sum """
        SELECT four, ten, SUM(SUM(four)) OVER (PARTITION BY four), AVG(ten) FROM ${tbName2}
        GROUP BY four, ten ORDER BY four, ten;
    """
    qt_sql_count """
        SELECT COUNT(1) OVER () FROM ${tbName2} WHERE unique2 < 10;
    """
    qt_sql_sum """
        SELECT sum(four) OVER (PARTITION BY ten ORDER BY unique2) AS sum_1, ten, four FROM ${tbName2} WHERE unique2 < 10 order by ten, four, sum_1;
    """
    qt_sql """
        SELECT ten, two, sum(hundred) AS gsum, sum(sum(hundred)) OVER (PARTITION BY two ORDER BY ten) AS wsum FROM ${tbName2} GROUP BY ten, two order by gsum, wsum;
    """
    qt_sql """
        SELECT count(1) OVER (PARTITION BY four) as c, four FROM (SELECT * FROM ${tbName2} WHERE two = 1)s WHERE unique2 < 10 order by c, four;
    """
    qt_sql """
        SELECT avg(four) OVER (PARTITION BY four ORDER BY thousand / 100) FROM ${tbName2} WHERE unique2 < 10 order by four;
    """
    qt_sql """
        SELECT count(1) OVER (PARTITION BY four) FROM (SELECT * FROM ${tbName2} WHERE FALSE)s;
    """
    qt_sql """
        SELECT sum(unique1) over (order by four range between current row and unbounded following) as s, unique1, four 
        FROM ${tbName2} WHERE unique1 < 10 order by s, unique1;
    """
    qt_sql """
        SELECT count() OVER () FROM ${tbName2} limit 5;
    """


    // ntile
    qt_sql_ntile_1 """
        SELECT ntile(3) OVER (ORDER BY ten, four), ten, four FROM ${tbName2} WHERE unique2 < 10;
    """


    // lag
    qt_sql_lag_1 """
        SELECT lag(ten, 1, 0) OVER (PARTITION BY four ORDER BY ten), ten, four FROM ${tbName2} WHERE unique2 < 10 order by four, ten, 1;
    """


    // lead
    qt_sql_lead_1 """
        SELECT lead(ten, 1, 0) OVER (PARTITION BY four ORDER BY ten), ten, four FROM ${tbName2} WHERE unique2 < 10 order by four, ten, 1;
    """
    qt_sql_lead_2 """
        SELECT lead(ten * 2, 1, 0) OVER (PARTITION BY four ORDER BY ten), ten, four FROM ${tbName2} WHERE unique2 < 10 order by four, ten, 1;
    """
    qt_sql_lead_3 """
        SELECT lead(ten * 2, 1, -1) OVER (PARTITION BY four ORDER BY ten), ten, four FROM ${tbName2} WHERE unique2 < 10 order by four, ten, 1;
    """


    // sub query
    qt_sql """
        SELECT * FROM(   SELECT count(1) OVER (PARTITION BY four ORDER BY ten) + sum(hundred) OVER (PARTITION BY two ORDER BY ten) AS total,     
        count(1) OVER (PARTITION BY four ORDER BY ten) AS fourcount, 
        sum(hundred) OVER (PARTITION BY two ORDER BY ten) AS twosum FROM ${tbName2} )sub 
        WHERE total <> fourcount + twosum;
    """

    // cte
    qt_sql_cte_1 """
        with cte as (select empno as x from ${tbName1}) 
        SELECT x, (sum(x) over  (ORDER BY x range between UNBOUNDED preceding and UNBOUNDED following)) FROM cte;
    """
    qt_sql_cte_2 """
        with cte as (select empno as x from ${tbName1}) 
        SELECT x, (sum(x) over  (ORDER BY x range between UNBOUNDED preceding and CURRENT ROW)) FROM cte;
    """
    qt_sql_cte_3 """
        WITH cte  AS (
        select 1 as x union all select 1 as x union all select 1 as x union all
        SELECT empno as x FROM ${tbName1})
        SELECT x, (sum(x) over  (ORDER BY x rows between 1 preceding and 1 following)) FROM cte;
    """

    sql "DROP TABLE IF EXISTS ${tbName1};"
    sql "DROP TABLE IF EXISTS ${tbName2};"

    sql """ DROP TABLE IF EXISTS example_window_tb """
    sql """
        CREATE TABLE IF NOT EXISTS example_window_tb (
        u_id int NULL COMMENT "",
        u_city varchar(20) NULL COMMENT "",
        u_salary int NULL COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`u_id`, `u_city`, `u_salary`)
        COMMENT ""
        DISTRIBUTED BY HASH(`u_id`, `u_city`, `u_salary`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
    );
    """

    sql """
        INSERT INTO example_window_tb(u_id, u_city, u_salary) VALUES
        ('1',  'gz', 30000),
        ('2',  'gz', 25000),
        ('3',  'gz', 17000),
        ('4',  'gz', 32000),
        ('5',  'sz', 40000),
        ('6',  'sz', 27000),
        ('7',  'sz', 27000),
        ('8',  'sz', 33000);
     """

    qt_sql_window_last_value """
        select u_id, u_city, u_salary,
        last_value(u_salary) over (partition by u_city order by u_id rows between unbounded preceding and 1 preceding) last_value_test
        from example_window_tb order by u_id;
    """

    sql """
        create view v as select row_number() over(partition by u_city order by u_salary) as wf from example_window_tb    
    """

    sql """
        drop view v
    """

    sql "DROP TABLE IF EXISTS example_window_tb;"

    sql """
        CREATE TABLE IF NOT EXISTS test_window_in_agg
        (
            `c1`  int ,
            `c2`  int ,
            `c3`  int
        )
        ENGINE=OLAP
        DUPLICATE KEY(`c1`)
        COMMENT ""
        DISTRIBUTED BY HASH(`c1`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
        """
    sql """set enable_nereids_planner=true;"""
    sql """SELECT SUM(MAX(c1) OVER (PARTITION BY c2, c3)) FROM  test_window_in_agg;"""

    sql "DROP TABLE IF EXISTS test_window_in_agg;"
}


