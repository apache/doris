suite("test_sum_avg_count_window") {
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

    qt_sql  """ 
        SELECT depname, empno, salary, sum(salary) OVER (PARTITION BY depname) 
        FROM ${tbName1} ORDER BY depname, salary 
    """

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

