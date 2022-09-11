suite("test_rank_row_number_window") {
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

    sql "DROP TABLE IF EXISTS ${tbName1};"
}

