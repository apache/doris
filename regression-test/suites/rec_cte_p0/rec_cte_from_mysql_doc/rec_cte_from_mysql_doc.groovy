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

import org.codehaus.groovy.runtime.IOGroovyMethods

// https://dev.mysql.com/doc/refman/8.4/en/with.html#common-table-expressions-recursive
suite ("rec_cte_from_mysql_doc") {
    qt_q1 """
    WITH RECURSIVE cte AS
    (
    SELECT cast(1 as int) AS n, cast('abc' as varchar(65533)) AS str
    UNION ALL
    SELECT cast(n + 1 as int), cast(CONCAT(str, str) as varchar(65533)) FROM cte WHERE n < 3
    )
    SELECT * FROM cte order by n;
    """

    qt_q2 """
    WITH RECURSIVE cte AS
    (
    SELECT cast(1 as int) AS n, cast(1 as int) AS p, cast(-1 as int) AS q
    UNION ALL
    SELECT cast(n + 1 as int), cast(q * 2 as int), cast(p * 2 as int) FROM cte WHERE n < 5
    )
    SELECT * FROM cte order by n;
    """

    test {
        sql """
            WITH RECURSIVE cte (n) AS
            (
            SELECT cast(1 as int)
            UNION ALL
            SELECT cast(n + 1 as int) FROM cte
            )
            SELECT n FROM cte order by n;
        """
        exception "ABORTED"
    }
    
    // do not support use limit to stop recursion now
    //qt_q3 """
    //WITH RECURSIVE cte (n) AS
    //(
    //SELECT cast(1 as int)
    //UNION ALL
    //SELECT cast(n + 1 as int) FROM cte LIMIT 10000
    //)
    //SELECT n FROM cte order by n;
    //"""

    sql "DROP TABLE IF EXISTS sales;"
    sql """
        CREATE TABLE sales
        (
            c_date date,
            c_price double
        ) DUPLICATE KEY (c_date)
        DISTRIBUTED BY HASH(c_date) BUCKETS 1 PROPERTIES ('replication_num' = '1');
    """
    sql """insert into sales values
        ('2017-01-03', 100.0),
        ('2017-01-03', 200.0),
        ('2017-01-06', 50.0),
        ('2017-01-08', 10.0),
        ('2017-01-08', 20.0),
        ('2017-01-08', 150.0),
        ('2017-01-10', 5.0);"""

    qt_q4 """
        WITH RECURSIVE dates (c_date) AS
        (
        SELECT MIN(c_date) FROM sales
        UNION ALL
        SELECT c_date + INTERVAL 1 DAY FROM dates
        WHERE c_date + INTERVAL 1 DAY <= (SELECT MAX(c_date) FROM sales)
        )
        SELECT * FROM dates order by 1;
    """

    qt_q5 """
        WITH RECURSIVE dates (c_date) AS
        (
        SELECT MIN(c_date) FROM sales
        UNION ALL
        SELECT c_date + INTERVAL 1 DAY FROM dates
        WHERE c_date + INTERVAL 1 DAY <= (SELECT MAX(c_date) FROM sales)
        )
        SELECT dates.c_date, COALESCE(SUM(c_price), 0) AS sum_price
        FROM dates LEFT JOIN sales ON dates.c_date = sales.c_date
        GROUP BY dates.c_date
        ORDER BY dates.c_date;
    """

    sql "DROP TABLE IF EXISTS employees;"
    sql """
        CREATE TABLE employees (
        id         INT NOT NULL,
        name       VARCHAR(100) NOT NULL,
        manager_id INT NULL
        ) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1');
    """
    sql """INSERT INTO employees VALUES
    (333, "Yasmina", NULL),
    (198, "John", 333),
    (692, "Tarek", 333),
    (29, "Pedro", 198),
    (4610, "Sarah", 29),
    (72, "Pierre", 29),
    (123, "Adil", 692);
    """

    qt_q6 """
        WITH RECURSIVE employee_paths (id, name, path) AS
        (
        SELECT id, name, CAST(id AS varchar(65533))
            FROM employees
            WHERE manager_id IS NULL
        UNION ALL
        SELECT e.id, e.name, cast(CONCAT(ep.path, ',', e.id) as varchar(65533))
            FROM employee_paths AS ep JOIN employees AS e
            ON ep.id = e.manager_id
        )
        SELECT * FROM employee_paths ORDER BY path;
    """
}
