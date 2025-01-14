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

suite("test_limit") {
    sql """
    drop table if exists test1
    """
    sql """
            CREATE TABLE IF NOT EXISTS test1(
                id int
            ) 
            DISTRIBUTED BY HASH(id) properties("replication_num" = "1");
        """

    sql """ insert into test1 values(1) """
    sql """ insert into test1 values(1) """

    test {
        sql "select * from test1 limit 2 offset 1"
        result([[1]])
    }

    test {
        sql """
            select * from test1 t1 join (select * from test1 limit 1 offset 1) t2
        """
        result([[1,1],[1,1]])
    }

    sql """
    drop table if exists row_number_limit_tbl; 
    """
    sql """
            CREATE TABLE row_number_limit_tbl (
                k1 INT NULL,
                k2 VARCHAR(255) NULL,
                k3 VARCHAR(255) NULL,
                k4 INT NULL,
                k5 VARCHAR(255) NULL,
                k6 FLOAT NULL,
                k7 FLOAT NULL,
                k8 INT NULL
                ) ENGINE=OLAP
                DUPLICATE KEY(k1, k2)
                DISTRIBUTED BY HASH(k1) BUCKETS 3
                PROPERTIES (
                    "replication_allocation" = "tag.location.default: 1"
                );
        """

    sql """ INSERT INTO row_number_limit_tbl VALUES (7788, 'SCOTT', 'ANALYST', 7566, '1987-04-19', 3000, 0, 20); """
    sql """ INSERT INTO row_number_limit_tbl VALUES (7844, 'TURNER', 'SALESMAN', 7698, '1981-09-08', 1500, 0, 30); """

    test {
        sql """
            select row_number() over(order by k6 desc) k6s, t.* from row_number_limit_tbl t order by k6s limit 1 offset 1
        """
        rowNum 1
    }

    sql """ truncate table row_number_limit_tbl; """

    sql """ INSERT INTO row_number_limit_tbl VALUES (7788, 'SCOTT', 'ANALYST', 7566, '1987-04-19', 3000, 0, 20); """
    sql """ INSERT INTO row_number_limit_tbl VALUES (7844, 'TURNER', 'SALESMAN', 7698, '1981-09-08', 1500, 0, 30); """
    sql """ INSERT INTO row_number_limit_tbl VALUES (7934, 'MILLER', 'CLERK', 7782, '1982-01-23', 1300, 0, 10); """

    test {
        sql """
            select row_number() over(order by k6 desc) k6s, t.* from row_number_limit_tbl t limit 1 offset 2
        """
        rowNum 1
    }

    sql """ set parallel_pipeline_task_num = 1; """
    test {
        sql """
            select row_number() over(order by k6 desc) k6s, t.* from row_number_limit_tbl t limit 1 offset 2
        """
        rowNum 1
    }
}
