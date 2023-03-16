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

suite("aggregate_multi_param_sum") {
    def table = "multi_param_sum"
    sql """ DROP TABLE IF EXISTS ${table} """
    sql """
            CREATE TABLE IF NOT EXISTS ${table} (
                id INT,
                a BOOLEAN,
                b SMALLINT,
                c INT,
                d BIGINT,
                e LARGEINT,
                f FLOAT,
                g DOUBLE,
                h DECIMAL,
                i DECIMALV3,
                j DATE,
                k CHAR(5),
                l VARCHAR(8)
            )
            UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
              "replication_num" = "1"
            )
        """
    sql """ INSERT INTO ${table} VALUES (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, '2023-03-01', 'aaaaa', 'aaaaaaaa') """
    sql """ INSERT INTO ${table} VALUES (0, NULL, 2, 3, 4, 5, 6, 7, 8, 9, '2023-03-02', 'bbbbb', 'bbbbbbbb') """
    sql """ INSERT INTO ${table} VALUES (0, 1, NULL, 3, 4, 5, 6, 7, 8, 9, '2023-03-03', 'ccccc', 'cccccccc') """
    sql """ INSERT INTO ${table} VALUES (0, 1, 2, NULL, 4, 5, 6, 7, 8, 9, '2023-03-04', 'ddddd', 'dddddddd') """
    sql """ INSERT INTO ${table} VALUES (0, 1, 2, 3, NULL, 5, 6, 7, 8, 9, '2023-03-05', 'eeeee', 'eeeeeeee') """
    sql """ INSERT INTO ${table} VALUES (0, 1, 2, 3, 4, NULL, 6, 7, 8, 9, '2023-03-06', 'fffff', 'ffffffff') """
    sql """ INSERT INTO ${table} VALUES (0, 1, 2, 3, 4, 5, NULL, 7, 8, 9, '2023-03-07', 'ggggg', 'gggggggg') """
    sql """ INSERT INTO ${table} VALUES (0, 1, 2, 3, 4, 5, 6, NULL, 8, 9, '2023-03-08', 'hhhhh', 'hhhhhhhh') """
    sql """ INSERT INTO ${table} VALUES (0, 1, 2, 3, 4, 5, 6, 7, NULL, 9, '2023-03-09', 'iiiii', 'iiiiiiii') """
    sql """ INSERT INTO ${table} VALUES (0, 1, 2, 3, 4, 5, 6, 7, 8, NULL, '2023-03-10', 'jjjjj', 'jjjjjjjj') """
    sql """ INSERT INTO ${table} VALUES (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, NULL, 'kkkkk', 'kkkkkkkk') """
    sql """ INSERT INTO ${table} VALUES (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, '2023-03-12', NULL, 'llllllll') """
    sql """ INSERT INTO ${table} VALUES (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, '2023-03-13', 'mmmmm', NULL) """


    // without group by
    test {
        sql "select sum(id, a, b, c) from ${table}"
        sql "select sum(b, a, b, c) from ${table}"
        sql "select sum(c, a, b, c) from ${table}"
    }
    // sum of boolean and other no support type
    test {
        sql "select sum(a, b, c) from ${table}"
        exception "sum requires a numeric parameter: sum(`a`, `b`, `c`)"
    }
    test {
        sql "select sum(j, k, l) from ${table}"
        exception "sum requires a numeric parameter: sum(`j`, `k`, `l`)"
    }
    test {
        sql "select sum(k, j, l) from ${table}"
        exception "sum requires a numeric parameter: sum(`k`, `j`, `l`)"
    }

    // with group by
    test {
        sql "select sum(id, a, b, c),d from ${table} group by d"
        sql "select sum(b, a, id, c),d from ${table} group by d"
        sql "select sum(c, g),d from ${table} group by d"
        sql "select sum(g, c),d from ${table} group by d"
        sql "select sum(h, i),d from ${table} group by d"
    }
}
