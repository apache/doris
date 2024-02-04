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

suite("test_plsql") {
    // TODO:
    //    1. doris parser support declare var
    //    2. Stmt.statement() support insert into var, impl Stmt.getIntoCount(), Stmt.populateVariable()

    // def tbl = "plsql_tbl"
    // sql "DROP TABLE IF EXISTS ${tbl}"
    // sql """
    //     create table ${tbl} (id int, name varchar(20)) DUPLICATE key(`id`) distributed by hash (`id`) buckets 4
    //     properties ("replication_num"="1");
    //     """

    // sql "declare id INT default = 0;"
    // sql """
    //     CREATE OR REPLACE PROCEDURE procedure_insert(IN name STRING, OUT result int)
    //     BEGIN
    //       select k1 into result from test_query_db.test where k7 = name;        
    //     END;
    //     """
    // sql "call procedure_insert('wangynnsf', id)"
    // qt_select "select * from test_query_db.test where k1 = id"

    // sql """
    //     CREATE OR REPLACE PROCEDURE cursor_demo()
    //     BEGIN
    //       DECLARE a CHAR(32);
    //       DECLARE b, c INT;
    //       DECLARE cur1 CURSOR FOR SELECT k7, k3 FROM test_query_db.test where k3 > 0 order by k3, k7;
    //       DECLARE cur2 CURSOR FOR SELECT k4 FROM test_query_db.baseall where k4 between 0 and 21011903 order by k4;

    //       OPEN cur1;
    //       OPEN cur2;

    //       read_loop: LOOP
    //           FETCH cur1 INTO a, b;
    //           IF(SQLCODE != 0) THEN
    //             LEAVE read_loop;
    //           END IF;
    //           FETCH cur2 INTO c;
    //           IF(SQLCODE != 0) THEN
    //             LEAVE read_loop;
    //           END IF;
    //           IF b < c THEN
    //             INSERT INTO ${tbl} (`name`,`id`) VALUES (a,b);
    //           ELSE
    //             INSERT INTO ${tbl} (`name`, `id`) VALUES (a,c);
    //           END IF;
    //       END LOOP;

    //       CLOSE cur1;
    //       CLOSE cur2;
    //     END;
    //     """

    // sql "call cursor_demo()"
    // qt_select """select * from ${tbl} order by 1, 2""";
}
