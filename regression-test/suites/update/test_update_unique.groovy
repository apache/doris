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

suite("test_update_unique", "p0") {
    def tbName1 = "test_update_unique_1"
    def tbName2 = "test_update_unique_2"
    def tbName3 = "test_update_unique_3"
    def tbName4 = "test_update_unique_4"
    sql "DROP TABLE IF EXISTS ${tbName1}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName1} (
                k int,
                value1 int,
                value2 int,
                date_value date
            )
            UNIQUE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 5 properties("replication_num" = "1");
        """
    sql "insert into ${tbName1} values(1, 1, 1, '2000-01-01');"
    sql "insert into ${tbName1} values(2, 1, 1, '2000-01-01');"
    sql "UPDATE ${tbName1} SET value1 = 2 WHERE k=1;"
    sql "UPDATE ${tbName1} SET value1 = value1+1 WHERE k=2;"
    sql "UPDATE ${tbName1} SET date_value = '1999-01-01' WHERE k in (1,2);"
    qt_select_uniq_table "select * from ${tbName1} order by k"
    sql "UPDATE ${tbName1} SET date_value = '1998-01-01' WHERE k is null or k is not null;"
    qt_select_uniq_table "select * from ${tbName1} order by k"
    qt_desc_uniq_table "desc ${tbName1}"
    sql "DROP TABLE ${tbName1}"

    sql "DROP TABLE IF EXISTS ${tbName1}"
    sql "DROP TABLE IF EXISTS ${tbName2}"
    sql "DROP TABLE IF EXISTS ${tbName3}"
    sql "DROP TABLE IF EXISTS ${tbName4}"

    // test complex update syntax
    sql """
        create table ${tbName1} (id int, c1 bigint, c2 string, c3 double, c4 date) unique key (id) distributed by hash(id) properties('replication_num'='1');
    """
    sql """
        create table ${tbName2} (id int, c1 bigint, c2 string, c3 double, c4 date) unique key (id) distributed by hash(id) properties('replication_num'='1');
    """
    sql """
        create table ${tbName3} (id int) distributed by hash (id) properties('replication_num'='1');
    """
    sql """
        create table ${tbName4} (id int) distributed by hash (id) properties('replication_num'='1');
    """
    sql """
        insert into ${tbName1} values(1, 1, '1', 1.0, '2000-01-01'),(2, 2, '2', 2.0, '2000-01-02'),(3, 3, '3', 3.0, '2000-01-03');
    """
    sql """
        insert into ${tbName2} values(1, 10, '10', 10.0, '2000-01-10'),(2, 20, '20', 20.0, '2000-01-20'),(3, 30, '30', 30.0, '2000-01-30'),(4, 4, '4', 4.0, '2000-01-04'),(5, 5, '5', 5.0, '2000-01-05');
    """
    sql """
        insert into ${tbName3} values(1), (4), (5);
    """
    sql """
        insert into ${tbName4} values(2), (4), (5);
    """

    sql """
        update ${tbName1} set ${tbName1}.c1 = ${tbName2}.c1, ${tbName1}.c3 = ${tbName2}.c3 * 100 from ${tbName2} inner join ${tbName3} on ${tbName2}.id = ${tbName3}.id where ${tbName1}.id = ${tbName2}.id;
    """

    qt_complex_update """
        select * from ${tbName1} order by id;
    """

    sql """
        update ${tbName1} t1a set t1a.c1 = ${tbName2}.c1, t1a.c3 = ${tbName2}.c3 * 100 from ${tbName2} inner join ${tbName4} on ${tbName2}.id = ${tbName4}.id where t1a.id = ${tbName2}.id;
    """

    qt_complex_update_by_alias """
        select * from ${tbName1} order by id;
    """

    sql "DROP TABLE IF EXISTS ${tbName1}"
    sql "DROP TABLE IF EXISTS ${tbName2}"
    sql "DROP TABLE IF EXISTS ${tbName3}"
    sql "DROP TABLE IF EXISTS ${tbName4}"
}
