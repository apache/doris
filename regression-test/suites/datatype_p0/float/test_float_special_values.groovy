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

suite("test_float_special_values", "datatype_p0") {
    def tableName = "tbl_test_float_nan"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "CREATE  TABLE if NOT EXISTS ${tableName} (k int, value float) DUPLICATE KEY(k) DISTRIBUTED BY HASH (k) BUCKETS 1 PROPERTIES ('replication_num' = '1');"
    sql """insert into ${tableName} select 1, sqrt(-1.0);"""

    qt_select "select * from ${tableName} order by 1;"
    qt_select "select sqrt(-1.0);"

    sql "DROP TABLE IF EXISTS ${tableName}"

    sql "drop table if exists test_float_nan_and_inf;"
    sql "create table test_float_nan_and_inf(k1 int, k2 float, k3 float) properties('replication_num' = '1');"
    sql "insert into test_float_nan_and_inf select 1, cast('infinity' as float), cast('infinity' as float);"
    sql "insert into test_float_nan_and_inf select 2, cast('infinity' as float), cast('-infinity' as float);"
    sql "insert into test_float_nan_and_inf select 3, cast('infinity' as float), cast('NaN' as float);"
    sql "insert into test_float_nan_and_inf select 4, cast('infinity' as float), 0;"
    sql "insert into test_float_nan_and_inf select 5, cast('infinity' as float), -0;"
    sql "insert into test_float_nan_and_inf select 6, cast('infinity' as float), 123.456;"
    sql "insert into test_float_nan_and_inf select 7, cast('infinity' as float), -123.456;"
    sql "insert into test_float_nan_and_inf select 8, cast('-infinity' as float), cast('infinity' as float);"
    sql "insert into test_float_nan_and_inf select 9, cast('-infinity' as float), cast('-infinity' as float);"
    sql "insert into test_float_nan_and_inf select 10, cast('-infinity' as float), cast('NaN' as float);"
    sql "insert into test_float_nan_and_inf select 11, cast('-infinity' as float), 0;"
    sql "insert into test_float_nan_and_inf select 12, cast('-infinity' as float), -0;"
    sql "insert into test_float_nan_and_inf select 13, cast('-infinity' as float), 123.456;"
    sql "insert into test_float_nan_and_inf select 14, cast('-infinity' as float), -123.456;"
    sql "insert into test_float_nan_and_inf select 15, 0, cast('infinity' as float);"
    sql "insert into test_float_nan_and_inf select 16, 0, cast('-infinity' as float);"
    sql "insert into test_float_nan_and_inf select 17, -0, cast('infinity' as float);"
    sql "insert into test_float_nan_and_inf select 18, -0, cast('-infinity' as float);"
    sql "insert into test_float_nan_and_inf select 19, 123.456, cast('infinity' as float);"
    sql "insert into test_float_nan_and_inf select 20, 123.456, cast('-infinity' as float);"
    sql "insert into test_float_nan_and_inf select 21, -123.456, cast('infinity' as float);"
    sql "insert into test_float_nan_and_inf select 22, -123.456, cast('-infinity' as float);"
    sql "insert into test_float_nan_and_inf select 23, cast('NaN' as float), cast('infinity' as float);"
    sql "insert into test_float_nan_and_inf select 24, cast('NaN' as float), cast('-infinity' as float);"
    sql "insert into test_float_nan_and_inf values(25, 123.456, 123.456);"
    qt_select_1 "select * from test_float_nan_and_inf order by k1;"

    // https://www.gnu.org/software/libc/manual/html_node/Infinity-and-NaN.html
    // positive infinity is larger than all values except itself and NaN
    // negative infinity is smaller than all values except itself and NaN

    // NaN, infects any calculation that involves it.
    // Unless the calculation would produce the same result no matter what real value replaced NaN,
    // the result is NaN.

    //  NaN is unordered: it is not equal to, greater than, or less than anything, including itself.

    // https://cppreference.com/w/cpp/language/operator_arithmetic.html
    // Built-in additive operators:
    // If one operand is NaN, the result is NaN.
    // Infinity minus infinity is NaN, and FE_INVALID is raised.
    // Infinity plus the negative infinity is NaN, and FE_INVALID is raised.

    // Built-in multiplicative operators:
    // Multiplication of a NaN by any number gives NaN.
    // Multiplication of infinity by zero gives NaN and FE_INVALID is raised.
    //
    // division:
    // If one operand is NaN, the result is NaN.
    // Dividing a non-zero number by ±0.0 gives the correctly-signed infinity and FE_DIVBYZERO is raised.
    // Dividing 0.0 by 0.0 gives NaN and FE_INVALID is raised.

    // Note: for floating-point remainder, see std::remainder and std::fmod.

    // inf arithmetics
    // inf add/sub
    qt_select_inf_add1 "select k1, k2, k3, k2 + k3 from test_float_nan_and_inf order by k1;"
    qt_select_inf_sub1 "select k1, k2, k3, k2 - k3 from test_float_nan_and_inf order by k1;"

    // inf multiply/divide
    // inf division by zero
    // PG:
    // test=# select f1, f1 / 0 from test_float_double_inf;
    // ERROR:  division by zero

    // spark:
    // park-sql (default)> select f1, f1 / 0 from test_float;
    // Infinity        NULL
    // NaN     NULL
    qt_select_inf_multi1 "select k1, k2, k3, k2 * k3 from test_float_nan_and_inf order by k1;"
    qt_select_inf_div1 "select k1, k2, k3, k2 / k3 from test_float_nan_and_inf order by k1;"

    // inf mod
    qt_select_inf_mod1 "select k1, k2, k3, k2 % k3 from test_float_nan_and_inf order by k1;"

    // inf compare
    qt_select_inf_cmp1 "select k1, k2, k3, k2 > k3 from test_float_nan_and_inf order by k1;"
    qt_select_inf_cmp2 "select k1, k2, k3, k2 >= k3 from test_float_nan_and_inf order by k1;"
    qt_select_inf_cmp3 "select k1, k2, k3, k2 < k3 from test_float_nan_and_inf order by k1;"
    qt_select_inf_cmp4 "select k1, k2, k3, k2 <= k3 from test_float_nan_and_inf order by k1;"
    qt_select_inf_cmp5 "select k1, k2, k3, k2 = k3 from test_float_nan_and_inf order by k1;"
    qt_select_inf_cmp6 "select k1, k2, k3, k2 != k3 from test_float_nan_and_inf order by k1;"

    // inf agg
    // inf + normal value
    qt_select_inf_agg1 "select sum(k2), count(k2), avg(k2) from test_float_nan_and_inf where k1 in(1, 15, 19);"
    // -inf + normal value
    qt_select_inf_agg2 "select sum(k2), count(k2), avg(k2) from test_float_nan_and_inf where k1 in(8, 15, 19);"
    // inf + inf
    qt_select_inf_agg3 "select sum(k2), count(k2), avg(k2) from test_float_nan_and_inf where k1 in(1, 2);"
    // inf + -inf
    qt_select_inf_agg4 "select sum(k2), count(k2), avg(k2) from test_float_nan_and_inf where k1 in(1, 8);"
    // inf + nan
    qt_select_inf_agg5 "select sum(k2), count(k2), avg(k2) from test_float_nan_and_inf where k1 in(1, 23);"
    // -inf + nan
    qt_select_inf_agg6 "select sum(k2), count(k2), avg(k2) from test_float_nan_and_inf where k1 in(2, 23);"

    // inf functions
    qt_select_inf_sqrt1 "select k1, k2, sqrt(k2) from test_float_nan_and_inf order by k1;"

    // NaN division by zero
    // PG:
    // test=# select f1, f1 / 0 from test_float_double_nan;
    // f1  | ?column? 
    // ----+----------
    // NaN |      NaN
    // test inf / 0
    // NaN arithmetics
    sql "drop table if exists test_float_nan_and_inf;"
    sql "create table test_float_nan_and_inf(k1 int, k2 float, k3 float) properties('replication_num' = '1');"
    sql "insert into test_float_nan_and_inf select 1, cast('NaN' as float), cast('infinity' as float);"
    sql "insert into test_float_nan_and_inf select 2, cast('NaN' as float), cast('-infinity' as float);"
    sql "insert into test_float_nan_and_inf select 3, cast('NaN' as float), 0;"
    sql "insert into test_float_nan_and_inf select 4, cast('NaN' as float), -0;"
    sql "insert into test_float_nan_and_inf select 5, cast('NaN' as float), 123.456;"
    sql "insert into test_float_nan_and_inf select 6, cast('NaN' as float), -123.456;"
    sql "insert into test_float_nan_and_inf select 7, cast('infinity' as float), cast('NaN' as float);"
    sql "insert into test_float_nan_and_inf select 8, cast('-infinity' as float), cast('NaN' as float);"
    sql "insert into test_float_nan_and_inf select 9, 0, cast('NaN' as float);"
    sql "insert into test_float_nan_and_inf select 10, -0, cast('NaN' as float);"
    sql "insert into test_float_nan_and_inf select 11, 123.456, cast('NaN' as float);"
    sql "insert into test_float_nan_and_inf select 12, -123.456, cast('NaN' as float);"
    sql "insert into test_float_nan_and_inf select 13, cast('NaN' as float), cast('NaN' as float);"
    sql "insert into test_float_nan_and_inf values(14, 123.456, 123.456);"
    // NaN add/sub
    qt_select_NaN_add_sub1 "select k1, k2, k3, k2 + k3 from test_float_nan_and_inf order by k1;"
    qt_select_NaN_add_sub2 "select k1, k2, k3, k2 - k3 from test_float_nan_and_inf order by k1;"

    // NaN multiply/divide
    qt_select_NaN_multi_div1 "select k1, k2, k3, k2 * k3 from test_float_nan_and_inf order by k1;"
    qt_select_NaN_multi_div2 "select k1, k2, k3, k2 / k3 from test_float_nan_and_inf order by k1;"

    // NaN mod
    qt_select_NaN_mod1 "select k1, k2, k3, k2 % k3 from test_float_nan_and_inf order by k1;"

    // NaN agg
    // nan + normal value
    qt_select_NaN_agg1 "select sum(k2), count(k2), avg(k2) from test_float_nan_and_inf where k1 in(1, 9, 11);"
    // nan + nan
    qt_select_NaN_agg2 "select sum(k2), count(k2), avg(k2) from test_float_nan_and_inf where k1 in(1, 2);"
    // nan + inf
    qt_select_NaN_agg3 "select sum(k2), count(k2), avg(k2) from test_float_nan_and_inf where k1 in(1, 7);"
    // nan + -inf
    qt_select_NaN_agg4 "select sum(k2), count(k2), avg(k2) from test_float_nan_and_inf where k1 in(1, 8);"

    // NaN compare
    qt_select_NaN_cmp1 "select k1, k2, k3, k2 > k3 from test_float_nan_and_inf order by k1;"
    qt_select_NaN_cmp2 "select k1, k2, k3, k2 >= k3 from test_float_nan_and_inf order by k1;"
    qt_select_NaN_cmp3 "select k1, k2, k3, k2 < k3 from test_float_nan_and_inf order by k1;"
    qt_select_NaN_cmp4 "select k1, k2, k3, k2 <= k3 from test_float_nan_and_inf order by k1;"
    qt_select_NaN_cmp5 "select k1, k2, k3, k2 = k3 from test_float_nan_and_inf order by k1;"
    qt_select_NaN_cmp6 "select k1, k2, k3, k2 != k3 from test_float_nan_and_inf order by k1;"

    def user = context.config.jdbcUser
    def password = context.config.jdbcPassword
    String url = getServerPrepareJdbcUrl(context.config.jdbcUrl, "regression_test_datatype_p0_float")
    url += "&jdbcCompliantTruncation=false"
    logger.info("jdbc prepare statement url: ${url}")
    def result1 = connect(user, password, url) {
        sql "set global max_prepared_stmt_count = 10000"
        sql "set enable_fallback_to_original_planner = false"
        def stmt_read0 = prepareStatement "select * from test_float_nan_and_inf where k1 > ? order by k1"
        assertEquals(com.mysql.cj.jdbc.ClientPreparedStatement, stmt_read0.class)
        stmt_read0.setInt(1, 0)
        qe_prepared_stmt_select_inf_nan stmt_read0
    }
}
