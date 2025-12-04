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

package org.apache.doris.nereids.parser;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class NereidsParserDigestTest extends ParserTestBase {

    private void assertDigestEquals(String expected,
            List<Pair<LogicalPlan, StatementContext>> logicalPlanList) {
        String digest = logicalPlanList.get(0).first.toDigest();
        Assertions.assertEquals(expected, digest);
    }

    @Test
    public void testDigest() {
        NereidsParser nereidsParser = new NereidsParser();
        // test simple query
        String sql
                = "SELECT (a+1) as b, c as d, abs(f) as f FROM test where not exists(select d from test2) "
                + "and c in (1,2,3) and e in (select e from test3) and d is null "
                + "and f = (select f from testf) and e = [1, 3, 5];";
        List<Pair<LogicalPlan, StatementContext>> logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals("SELECT a + ? AS b, c AS d, ABS(f) AS f FROM test WHERE "
                        + "(NOT EXISTS (SELECT d FROM test2) AND c IN (?) AND e IN (SELECT e FROM test3) "
                        + "AND d IS NULL AND f = (SELECT f FROM testf) AND e = [?])",
                logicalPlanList);

        // test group by and order by
        sql = "select a,b from test_table group by 1,2 order by 1";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals("SELECT a, b FROM test_table GROUP BY 1, 2 ORDER BY 1 ASC NULLS FIRST",
                logicalPlanList);

        // test explain
        sql = "explain select a from test";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals("EXPLAIN SELECT a FROM test", logicalPlanList);

        // test variable
        sql = "SELECT @@session.auto_increment_increment AS auto_increment_increment, @query_timeout AS query_timeout;";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals(
                "SELECT @@auto_increment_increment AS auto_increment_increment, @query_timeout AS query_timeout",
                logicalPlanList);

        // test one row relation
        sql = "select 100, 'value'";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals("SELECT ?, ?", logicalPlanList);

        // test select tablet
        sql = "select * from test tablet(1024)";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals("SELECT * FROM test TABLET(?)", logicalPlanList);

        // test except
        sql = "select * except(age) from student;";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals("SELECT * EXCEPT(age) FROM student", logicalPlanList);

        // test lateral view
        sql = "SELECT * FROM person LATERAL VIEW EXPLODE(ARRAY(30, 60)) tableName AS c_age;";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals("SELECT * FROM person LATERAL VIEW EXPLODE(ARRAY(?, ?)) tableName AS c_age",
                logicalPlanList);

        // test lambda
        sql = "SELECT ARRAY_MAP(x->x+1, ARRAY(87, 33, -49))";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals("SELECT ARRAY_MAP(x -> x + ?, ARRAY(?, ?, ?)) "
                + "AS ARRAY_MAP(x->x+1, ARRAY(87, 33, -49))", logicalPlanList);

        // test set operation
        sql = "SELECT student_id, name\n"
                + "FROM students\n"
                + "EXCEPT\n"
                + "SELECT student_id, name\n"
                + "FROM graduated_students;";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals("(SELECT student_id, name FROM students) "
                + "EXCEPT DISTINCT (SELECT student_id, name FROM graduated_students)", logicalPlanList);

        sql = "SELECT student_id, name\n"
                + "FROM math_students\n"
                + "INTERSECT\n"
                + "SELECT student_id, name\n"
                + "FROM physics_students;";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals("(SELECT student_id, name FROM math_students) "
                + "INTERSECT DISTINCT (SELECT student_id, name FROM physics_students)", logicalPlanList);

        sql = "SELECT student_id, name, age\n"
                + "FROM class1_students\n"
                + "UNION\n"
                + "SELECT student_id, name, age\n"
                + "FROM class2_students;";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals("(SELECT student_id, name, age FROM class1_students) "
                + "UNION DISTINCT (SELECT student_id, name, age FROM class2_students)", logicalPlanList);

        // test tvf
        sql = "SELECT cast(id as INT) as id, name, cast (age as INT) as age FROM "
                + "s3(\"s3.access_key\"= \"ak\", \"s3.secret_key\" = \"sk\");";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals("SELECT cast(id as INT) AS id, name, cast(age as INT) AS age FROM s3(?)", logicalPlanList);

        // test filter + subquery + in list
        sql = "select id, concat(firstname, ' ', lastname) as fullname from student where age in (18,20,25) and "
                + "name like('_h%') and id in (select id from application) order by id desc limit 1,3;";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals("SELECT id, CONCAT(firstname, ?, lastname) AS fullname FROM student "
                        + "WHERE (age IN (?) AND name like ? AND id IN (SELECT id FROM application)) "
                        + "ORDER BY id DESC LIMIT ?  OFFSET ?",
                logicalPlanList);

        // test agg
        sql = "select sum(price) as total,type, count(1) as t_count from tb_book where level > 1 group by type;";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals(
                "SELECT SUM(price) AS total, type, COUNT(?) AS t_count FROM tb_book WHERE level > ? GROUP BY type",
                logicalPlanList);

        // test distinct agg
        sql = "select distinct type, id from tb_book group by type;";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        // NOTE: this is special for distinct group by
        assertDigestEquals("SELECT DISTINCT * FROM tb_book GROUP BY type", logicalPlanList);

        sql = "select type, id from tb_book group by type;";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals("SELECT type, id FROM tb_book GROUP BY type", logicalPlanList);

        // test distinct
        sql = "select distinct type, id from tb_book";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals("SELECT DISTINCT type, id FROM tb_book", logicalPlanList);

        // test case when
        sql = "select d_week_seq,\n"
                + "        sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,\n"
                + "        sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,\n"
                + "        sum(case when (d_day_name='Tuesday') then sales_price else  null end) tue_sales,\n"
                + "        sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,\n"
                + "        sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,\n"
                + "        sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales,\n"
                + "        sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales\n"
                + " from wscs\n"
                + "     ,date_dim\n"
                + " where d_date_sk = sold_date_sk\n"
                + " group by d_week_seq";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals(
                "SELECT d_week_seq, "
                        + "SUM(CASE WHEN d_day_name = ? THEN sales_price ELSE ? END) AS sun_sales, "
                        + "SUM(CASE WHEN d_day_name = ? THEN sales_price ELSE ? END) AS mon_sales, "
                        + "SUM(CASE WHEN d_day_name = ? THEN sales_price ELSE ? END) AS tue_sales, "
                        + "SUM(CASE WHEN d_day_name = ? THEN sales_price ELSE ? END) AS wed_sales, "
                        + "SUM(CASE WHEN d_day_name = ? THEN sales_price ELSE ? END) AS thu_sales, "
                        + "SUM(CASE WHEN d_day_name = ? THEN sales_price ELSE ? END) AS fri_sales, "
                        + "SUM(CASE WHEN d_day_name = ? THEN sales_price ELSE ? END) AS sat_sales "
                        + "FROM wscs CROSS_JOIN date_dim WHERE d_date_sk = sold_date_sk GROUP BY d_week_seq",
                logicalPlanList);

        // test cte
        sql = "WITH\n"
                + "  cte1 AS (SELECT a，b FROM table1),\n"
                + "  cte2 AS (SELECT c，d FROM table2)\n"
                + "SELECT b，d FROM cte1 JOIN cte2\n"
                + "WHERE cte1.a = cte2.c;";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals("WITH\n"
                + "(SELECT a，b FROM table1) AS cte1, (SELECT c，d FROM table2) AS cte2\n"
                + "SELECT b，d FROM cte1 CROSS_JOIN cte2 WHERE cte1.a = cte2.c", logicalPlanList);

        // test hint
        sql = "select /*+ leading(t1 {t2 t3}) */ * from t1 left join t2 on c1 = c2 join t3 on c2 = c3;";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals("SELECT * FROM t1 LEFT_OUTER_JOIN t2 ON c1 = c2 INNER_JOIN t3 ON c2 = c3", logicalPlanList);

        // test using join
        sql = "SELECT order_id, name, order_date\n"
                + "FROM orders\n"
                + "JOIN customers\n"
                + "USING (customer_id);";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals("SELECT order_id, name, order_date FROM orders "
                + "INNER_JOIN customers USING (customer_id)", logicalPlanList);

        // test window function
        sql = "select k1, sum(k2), rank() over(partition by k1 order by k1) as ranking from t1 group by k1";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals(
                "SELECT k1, SUM(k2) AS sum(k2), RANK() OVER (PARTITION BY k1 ORDER BY k1 ASC NULLS FIRST) AS ranking FROM t1 GROUP BY k1",
                logicalPlanList);

        // test binary keyword
        sql = "SELECT BINARY 'abc' FROM t";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals("SELECT ? FROM t", logicalPlanList);

        // test rollup
        sql = "SELECT a, b, sum(c) from test group by a ASC, b ASC WITH ROLLUP";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals(
                "SELECT a, b, SUM(c) AS sum(c) FROM test GROUP BY GROUPING SETS ((a,b), (a), ()) ORDER BY a ASC NULLS FIRST, b ASC NULLS FIRST",
                logicalPlanList);
        sql = "SELECT a, b from test group by a, b WITH ROLLUP";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals("SELECT a, b FROM test GROUP BY GROUPING SETS ((a,b), (a), ())", logicalPlanList);
        sql = "SELECT distinct a from test group by a, b WITH ROLLUP";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals("SELECT DISTINCT * FROM test GROUP BY GROUPING SETS ((a,b), (a), ())", logicalPlanList);

        // test qualify
        sql = "select country, sum(profit) as total, row_number() over (order by country) as rk from sales "
                + "where year >= 2000 group by country having sum(profit) > 100 qualify rk = 1";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals("SELECT country, SUM(profit) AS total, "
                        + "ROW_NUMBER() OVER (ORDER BY country ASC NULLS FIRST) AS rk "
                        + "FROM sales WHERE year >= ? GROUP BY country HAVING SUM(profit) > ? QUALIFY rk = ?",
                logicalPlanList);

        sql = "select country, sum(profit) as total from sales where year >= 2000 group by country "
                + "having sum(profit) > 100 qualify row_number() over (order by country) = 1";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals("SELECT country, SUM(profit) AS total FROM sales WHERE year >= ? GROUP BY country "
                        + "HAVING SUM(profit) > ? QUALIFY ROW_NUMBER() OVER (ORDER BY country ASC NULLS FIRST) = ?",
                logicalPlanList);

        // test export
        sql = "EXPORT TABLE test\n"
                + "WHERE k1 < 50\n"
                + "TO \"s3://bucket/export\"\n"
                + "PROPERTIES (\n"
                + "    \"columns\" = \"k1,k2\",\n"
                + "    \"column_separator\"=\",\"\n"
                + ") WITH s3 (\n"
                + "    \"s3.endpoint\" = \"xxxxx\",\n"
                + "    \"s3.region\" = \"xxxxx\"\n"
                + ")";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals("EXPORT TABLE test WHERE k1 < ? TO ?", logicalPlanList);

        // test insert
        ConnectContext ctx = ConnectContext.get();
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setRemoteIP("127.0.0.1");
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setDatabase("mysql");

        ctx.setThreadLocalInfo();
        sql = "INSERT INTO test VALUES (1, 2);";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals("INSERT INTO test VALUES ?", logicalPlanList);

        sql = "INSERT INTO test (c1, c2) VALUES (1, 2), (3, 2 * 2);";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals("INSERT INTO test (c1, c2) VALUES ?", logicalPlanList);

        sql = "INSERT INTO test PARTITION(p1, p2) WITH LABEL `label1` SELECT * FROM test2;";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        assertDigestEquals("INSERT INTO test SELECT * FROM test2", logicalPlanList);

        sql = "INSERT OVERWRITE table test (c1, c2) VALUES (1, 2), (3, 2 * 2);";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        // special for insert overwrite
        assertDigestEquals("OVERWRITE TABLE INSERT INTO test (c1, c2) VALUES ?", logicalPlanList);

        sql = "INSERT OVERWRITE table test (c1, c2) SELECT * from test2;";
        logicalPlanList = nereidsParser.parseMultiple(sql);
        // special for insert overwrite
        assertDigestEquals("OVERWRITE TABLE INSERT INTO test (c1, c2) SELECT * FROM test2", logicalPlanList);
    }
}
