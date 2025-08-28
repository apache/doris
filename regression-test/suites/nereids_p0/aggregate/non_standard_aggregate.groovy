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

suite("non_standard_aggregate") {
    sql """
        set sql_mode = "";
    """

    sql """
        DROP TABLE IF EXISTS non_standard_aggregate
    """

    sql """
        DROP TABLE IF EXISTS non_standard_aggregate_2
    """

    sql """
        CREATE TABLE non_standard_aggregate (
          c1 int,
          c2 int,
          c3 string,
          c4 array<int>
        )
        PROPERTIES (
          'replication_num' = '1'
        )
    """

    sql """
        CREATE TABLE non_standard_aggregate_2 (
          c5 int,
          c6 int,
          c7 string,
          c8 array<int>
        )
        PROPERTIES (
          'replication_num' = '1'
        )
    """

    sql """
        INSERT INTO non_standard_aggregate VALUES (1, 2, 'hello,world', [1, 2, 3, 4])
    """

    sql """
        INSERT INTO non_standard_aggregate_2 VALUES (1, 2, 'hello,world', [1, 2, 3, 4])
    """

    // simple case
    sql """SELECT c1, c2, c3 FROM non_standard_aggregate GROUP BY c1"""

    // not group by key
    sql """SELECT c1, sum(c2) FROM non_standard_aggregate"""

    // use two scalar column as group by key
    sql """SELECT c1, c2, c3 FROM non_standard_aggregate GROUP BY c1 + c2"""

    // both group by key and scalar column in one expression
    sql """SELECT c1 + c2, c3 FROM non_standard_aggregate GROUP BY c1"""

    // all scalar column not in group by key
    sql """SELECT c1, c2, c3 FROM non_standard_aggregate GROUP BY c1 + 1"""

    // scalar column with function
    sql """SELECT c1, c2 + 1 FROM non_standard_aggregate GROUP BY c1 + 1"""

    // use two scalar column as group by key
    sql """SELECT c1, c2, c3 FROM non_standard_aggregate GROUP BY c1 + c2 HAVING c1 < 5"""

    // use function with two scalar column as group by key, function with two scalar column in having
    sql """SELECT c1, c2, c3 FROM non_standard_aggregate GROUP BY c1 + c2 HAVING c1 + c2 < 5"""

    // use function with two scalar column as group by key, function with two scalar column in having
    sql """SELECT c1, c2, c3 FROM non_standard_aggregate GROUP BY c1 + c2 HAVING c1 + c3 < 5"""

    // use function with two scalar column as group by key, function with both scalar column and group by key in having
    sql """SELECT c1, c2, c3 FROM non_standard_aggregate GROUP BY c1 + c2 HAVING c1 + c2 + c1 < 10"""

    // both group by key and scalar column in one expression and use it in having
    sql """SELECT c1 + c2, c3 FROM non_standard_aggregate GROUP BY c1 HAVING c1 + c2 < 5"""

    // having with aggregate function but project not
    sql """SELECT c1 FROM non_standard_aggregate HAVING max(c1) = c1"""
    sql """SELECT c2 FROM non_standard_aggregate HAVING max(c1) = c2"""

    // having with both aggregate function and scalar column
    sql """SELECT max(c1) FROM non_standard_aggregate HAVING c1 = count(c1)"""
    sql """SELECT max(c1) FROM non_standard_aggregate HAVING c2 = count(c1)"""

    // lateral view, be do not support any_value(array)
    // sql """SELECT c1, c4, c5 FROM non_standard_aggregate LATERAL VIEW explode(c4) tmp as c5 GROUP BY c1 HAVING c5 < 5"""
    sql """SELECT c1, c5 FROM non_standard_aggregate LATERAL VIEW explode(c4) tmp as c5 GROUP BY c1 HAVING c5 < 5"""

    // join
    sql """SELECT c1, c5 FROM non_standard_aggregate JOIN non_standard_aggregate_2 ON c2 = c6 GROUP BY c1"""

    // filter
    sql """SELECT c1, c2 FROM non_standard_aggregate WHERE c3 = 'hello,world' GROUP BY c2"""

    // window as scalar column
    sql """SELECT c1 + 1, LAG(c2, 0, NULL) OVER(PARTITION BY c1 ORDER BY c3) FROM non_standard_aggregate GROUP BY c1 + 1"""

    // having to filter group by key
    sql """SELECT c1, c2 + 1 FROM non_standard_aggregate GROUP BY c1 + 1 HAVING c1 + 1 < 5"""

    // having to filter scalar column
    sql """SELECT c1, c2 + 1 FROM non_standard_aggregate GROUP BY c1 + 1 HAVING c2 + 1 < 5"""

    // having with neither group by key nor scalar column
    sql """SELECT c1, c2 + 1 FROM non_standard_aggregate GROUP BY c1 + 1 HAVING c2 + 2 < 5"""

    // having to filter window
    sql """SELECT c1 + 1, LAG(c2, 0, NULL) OVER(PARTITION BY c1 ORDER BY c3) AS c3 FROM non_standard_aggregate GROUP BY c1 + 1 HAVING c3 < 10"""

    // order by with group by key
    sql """SELECT c1, c2, c3 FROM non_standard_aggregate GROUP BY c1 ORDER BY c1"""

    // order by with scalar column
    sql """SELECT c1, c2, c3 FROM non_standard_aggregate GROUP BY c1 ORDER BY c2"""

    // order by with function of group by key
    sql """SELECT c1, c2, c3 FROM non_standard_aggregate GROUP BY c1 ORDER BY c1 + 1"""

    // order by with function of scalar column
    sql """SELECT c1, c2, c3 FROM non_standard_aggregate GROUP BY c1 ORDER BY c2 + 1"""

    // order by with aggregate function
    sql """SELECT c1, c2, c3 FROM non_standard_aggregate GROUP BY c1 ORDER BY sum(c2)"""

    // top-n with group by key
    sql """SELECT c1, c2, c3 FROM non_standard_aggregate GROUP BY c1 ORDER BY c1 LIMIT 10"""

    // top-n with scalar column
    sql """SELECT c1, c2, c3 FROM non_standard_aggregate GROUP BY c1 ORDER BY c2 LIMIT 10"""

    // top-n with function of group by key
    sql """SELECT c1, c2, c3 FROM non_standard_aggregate GROUP BY c1 ORDER BY c1 + 1 LIMIT 10"""

    // top-n with function of scalar column
    sql """SELECT c1, c2, c3 FROM non_standard_aggregate GROUP BY c1 ORDER BY c2 + 1 LIMIT 10"""

    // top-n with aggregate function
    sql """SELECT c1, c2, c3 FROM non_standard_aggregate GROUP BY c1 ORDER BY sum(c2) LIMIT 10"""

    // having scalar column + order by aggregate function
    sql """SELECT c1, c2, c3 FROM non_standard_aggregate GROUP BY c1 HAVING c2 < 10 ORDER BY sum(c2)"""

    // having scalar column + order by scalar column
    sql """SELECT c1, c2, c3 FROM non_standard_aggregate GROUP BY c1 HAVING c2 < 10 ORDER BY c3"""

    // having aggregate function + order by scalar column
    sql """SELECT c1, c2, c3 FROM non_standard_aggregate GROUP BY c1 HAVING sum(c2) < 10 ORDER BY c3"""


    // repeat
    sql """SELECT c1, c2, c3 FROM non_standard_aggregate GROUP BY GROUPING SETS((c1), (c1, c2), ())"""

    // repeat with having on group by key
    sql """SELECT c1, c2, c3 FROM non_standard_aggregate GROUP BY GROUPING SETS((c1), (c1, c2), ()) HAVING c1 < 5"""

    // repeat with having on function with group by key
    sql """SELECT c1, c2, c3 FROM non_standard_aggregate GROUP BY GROUPING SETS((c1), (c1, c2), ()) HAVING c1 + 1 < 5"""

    // repeat with having on scalar column
    sql """SELECT c1, c2, c3 FROM non_standard_aggregate GROUP BY GROUPING SETS((c1), (c1, c2), ()) HAVING c3 < 5"""

    // repeat with having on function with scalar column
    sql """SELECT c1, c2, c3 FROM non_standard_aggregate GROUP BY GROUPING SETS((c1), (c1, c2), ()) HAVING c3 + 1 < 5"""

    // repeat with having with neither group by key nor scalar column
    sql """SELECT c1, c2, c3 + 1 FROM non_standard_aggregate GROUP BY GROUPING SETS((c1), (c1, c2), ()) HAVING c3 < 5"""

}
