/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("agg_with_distinct_project") {
    sql "DROP TABLE IF EXISTS agg_with_distinct_project;"
    sql """
        CREATE TABLE agg_with_distinct_project (
          id int NOT NULL,
          a int DEFAULT NULL,
          b int DEFAULT NULL
        )
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """INSERT INTO agg_with_distinct_project VALUES(83,0,38),(26,0,79),(43,81,24)"""

    order_qt_base_case """
        SELECT DISTINCT a as c1 FROM agg_with_distinct_project GROUP BY b, a;
    """

    qt_with_order """
        select distinct  a + 1 from agg_with_distinct_project group by a + 1, b order by a + 1;
    """

    order_qt_with_having """
        select distinct a + 1 from agg_with_distinct_project group by a + 1, b having b > 1;
    """

    qt_with_having_with_order """
        select distinct a + 1 from agg_with_distinct_project group by a + 1, b having b > 1 order by a + 1;
    """

    qt_with_order_with_grouping_sets """
         select distinct  a + 1 from agg_with_distinct_project group by grouping sets(( a + 1, b ), (b + 1)) order by a + 1;
    """

    order_qt_with_having_with_grouping_sets """
         select distinct a + 1 from agg_with_distinct_project group by grouping sets(( a + 1, b ), (b + 1)) having b > 1;
    """

    qt_with_having_with_order_with_grouping_sets """
         select distinct a + 1 from agg_with_distinct_project group by grouping sets(( a + 1, b ), (b + 1)) having b > 1 order by a + 1;
    """

    // order by column not in select list
    test {
        sql """
             select distinct  a + 1 from agg_with_distinct_project group by a + 1, b order by b;
        """
        exception "b of ORDER BY clause is not in SELECT list"
    }

    // order by column not in select list
    test {
        sql """
             select distinct  a + 1 from agg_with_distinct_project group by grouping sets(( a + 1, b ), (b + 1)) order by b;
        """
        exception "b of ORDER BY clause is not in SELECT list"
    }

    sql "DROP TABLE IF EXISTS agg_with_distinct_project;"
}
