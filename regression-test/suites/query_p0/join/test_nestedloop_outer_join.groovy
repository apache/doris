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

suite("test_nestedloop_outer_join", "query_p0") {
    def tbl1 = "test_nestedloop_outer_join1"
    def tbl2 = "test_nestedloop_outer_join2"

    sql "DROP TABLE IF EXISTS ${tbl1}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbl1} (
                `user_id` LARGEINT NOT NULL COMMENT "",
                `user_id2` LARGEINT NOT NULL COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """

    sql "DROP TABLE IF EXISTS ${tbl2}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbl2} (
                `user_id` LARGEINT NOT NULL COMMENT "",
                `user_id2` LARGEINT NOT NULL COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """

    qt_join """
        select * from ${tbl1} full outer join ${tbl2} on ${tbl1}.user_id < ${tbl2}.user_id order by ${tbl1}.user_id, ${tbl2}.user_id;
    """

    qt_join """
        select * from ${tbl1} right outer join ${tbl2} on ${tbl1}.user_id < ${tbl2}.user_id order by ${tbl1}.user_id, ${tbl2}.user_id;
    """

    qt_join """
        select * from ${tbl1} left outer join ${tbl2} on ${tbl1}.user_id < ${tbl2}.user_id order by ${tbl1}.user_id, ${tbl2}.user_id;
    """

    qt_join """
        select * from ${tbl1} inner join ${tbl2} on ${tbl1}.user_id < ${tbl2}.user_id order by ${tbl1}.user_id, ${tbl2}.user_id;
    """
    sql """ INSERT INTO ${tbl1} VALUES (1, 1), (2, 2), (3, 3), (10, 10); """
    qt_join """
        select * from ${tbl1} full outer join ${tbl2} on ${tbl1}.user_id < ${tbl2}.user_id order by ${tbl1}.user_id, ${tbl2}.user_id;
    """

    qt_join """
        select * from ${tbl1} right outer join ${tbl2} on ${tbl1}.user_id < ${tbl2}.user_id order by ${tbl1}.user_id, ${tbl2}.user_id;
    """

    qt_join """
        select * from ${tbl1} left outer join ${tbl2} on ${tbl1}.user_id < ${tbl2}.user_id order by ${tbl1}.user_id, ${tbl2}.user_id;
    """

    qt_join """
        select * from ${tbl1} inner join ${tbl2} on ${tbl1}.user_id < ${tbl2}.user_id order by ${tbl1}.user_id, ${tbl2}.user_id;
    """

    qt_join """
        select * from ${tbl2} full outer join ${tbl1} on ${tbl1}.user_id < ${tbl2}.user_id order by ${tbl1}.user_id, ${tbl2}.user_id;
    """

    qt_join """
        select * from ${tbl2} right outer join ${tbl1} on ${tbl1}.user_id < ${tbl2}.user_id order by ${tbl1}.user_id, ${tbl2}.user_id;
    """

    qt_join """
        select * from ${tbl2} left outer join ${tbl1} on ${tbl1}.user_id < ${tbl2}.user_id order by ${tbl1}.user_id, ${tbl2}.user_id;
    """

    qt_join """
        select * from ${tbl2} inner join ${tbl1} on ${tbl1}.user_id < ${tbl2}.user_id order by ${tbl1}.user_id, ${tbl2}.user_id;
    """


    sql """ INSERT INTO ${tbl2} VALUES (2, 2), (3, 3), (4, 4), (0, 0); """
    qt_join """
        select * from ${tbl1} full outer join ${tbl2} on ${tbl1}.user_id < ${tbl2}.user_id order by ${tbl1}.user_id, ${tbl2}.user_id;
    """

    qt_join """
        select * from ${tbl1} right outer join ${tbl2} on ${tbl1}.user_id < ${tbl2}.user_id order by ${tbl1}.user_id, ${tbl2}.user_id;
    """

    qt_join """
        select * from ${tbl1} left outer join ${tbl2} on ${tbl1}.user_id < ${tbl2}.user_id order by ${tbl1}.user_id, ${tbl2}.user_id;
    """

    qt_join """
        select * from ${tbl1} inner join ${tbl2} on ${tbl1}.user_id < ${tbl2}.user_id order by ${tbl1}.user_id, ${tbl2}.user_id;
    """

    sql """ INSERT INTO ${tbl2} VALUES (2, 1); """

    qt_join """
        select * from ${tbl1} full outer join ${tbl2} on ${tbl1}.user_id < ${tbl2}.user_id where ${tbl1}.user_id2 = ${tbl2}.user_id2 order by ${tbl1}.user_id, ${tbl2}.user_id;
    """

    qt_join """
        select * from ${tbl1} right outer join ${tbl2} on ${tbl1}.user_id < ${tbl2}.user_id where ${tbl1}.user_id2 = ${tbl2}.user_id2 order by ${tbl1}.user_id, ${tbl2}.user_id;
    """

    qt_join """
        select * from ${tbl1} left outer join ${tbl2} on ${tbl1}.user_id < ${tbl2}.user_id where ${tbl1}.user_id2 = ${tbl2}.user_id2 order by ${tbl1}.user_id, ${tbl2}.user_id;
    """

    qt_join """
        select * from ${tbl1} inner join ${tbl2} on ${tbl1}.user_id < ${tbl2}.user_id where ${tbl1}.user_id2 = ${tbl2}.user_id2 order by ${tbl1}.user_id, ${tbl2}.user_id;
    """

    sql "DROP TABLE IF EXISTS ${tbl1}"
    sql "DROP TABLE IF EXISTS ${tbl2}"
}
