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

suite("test_nestedloop_semi_anti_join", "query_p0") {
    def tbl1 = "test_nestedloop_semi_anti_join1"
    def tbl2 = "test_nestedloop_semi_anti_join2"

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
        select * from ${tbl1} where exists (select * from ${tbl2} where ${tbl1}.user_id >  ${tbl2}.user_id) order by ${tbl1}.user_id;
    """
    qt_join """
        select * from ${tbl1} where not exists (select * from ${tbl2} where ${tbl1}.user_id >  ${tbl2}.user_id) order by ${tbl1}.user_id;
    """
    qt_join """
        select * from ${tbl1} where user_id in (select user_id from ${tbl2} where ${tbl1}.user_id >  ${tbl2}.user_id) order by ${tbl1}.user_id;
    """
    qt_join """
        select * from ${tbl1} where user_id not in (select user_id from ${tbl2} where ${tbl1}.user_id >  ${tbl2}.user_id) order by ${tbl1}.user_id;
    """
    sql """ INSERT INTO ${tbl1} VALUES (1, 1), (2, 2), (3, 3), (10, 10); """
    qt_join """
        select * from ${tbl1} where exists (select * from ${tbl2} where ${tbl1}.user_id >  ${tbl2}.user_id) order by ${tbl1}.user_id;
    """
    qt_join """
        select * from ${tbl1} where not exists (select * from ${tbl2} where ${tbl1}.user_id >  ${tbl2}.user_id) order by ${tbl1}.user_id;
    """
    qt_join """
        select * from ${tbl1} where user_id in (select user_id from ${tbl2} where ${tbl1}.user_id >  ${tbl2}.user_id) order by ${tbl1}.user_id;
    """
    qt_join """
        select * from ${tbl1} where user_id not in (select user_id from ${tbl2} where ${tbl1}.user_id >  ${tbl2}.user_id) order by ${tbl1}.user_id;
    """

    sql """ INSERT INTO ${tbl2} VALUES (2, 2), (3, 3), (4, 4), (0, 0); """
    qt_join """
        select * from ${tbl1} where exists (select * from ${tbl2} where ${tbl1}.user_id >  ${tbl2}.user_id) order by ${tbl1}.user_id;
    """
    qt_join """
        select * from ${tbl1} where not exists (select * from ${tbl2} where ${tbl1}.user_id >  ${tbl2}.user_id) order by ${tbl1}.user_id;
    """
    qt_join """
        select * from ${tbl1} where user_id in (select user_id from ${tbl2} where ${tbl1}.user_id >  ${tbl2}.user_id) order by ${tbl1}.user_id;
    """
    qt_join """
        select * from ${tbl1} where user_id not in (select user_id from ${tbl2} where ${tbl1}.user_id >  ${tbl2}.user_id) order by ${tbl1}.user_id;
    """

    qt_join_mark_join1 """
        select * from ${tbl1} where exists (select * from ${tbl2} where ${tbl1}.user_id >  ${tbl2}.user_id) or ${tbl1}.user_id2 > 3 order by ${tbl1}.user_id;
    """

    qt_join_mark_join2 """
        select * from ${tbl1} where not exists (select * from ${tbl2} where ${tbl1}.user_id >  ${tbl2}.user_id) or ${tbl1}.user_id2 > 3 order by ${tbl1}.user_id;
    """

    sql "DROP TABLE IF EXISTS ${tbl1}"

    sql """
        CREATE TABLE ${tbl1} (id int)
        DISTRIBUTED BY HASH(id)
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    sql "insert into ${tbl1} values (1),(2),(3);"

    qt_nlj_left_semi "select a.id\n" +
            "from ${tbl1} a\n" +
            "join (select id from ${tbl1} where id = (select 1)) b on a.id = b.id"
}
