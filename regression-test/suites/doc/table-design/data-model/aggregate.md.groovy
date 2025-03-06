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

import org.junit.jupiter.api.Assertions;

suite("docs/table-design/data-model/aggregate.md") {
    try {
        multi_sql """
        CREATE TABLE IF NOT EXISTS example_tbl_agg1
        (
            `user_id` LARGEINT NOT NULL COMMENT "用户id",
            `date` DATE NOT NULL COMMENT "数据灌入日期时间",
            `city` VARCHAR(20) COMMENT "用户所在城市",
            `age` SMALLINT COMMENT "用户年龄",
            `sex` TINYINT COMMENT "用户性别",
            `last_visit_date` DATETIME REPLACE DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
            `cost` BIGINT SUM DEFAULT "0" COMMENT "用户总消费",
            `max_dwell_time` INT MAX DEFAULT "0" COMMENT "用户最大停留时间",
            `min_dwell_time` INT MIN DEFAULT "99999" COMMENT "用户最小停留时间"
        )
        AGGREGATE KEY(`user_id`, `date`, `city`, `age`, `sex`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 3"
        );
        """

        multi_sql """
        insert into example_tbl_agg1 values
        (10000,"2017-10-01","北京",20,0,"2017-10-01 06:00:00",20,10,10),
        (10000,"2017-10-01","北京",20,0,"2017-10-01 07:00:00",15,2,2),
        (10001,"2017-10-01","北京",30,1,"2017-10-01 17:05:45",2,22,22),
        (10002,"2017-10-02","上海",20,1,"2017-10-02 12:59:12",200,5,5),
        (10003,"2017-10-02","广州",32,0,"2017-10-02 11:20:00",30,11,11),
        (10004,"2017-10-01","深圳",35,0,"2017-10-01 10:00:15",100,3,3),
        (10004,"2017-10-03","深圳",35,0,"2017-10-03 10:20:22",11,6,6);
        """

        multi_sql """
        insert into example_tbl_agg1 values
        (10004,"2017-10-03","深圳",35,0,"2017-10-03 11:22:00",44,19,19),
        (10005,"2017-10-03","长沙",29,1,"2017-10-03 18:11:02",3,1,1);
        """

        sql "drop table if exists aggstate"
        multi_sql """
        set enable_agg_state=true;
        create table aggstate(
            k1 int null,
            k2 agg_state<sum(int)> generic,
            k3 agg_state<group_concat(string)> generic
        )
        aggregate key (k1)
        distributed BY hash(k1) buckets 3
        properties("replication_num" = "1");
        """

        multi_sql """
        insert into aggstate values(1,sum_state(1),group_concat_state('a'));
        insert into aggstate values(1,sum_state(2),group_concat_state('b'));
        insert into aggstate values(1,sum_state(3),group_concat_state('c'));
        """

        multi_sql "insert into aggstate values(2,sum_state(4),group_concat_state('d'));"
        multi_sql "select sum_merge(k2) from aggstate;"
        multi_sql "select group_concat_merge(k3) from aggstate;"
        multi_sql "insert into aggstate select 3,sum_union(k2),group_concat_union(k3) from aggstate;"
        multi_sql """
        select sum_merge(k2) , group_concat_merge(k3)from aggstate;
        select sum_merge(k2) , group_concat_merge(k3)from aggstate where k1 != 2;
        """
    } catch (Throwable t) {
        Assertions.fail("examples in docs/table-design/data-model/aggregate.md failed to exec, please fix it", t)
    }
}
