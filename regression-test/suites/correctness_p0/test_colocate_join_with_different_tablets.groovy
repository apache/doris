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

suite("test_colocate_join_with_different_tablets") {
    sql """
    DROP TABLE IF EXISTS `USR_V_KHZHSJ_ES_POC1`;
    DROP TABLE IF EXISTS `USR_TLBL_VAL_R1`;
        CREATE TABLE `USR_V_KHZHSJ_ES_POC1` (
          `khid` bigint NULL,
          `khh` bigint NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`khid`, `khh`)
        DISTRIBUTED BY HASH(`khid`) BUCKETS 16
        PROPERTIES (
        "colocate_with" = "test_colocate_join_with_different_tabletsgroup1",
        "replication_allocation" = "tag.location.default: 1"
        );


        CREATE TABLE `USR_TLBL_VAL_R1` (
          `lbl_id` bigint NOT NULL COMMENT "标签ID",
          `khh` bigint NULL COMMENT "客户号"
        ) ENGINE=OLAP
        DUPLICATE KEY(`lbl_id`, `khh`)
        COMMENT '标签结果日表'
        PARTITION BY LIST (`lbl_id`)
        (PARTITION p0 VALUES IN ("0"),
        PARTITION p1 VALUES IN ("1"),
        PARTITION p29 VALUES IN ("29"),
        PARTITION p35 VALUES IN ("35"),
        PARTITION p57 VALUES IN ("57"),
        PARTITION p352 VALUES IN ("352"),
        PARTITION p402 VALUES IN ("402"),
        PARTITION p523 VALUES IN ("523"),
        PARTITION p2347 VALUES IN ("2347"),
        PARTITION p10376 VALUES IN ("10376"),
        PARTITION p42408 VALUES IN ("42408"),
        PARTITION p44410 VALUES IN ("44410"),
        PARTITION p48414 VALUES IN ("48414"),
        PARTITION p50416 VALUES IN ("50416"),
        PARTITION p52418 VALUES IN ("52418"),
        PARTITION p56422 VALUES IN ("56422"),
        PARTITION p60426 VALUES IN ("60426"),
        PARTITION p64430 VALUES IN ("64430"),
        PARTITION p66432 VALUES IN ("66432"),
        PARTITION p70436 VALUES IN ("70436"),
        PARTITION p72438 VALUES IN ("72438"),
        PARTITION p74440 VALUES IN ("74440"),
        PARTITION p78444 VALUES IN ("78444"),
        PARTITION p84450 VALUES IN ("84450"),
        PARTITION p86452 VALUES IN ("86452"),
        PARTITION p88454 VALUES IN ("88454"),
        PARTITION p90456 VALUES IN ("90456"),
        PARTITION p92458 VALUES IN ("92458"),
        PARTITION p94460 VALUES IN ("94460"),
        PARTITION p96462 VALUES IN ("96462"),
        PARTITION p98464 VALUES IN ("98464"),
        PARTITION p100466 VALUES IN ("100466"),
        PARTITION p102468 VALUES IN ("102468"),
        PARTITION p104470 VALUES IN ("104470"),
        PARTITION p106472 VALUES IN ("106472"),
        PARTITION p108474 VALUES IN ("108474"),
        PARTITION p110476 VALUES IN ("110476"),
        PARTITION p112478 VALUES IN ("112478"),
        PARTITION p114480 VALUES IN ("114480"),
        PARTITION p122488 VALUES IN ("122488"),
        PARTITION p124490 VALUES IN ("124490"),
        PARTITION p126492 VALUES IN ("126492"),
        PARTITION p130496 VALUES IN ("130496"),
        PARTITION p134500 VALUES IN ("134500"),
        PARTITION p150516 VALUES IN ("150516"),
        PARTITION p154520 VALUES IN ("154520"),
        PARTITION p158524 VALUES IN ("158524"),
        PARTITION p158525 VALUES IN ("158525"),
        PARTITION p1848141 VALUES IN ("1848141"),
        PARTITION p1848161 VALUES IN ("1848161"),
        PARTITION p1848177 VALUES IN ("1848177"),
        PARTITION p1848197 VALUES IN ("1848197"),
        PARTITION p1848218 VALUES IN ("1848218"))
        DISTRIBUTED BY HASH(`khh`) BUCKETS 16
        PROPERTIES (
        "colocate_with" = "test_colocate_join_with_different_tabletsgroup1",
        "replication_allocation" = "tag.location.default: 1"
        );

        insert into USR_V_KHZHSJ_ES_POC1 values(10000007, 10000007);
        insert into USR_V_KHZHSJ_ES_POC1 values(10000007, 10000007);
        insert into USR_V_KHZHSJ_ES_POC1 values(10000007, 10000007);

        insert into USR_TLBL_VAL_R1 values(48414, 10000007);
        insert into USR_TLBL_VAL_R1 values(94460, 10000007);
        insert into USR_TLBL_VAL_R1 values(60426, 10000007);
    """
    qt_sql """ select  *    from  USR_V_KHZHSJ_ES_POC1  A,USR_TLBL_VAL_R1  B  WHERE  A.khid  =  B.khh order by lbl_id; """
}
