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

suite("test_equal_for_null") {
    sql """
        DROP TABLE IF EXISTS `test_equal_for_null_tbl`;
    """

    sql """
        CREATE TABLE `test_equal_for_null_tbl` (
          `k1` int,
          `k2` varchar(10),
          `v1` varchar(1024),
          `pk` int
        )
        DISTRIBUTED BY HASH(`pk`) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        insert into `test_equal_for_null_tbl`(`pk`,`k1`,`k2`,`v1`) values
            (0,null,'y',"tell"),
            (1,null,"his",'b'),
            (2,null,"we",'g'),
            (3,null,'n','t'),
            (4,null,"were",'f'),
            (5,null,"i",'q'),
            (6,null,'f','g'),
            (7,null,"time","are"),
            (8,null,'k',"you");
    """

    qt_select " select * from `test_equal_for_null_tbl` where `k1` <=> 4 order by 1, 2, 3, 4; "
    qt_select " select * from `test_equal_for_null_tbl` where `k1` <=> 4 or `k1` <=> 3 order by 1, 2, 3, 4; "
}
