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

suite("test_dup_tab_basic_int_nullable") {

    def table1 = "test_dup_tab_basic_int_tab_nullable"

    sql "drop table if exists ${table1}"

    sql """
CREATE TABLE `${table1}` (
  `siteid` int(11) NULL COMMENT "",
  `citycode` int(11) NULL COMMENT "",
  `userid` int(11) NULL COMMENT "",
  `pv` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`siteid`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`siteid`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2"
)
"""

    sql """insert into ${table1} values
        (9,10,11,12),
        (9,10,11,12),
        (21,null,23,null),
        (1,2,3,4),
        (1,2,3,4),
        (13,14,15,16),
        (13,21,22,16),
        (13,14,15,16),
        (13,21,22,16),
        (17,18,19,20),
        (17,18,19,20),
        (null,21,null,23),
        (22,null,24,25),
        (26,27,null,29),
        (5,6,7,8),
        (5,6,7,8)
"""

    test {
        // siteid column not contain null
        sql "select siteid,citycode,userid,pv from ${table1} where siteid = 21 "
        result ([[21, null, 23, null]])
    }

    // key is not/is null
    test {
        sql "select siteid,citycode,userid,pv from ${table1} where siteid is null "
        result([[null, 21, null, 23]])
    }

    test {
        sql "select siteid,citycode,userid,pv from ${table1} where siteid is not null order by siteid,citycode,userid,pv"
        result ([
                [1,2,3,4],[1,2,3,4],[5,6,7,8],[5,6,7,8],[9,10,11,12],[9,10,11,12],
                 [13,14,15,16],[13,14,15,16],[13,21,22,16],[13,21,22,16],
            [17,18,19,20],[17,18,19,20],[21,null,23,null],[22,null,24,25],[26,27,null,29]
        ])

    }

    // non-key is null/is not null
    test {
        sql "select siteid,citycode,userid,pv from ${table1} where citycode is null "
        result([[21, null, 23, null], [22, null, 24, 25]])

    }

    test {
        sql "select siteid,citycode,userid,pv from ${table1} where citycode is not null order by siteid,citycode,userid,pv"
        result ([
                [null, 21, null, 23], [1,2,3,4],[1,2,3,4],[5,6,7,8],[5,6,7,8],[9,10,11,12],[9,10,11,12],[13,14,15,16],
        [13,14,15,16],[13,21,22,16],[13,21,22,16],[17,18,19,20],[17,18,19,20],[26,27,null,29]
        ])
    }

    // query column contains null result
    test {
        sql "select siteid from ${table1} order by siteid"
        result([[null], [1], [1], [5], [5], [9], [9], [13], [13], [13], [13], [17], [17], [21], [22], [26]])
    }

    test {
        sql "select citycode from ${table1} order by citycode"
        result([[null], [null], [2], [2], [6], [6], [10], [10], [14], [14], [18], [18], [21], [21], [21], [27]])
    }

    test {
        sql "select siteid,citycode from ${table1} order by siteid,citycode"
        result([[null, 21], [1, 2], [1, 2], [5, 6], [5, 6], [9, 10], [9, 10], [13, 14], [13, 14], [13, 21], [13, 21], [17, 18], [17, 18], [21, null], [22, null], [26, 27]])
    }

    test {
        sql "select userid, citycode from ${table1} order by userid,citycode"
        result([[null,21],[null,27],[3,2],[3,2],[7,6],[7,6],[11,10],[11,10],[15,14],[15,14],[19,18],[19,18],[22,21],[22,21],[23,null],[24,null]])
    }

    // query with pred column
    // query key, pred is key
    test {

        sql "select siteid from ${table1} where siteid!=13 order by siteid"
        result([[1], [1], [5], [5], [9], [9], [17], [17], [21], [22], [26]])
    }

    test {
        sql "select siteid from ${table1} where siteid=13"
        result([[13], [13], [13], [13]])
    }

    // query non key, pred is non-key
    test {
        sql "select citycode from ${table1} where citycode=18"
        result([[18], [18]])
    }

    test {

        sql "select citycode from ${table1} where citycode!=18 order by citycode"
        result([[2], [2], [6], [6], [10], [10], [14], [14], [21], [21], [21], [27]])
    }

    // multiple column
    test {

        sql "select siteid,citycode from ${table1} where siteid=13 order by siteid,citycode"
        result([[13, 14], [13, 14], [13, 21], [13, 21]])
    }

    test {
        sql "select citycode,siteid from ${table1} where siteid=13 order by citycode,siteid"
        result([[14, 13], [14, 13], [21, 13], [21, 13]])
    }

    test {
        sql "select citycode,siteid from ${table1} where siteid!=13 order by citycode,siteid"
        result([[null, 21], [null, 22], [2, 1], [2, 1], [6, 5], [6, 5], [10, 9], [10, 9], [18, 17], [18, 17], [27, 26]])
    }

    test {
        sql "select siteid from ${table1} where siteid!=13 order by siteid"
        result([[1], [1], [5], [5], [9], [9], [17], [17], [21], [22], [26]])
    }

    test {
        sql "select siteid,citycode from ${table1} where citycode=18 order by siteid,citycode"
        result([[17, 18], [17, 18]])
    }

    test {
        sql "select citycode from ${table1} where citycode=18 order by citycode"
        result([[18], [18]])
    }

    test {
        sql "select siteid,citycode from ${table1} where citycode!=18 order by siteid,citycode"
        result([[null, 21], [1, 2], [1, 2], [5, 6], [5, 6], [9, 10], [9, 10], [13, 14], [13, 14], [13, 21], [13, 21], [26, 27]])
    }

    test {
        sql "select citycode,siteid from ${table1} where citycode!=18 order by citycode,siteid"
        result([[2, 1], [2, 1], [6, 5], [6, 5], [10, 9], [10, 9], [14, 13], [14, 13], [21, null], [21, 13], [21, 13], [27, 26]])
    }

    test {
        sql "select citycode from ${table1} where citycode!=18 order by citycode"
        result([[2],[2],[6],[6],[10],[10],[14],[14],[21],[21],[21],[27]])
    }

    sql "drop table if exists ${table1}"
}
