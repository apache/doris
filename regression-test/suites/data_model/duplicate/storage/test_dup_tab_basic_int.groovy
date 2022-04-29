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

suite("test_dup_tab_basic_int") {

    def table1 = "test_dup_tab_basic_int_tab"

    sql "drop table if exists ${table1}"

    sql """
CREATE TABLE `${table1}` (
  `siteid` int(11) NOT NULL COMMENT "",
  `citycode` int(11) NOT NULL COMMENT "",
  `userid` int(11) NOT NULL COMMENT "",
  `pv` int(11) NOT NULL COMMENT ""
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
        (1,2,3,4),
        (13,21,22,16),
        (13,14,15,16),
        (17,18,19,20),
        (1,2,3,4),
        (13,21,22,16),
        (13,14,15,16),
        (17,18,19,20),
        (5,6,7,8),
        (5,6,7,8)
"""

    // read key column
    test {
        sql """
            select siteid from ${table1} order by siteid
        """
        result([[1],[1],[5],[5],[9],[9],[13],[13],[13],[13],[17],[17]])
    }

    // read non-key column
    test {
        sql """
            select citycode from ${table1} order by citycode
        """
        result([[2], [2], [6], [6], [10], [10], [14], [14], [18], [18], [21], [21]])
    }

    // read key, key is predicate
    test {
        sql """
            select siteid from ${table1} where siteid = 13
        """
        result([[13], [13], [13], [13]])
    }

    test {
        sql """
            select siteid from ${table1} where siteid != 13 order by siteid
        """
        result([[1],[1],[5],[5],[9],[9],[17],[17]])
    }

    // read non-key, non-key is predicate
    test {
        sql """
            select citycode from ${table1} where citycode = 14
        """
        result([[14], [14]])
    }

    test {
        sql """
            select citycode from ${table1} where citycode != 14 order by citycode
        """
        result([[2],[2],[6],[6],[10],[10],[18],[18],[21],[21]])
    }

    // read key, predicate is non-key
    test {
        sql """
            select siteid from ${table1} where citycode = 14
        """
        result([[13], [13]])
    }

    test{
        sql """
            select siteid from ${table1} where citycode != 14 order by siteid
        """
        result([[1],[1],[5],[5],[9],[9],[13],[13],[17],[17]])
    }

    // read non-key, predicate is key
    test {
        sql """
            select citycode from ${table1} where siteid = 13 order by citycode
        """
        result([[14], [14], [21], [21]])
    }

    test {
        sql """
            select citycode from ${table1} where siteid != 13 order by citycode
        """
        result([[2], [2], [6], [6], [10], [10], [18], [18]])
    }

    test {
        sql """
            select siteid,citycode from ${table1} where siteid = 13 order by siteid,citycode
        """
        result([[13, 14], [13, 14], [13, 21], [13, 21]])
    }

    test {
        sql """
            select siteid,citycode from ${table1} where siteid != 13 order by siteid,citycode
        """
        result([[1,2],[1,2],[5,6],[5,6],[9,10],[9,10],[17,18],[17,18]])
    }


    // common read
    test {
        sql """
            select siteid,citycode,userid,pv from ${table1} order by siteid,citycode,pv limit 1
        """
        result([[1,2,3,4]])
    }

    sql "drop table if exists ${table1}"
}
