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

suite("test_dup_tab_mixed_type") {

    def table1 = "test_dup_tab_mixed_type_tab"

    sql "drop table if exists ${table1}"

    sql """

CREATE TABLE `${table1}` (
  `siteid` int(11) NOT NULL COMMENT "",
  `cardid` int(11) NOT NULL COMMENT "",
  `low` double NOT NULL COMMENT "",
  `high` double NOT NULL COMMENT "",
  `cash1` decimal(10, 5) NOT NULL COMMENT "",
  `cash2` decimal(10, 5) NOT NULL COMMENT "",
  `name` varchar(20) NOT NULL COMMENT "",
  `addr` varchar(20) NOT NULL COMMENT ""
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

    sql """insert into ${table1} values(1,2,3.1,4.2,5.3,5.4,'a1','a2'),
            (2,3,4.1,5.2,6.3,7.4,'b1','b2'),
            (3,4,5.1,6.2,7.3,8.4,'c1','c2'),
            (4,5,6.1,7.2,8.3,9.4,'d1','d2'),
            (5,6,5.1,8.2,6.3,11.4,'e1','e2'),
            (5,6,7.1,8.2,9.3,10.4,'d1','e2')
"""

    // read string + int
    test {
        sql "select siteid, cardid, name, addr from ${table1} order by siteid, cardid, name, addr"
        result([[1,2,'a1','a2'],[2,3,'b1','b2'],[3,4,'c1','c2'],[4,5,'d1','d2'],[5,6,'d1','e2'],[5,6,'e1','e2']])
    }

    // predicate is not key
    test {
        sql "select name,addr from ${table1} where cardid=6 order by name,addr "
        result([['d1','e2'],['e1','e2']])
    }

    // string is predicate
    test {
        sql "select siteid,cardid from ${table1} where name='c1'"
        result([[3,4]])
    }

    // predicate contains int and string
    // predicate contains key
    test {
        sql "select siteid,cardid,name,addr from ${table1} where siteid=5 and name='d1'"
        result([[5,6,'d1','e2']])
    }
    // predicate not contains key
    test {
        sql "select siteid,cardid,name,addr from ${table1} where cardid=6 and name='d1'"
        result([[5,6,'d1','e2']])
    }
    // query empty result
    test {
        sql "select siteid,cardid,name,addr from ${table1} where siteid=5 and name='d3'"
        result([])
    }
    test {
        sql "select siteid,cardid,name,addr from ${table1} where siteid=888 and name='d1'"
        result([])
    }

    sql "drop table if exists ${table1}"
}
