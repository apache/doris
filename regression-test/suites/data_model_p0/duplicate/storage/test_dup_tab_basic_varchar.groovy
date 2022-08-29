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


suite("test_dup_tab_basic_varchar") {

    def table1 = "test_dup_tab_basic_varchar_tab"

    sql "drop table if exists ${table1}"

    sql """
    CREATE TABLE `${table1}` (
  `city` varchar(20) NOT NULL COMMENT "",
  `name` varchar(20) NOT NULL COMMENT "",
  `addr` varchar(20) NOT NULL COMMENT "",
  `compy` varchar(20) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`city`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`city`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2"
)
    """

    sql """insert into ${table1} values('a1','a2','a3','a4'),
        ('b1','b2','b3','b4'),
        ('c1','f1','d3','f4'),
        ('c1','c2','c3','c4'),
        ('d1','d2','d3','d4'),
        ('e1','e2','e3','e4')
"""

    // query without predicate
    test {
        // read key
        sql "select city from ${table1} order by city"
        result([['a1'], ['b1'], ['c1'], ['c1'], ['d1'], ['e1']])
    }

    test {
        // read non-key
        sql "select addr from ${table1} order by addr"
        result([['a3'], ['b3'], ['c3'], ['d3'], ['d3'], ['e3']])
    }

    test {
        // read multiple column
        sql "select city,addr from ${table1} order by city,addr"
        result([['a1', 'a3'], ['b1', 'b3'], ['c1', 'c3'], ['c1', 'd3'], ['d1', 'd3'], ['e1', 'e3']])
    }

    // query with predicate
    // key is pred
    test {

        sql "select city from ${table1} where city='e1' order by city"
        result([['e1']])
    }

    test {
        sql "select city from ${table1} where city!='e1' order by city"
        result([['a1'], ['b1'], ['c1'], ['c1'], ['d1']])
    }

    // non-key is pred
    test {

        sql "select addr from ${table1} where addr='d3'"
        result([['d3'], ['d3']])
    }

    test {
        sql "select addr from ${table1} where addr!='d3' order by addr"
        result([['a3'], ['b3'], ['c3'], ['e3']])
    }

    // multiple column
    test {

        sql "select city,addr from ${table1} where city='e1' order by city,addr"
        result([['e1', 'e3']])
    }

    test {

        sql "select city,addr from ${table1} where city!='e1' order by city,addr"
        result([['a1', 'a3'], ['b1', 'b3'], ['c1', 'c3'], ['c1', 'd3'], ['d1', 'd3']])
    }

    test {
        sql "select addr,city from ${table1} where addr='c3'"
        result([['c3', 'c1']])
    }

    test {
        sql "select addr,city from ${table1} where addr!='c3' order by addr,city"
        result([['a3','a1'],['b3','b1'],['d3','c1'],['d3','d1'],['e3','e1']])
    }

    sql "drop table if exists ${table1}"
}
