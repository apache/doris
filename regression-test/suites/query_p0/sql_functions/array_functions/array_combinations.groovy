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

suite("array_combinations") {
    sql """DROP TABLE IF EXISTS t_array_combinations"""
    sql """
            CREATE TABLE IF NOT EXISTS t_array_combinations (
              `k1` int(11) NULL COMMENT "",
              `s1` array<string> NULL COMMENT "",
              `a1` array<tinyint(4)> NULL COMMENT "",
              `a2` array<largeint(40)> NULL COMMENT "",
              `aa1` array<array<int(11)>> NOT NULL COMMENT "",
            ) ENGINE=OLAP
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """
    sql """ INSERT INTO t_array_combinations VALUES(1, ['foo','bar','baz'], [1,2,3], [1,2,2], [[1,1],[4,5],[1,4]]) """
    sql """ INSERT INTO t_array_combinations VALUES(2, [null,null], [null,null], [], [[null,null],[null,null],[null,null]]) """
    sql """ INSERT INTO t_array_combinations VALUES(3, [], [], [], [[]]) """
    sql """ INSERT INTO t_array_combinations VALUES(4, ['a','b','c','d'], [1,2,3,4,5], [10,20,30], [[1],[2],[3],[4]]) """
    
    qt_test_basic """
    select k1, array_combinations(s1, 2), array_combinations(a1, 2), array_combinations(a2, 2), array_combinations(aa1, 2) from t_array_combinations order by k1;
    """
    
    qt_test_empty_array """
    select array_combinations([], 1), array_combinations([], 2);
    """
    
    qt_test_param1_null """
    select array_combinations(null, 2);
    """

    qt_test_param2_null """
    select array_combinations([1,2,3], null);
    """

    test {
        sql """select k1, array_combinations(['x','y','z'], k1) from t_array_combinations where k1 <= 3 order by k1;"""
        exception("Array_Combinations's second argument must be a constant literal.")
    }
    
    qt_test_param_3 """
    select array_combinations(['a','b','c','d'], 3), array_combinations([1,2,3,4], 3);
    """
    
    qt_test_param_4 """
    select array_combinations(['a','b','c','d','e'], 4), array_combinations([1,2,3,4,5], 4);
    """
    
    qt_test_param_5 """
    select array_combinations([1,2,3,4,5,6], 5);
    """
    
    qt_test_param_greater_than_length """
    select array_combinations([1,2], 3), array_combinations(['a'], 2);
    """
    
    qt_test_array_with_null """
    select array_combinations([1,null,3], 2), array_combinations(['a',null,'c'], 2);
    """
    
    qt_test_different_types """
    select array_combinations([1.5, 2.5, 3.5], 2), array_combinations([true, false], 2);
    """

    qt_test_date """
    select array_combinations(['2015-03-13','2015-03-14','2015-03-15'], 2);
    """

    qt_test_date_cast """
    select array_combinations(
        array(cast('2015-03-13' as date), cast('2015-03-14' as date), cast('2015-03-15' as date)),
        2
    );
    """

    qt_test_datetime """
    select array_combinations(['2015-03-13 12:36:38','2015-03-14 12:36:38'], 2);
    """

    qt_test_datetime_cast """
    select array_combinations(
        array(cast('2015-03-13 12:36:38' as datetime), cast('2015-03-14 12:36:38' as datetime)),
        2
    );
    """

    qt_test_datev2 """
    select array_combinations(
        array(cast('2023-02-05' as datev2), cast('2023-02-06' as datev2), cast('2023-02-07' as datev2)), 2
    );
    """

    qt_test_datetimev2 """
    select array_combinations(
        array(
            cast('2022-10-15 10:30:00.999' as datetimev2(3)),
            cast('2022-10-16 10:30:00.999' as datetimev2(3)),
            cast('2022-10-17 10:30:00.999' as datetimev2(3))
        ), 
        2
    );
    """

    qt_test_datetimev2_6 """
    select array_combinations(
        array(
            cast('2022-10-15 10:30:00.999999' as datetimev2(6)),
            cast('2022-10-16 10:30:00.999999' as datetimev2(6)),
            cast('2022-10-17 10:30:00.999999' as datetimev2(6))
        ),
        2
    );
    """

    qt_test_decimalv3 """
    select array_combinations(
        array(
            cast(111.111 as decimalv3(6,3)),
            cast(222.222 as decimalv3(6,3)),
            cast(333.333 as decimalv3(6,3))
        ), 
        2
    );
    """

    qt_test_decimalv2 """
    select array_combinations(
        cast(
            array(
                cast(0.100 as decimal(9,3)),
                cast(0.200 as decimal(9,3)),
                cast(0.300 as decimal(9,3))
            ) as array<decimal(9,3)>
        ),
        2
    );
    """

    qt_test_smallint_bigint """
    select
        array_combinations(cast(array(1,2,3,4) as array<smallint>), 2),
        array_combinations(cast(array(1,2,3,4,5) as array<bigint>), 3);
    """

    qt_test_char_varchar """
    select array_combinations(
        cast(array('a','bb','ccc') as array<varchar(10)>), 
        2
    ),
    array_combinations(
        cast(array('x','y','z') as array<char(5)>), 
        2
    );
    """

    qt_test_array_decimal """
    select array_combinations(
        array(
            array(cast(1.1 as decimalv3(6,2))),
            array(cast(2.2 as decimalv3(6,2))),
            array(cast(3.3 as decimalv3(6,2)))
        ), 
        2
    );
    """

    qt_test_array_datetime """
    select array_combinations(
        array(
            array(cast('2023-01-01 00:00:00' as datetime)),
            array(cast('2023-01-02 00:00:00' as datetime)),
            array(cast('2023-01-03 00:00:00' as datetime))
        ), 
        2
    );
    """

    qt_test_array_timestamptz """
    select array_combinations(
        array(
            cast('2026-01-01 00:00:00' as TIMESTAMPTZ),
            cast('2026-01-01 00:00:00' as TIMESTAMPTZ),
            cast('2026-01-01 00:00:00' as TIMESTAMPTZ)
        ), 
        2
    );
    """

    qt_test_ipv4 """
    select array_combinations(
        array(
            cast('192.168.1.1' as ipv4),
            cast('10.0.0.1' as ipv4),
            cast('172.16.0.1' as ipv4)
        ),
        2
    );
    """

    qt_test_ipv6 """
    select array_combinations(
        array(
            cast('2001:db8::1' as ipv6),
            cast('2001:db8::2' as ipv6),
            cast('2001:db8::3' as ipv6)
        ),
        2
    );
    """

    test {
        sql """select array_combinations([1,2,3], -1);"""
        exception("execute failed, function array_combinations's second argument must be bigger than 0 and not bigger than 5")
    }
    
    test {
        sql """select array_combinations([1,2,3], 0);"""
        exception("execute failed, function array_combinations's second argument must be bigger than 0 and not bigger than 5")
    }
    
    test {
        sql """select array_combinations([1,2,3,4,5,6], 6);"""
        exception("execute failed, function array_combinations's second argument must be bigger than 0 and not bigger than 5")
    }

    test {
        sql """select array_combinations([1,2,3,4,5,6], 6);"""
        exception("execute failed, function array_combinations's second argument must be bigger than 0 and not bigger than 5")
    }
    
    test {
        sql """select array_combinations([1,2,3,4,5,6,7,8,9,10,
                                           11,12,13,14,15,16,17,18,19,20,
                                           21,22,23,24,25,26,27,28,29], 5);"""
        exception("execute failed, function array_combinations's total size of sub-groups generated must be smaller than 100,000")
    }
    
}