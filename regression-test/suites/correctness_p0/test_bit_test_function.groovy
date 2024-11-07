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

suite("test_bit_test_function") {
    qt_bit_test1 'select number,bit_test(number,0) from numbers("number"="10") order by 1;'
    qt_bit_test2 'select number,bit_test_all(number,0) from numbers("number"="10") order by 1;'
    qt_bit_test3 'select number,bit_test_all(number,1) from numbers("number"="10") order by 1;'
    qt_bit_test4 'select number,bit_test(number,0,1) from numbers("number"="10") order by 1;'
    qt_bit_test5 'select bit_test(cast (-1 as tinyint),0);'    
    qt_bit_test6 'select bit_test(cast (-1 as smallint),1);'   
    qt_bit_test7 'select bit_test(cast (-1 as int),2);'        
    qt_bit_test8 'select bit_test(cast (-1 as bigint),3);'    
    qt_bit_test9 'select bit_test(cast (-1 as largeint),4);'
    qt_bit_test10 'select bit_test(10,-1);'
    qt_bit_test11 'select bit_test(100,-2);'
    qt_bit_test12 'select bit_test(100,1000);'
    qt_bit_test13 'select bit_test(-43,1);'
    qt_bit_test14 'select bit_test(-43,2);'
    qt_bit_test_TINYINT_MAX 'select bit_test(cast (127 as tinyint),2);'                  // TINYINT_MAX
    qt_bit_test_TINYINT_MIN 'select bit_test(cast (-128 as tinyint),4);'                 // TINYINT_MIN
    qt_bit_test_SMALLINT_MAX 'select bit_test(cast (32767 as smallint),5);'              // SMALLINT_MAX
    qt_bit_test_SMALLINT_MIN 'select bit_test(cast (-32768 as smallint),10);'            // SMALLINT_MIN
    qt_bit_test_INT_MAX 'select bit_test(cast (2147483647 as int),12);'                  // INT_MAX
    qt_bit_test_INT_MIN 'select bit_test(cast (-2147483648 as int),11);'                  // INT_MIN
    qt_bit_test_INT64_MAX 'select bit_test(cast (9223372036854775807 as bigint),12);'    // INT64_MAX
    qt_bit_test_INT64_MIN 'select bit_test(cast (-9223372036854775808 as bigint),12);'   // INT64_MIN
    // INT128_MAX
    qt_bit_test_INT128_MAX """
        select bit_test(170141183460469231731687303715884105727,13),
               bit_test(cast (170141183460469231731687303715884105727 as largeint),13);
    """
    // INT128_MIN
    qt_bit_test_INT128_MIN """
        select  bit_test(-170141183460469231731687303715884105728,11),
                bit_test(cast (-170141183460469231731687303715884105728 as largeint),11);
    """
    // NULL
    qt_select1_const "select bit_test(NULL,1);"
    qt_select2_const "select bit_test(1,NULL);"
    qt_select3_const "select bit_test(NULL,NULL);"
    qt_select4_const "select bit_test(111,1,2,3,NULL);"

   sql """DROP TABLE IF EXISTS test_bit_test"""
   sql """ 
             CREATE TABLE IF NOT EXISTS test_bit_test (
               `k1` int(11) NULL COMMENT "",
               `s1` int(20) NULL COMMENT "",
               `s2` int(20) NOT NULL COMMENT "",
               `p1` int(20) NULL COMMENT "",
               `p2` int(20) NOT NULL COMMENT ""
             ) ENGINE=OLAP
             DUPLICATE KEY(`k1`)
             DISTRIBUTED BY HASH(`k1`) BUCKETS 1
             PROPERTIES (
             "replication_allocation" = "tag.location.default: 1",
             "storage_format" = "V2"
             )
    """
    sql """ INSERT INTO test_bit_test VALUES(1, 1, 1, 1, 1); """
    sql """ INSERT INTO test_bit_test VALUES(2, NULL, 1, 1, 1); """
    sql """ INSERT INTO test_bit_test VALUES(3, NULL, 1, NULL, 1); """
    sql """ INSERT INTO test_bit_test VALUES(4, NULL, 2147483647, NULL, 1); """
    sql """ INSERT INTO test_bit_test VALUES(5, NULL, 2147483647, NULL, 2147483647); """

    
    // null and not_null combine
    qt_select1_null_null "select k1,s1,p1,bit_test(s1, p1) from test_bit_test order by k1;"
    qt_select2_null_not_null "select k1,s1,p2,bit_test(s1, p2) from test_bit_test order by k1;"
    qt_select3_not_null_not_null "select k1,s2,p2,bit_test(s2, p2) from test_bit_test order by k1;"
    qt_select4_not_null_null "select k1,s2,p1,bit_test(s2, p1) from test_bit_test order by k1;"
    qt_select5_null_const "select k1,s1,1,bit_test(s1, 1) from test_bit_test order by k1;"
    qt_select6_not_null_const "select k1,s2,1,bit_test(s2, 1) from test_bit_test order by k1;"
    qt_select7_const_null "select k1,6,p1,bit_test(6, p1) from test_bit_test order by k1;"
    qt_select7_const_not_null "select k1,6,p2,bit_test(6, p2) from test_bit_test order by k1;"
    qt_select7_null_null "select k1,s1,p1,bit_test(s1, p1,1,2,3) from test_bit_test order by k1;"
    qt_select7_not_null_not_null "select k1,s2,p2,bit_test(s2, p2,1,2,3) from test_bit_test order by k1;"
}
