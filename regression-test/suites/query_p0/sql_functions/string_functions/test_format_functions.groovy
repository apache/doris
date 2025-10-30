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

suite("test_format_functions") {
    sql " drop table if exists test_format_functions"
    sql """
        create table test_format_functions (
            id int,
            s1 string not null,
            s2 string null,
            k1 largeint not null,
            k2 largeint null
        )
        DISTRIBUTED BY HASH(id)
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """

    //empty table
    order_qt_empty_nullable1 "select parse_data_size(s2) from test_format_functions"
    order_qt_empty_nullable2 "select format(s2,'') from test_format_functions"
    order_qt_empty_nullable3 "select format_number(k2) from test_format_functions"
    order_qt_empty_not_nullable1 "select parse_data_size(s1) from test_format_functions"
    order_qt_empty_not_nullable2 "select format(s1,'') from test_format_functions"
    order_qt_empty_not_nullable3 "select format_number(k1) from test_format_functions"

    //null / const
    order_qt_empty_null1 "select parse_data_size(NULL)"
    order_qt_empty_null2 "select format_number(NULL)"
    order_qt_empty_null3 "select format(NULL,'')"
    order_qt_empty_null4 "select format('',NULL)"
    order_qt_empty_null5 "select format(NULL,NULL)"
    
    //valid data
    order_qt_empty_const1 "select parse_data_size('0B')"
    order_qt_empty_const2 "select parse_data_size('1B')"
    order_qt_empty_const3 "select parse_data_size('1.2B')"
    order_qt_empty_const4 "select parse_data_size('1.9B')"
    order_qt_empty_const5 "select parse_data_size('2.2kB')"
    order_qt_empty_const6 "select parse_data_size('2.23kB')"
    order_qt_empty_const7 "select parse_data_size('2.234kB')"
    order_qt_empty_const8 "select parse_data_size('3MB')"
    order_qt_empty_const9 "select parse_data_size('4GB')"
    order_qt_empty_const10 "select parse_data_size('4TB')"
    order_qt_empty_const11 "select parse_data_size('5PB')"
    order_qt_empty_const12 "select parse_data_size('6EB')"
    order_qt_empty_const13 "select parse_data_size('7ZB')"
    order_qt_empty_const14 "select parse_data_size('8YB')"
    order_qt_empty_const15 "select parse_data_size('6917529027641081856EB')"
    order_qt_empty_const16 "select parse_data_size('69175290276410818560EB')"
    //invalid data    
    test {
         sql """ select parse_data_size(''); """
         exception "Invalid Input argument"
    }
    test {
         sql """ select parse_data_size('0'); """
         exception "Invalid Input argument"
    }
    test {
         sql """ select parse_data_size('10KB'); """
         exception "Invalid Input argument"
    }
    test {
         sql """ select parse_data_size('KB'); """
         exception "Invalid Input argument"
    }
    test {
         sql """ select parse_data_size('-1B'); """
         exception "Invalid Input argument"
    }
    test {
         sql """ select parse_data_size('12345K'); """
         exception "Invalid Input argument"
    }
    test {
         sql """ select parse_data_size('A12345B'); """
         exception "Invalid Input argument"
    }

    
    //format_number
    order_qt_format_number_1 "select format_number(123);"
    order_qt_format_number_2 "select format_number(12345);"
    order_qt_format_number_3 "select format_number(12399);"
    order_qt_format_number_4 "select format_number(12345678);"
    order_qt_format_number_5 "select format_number(12399999);"
    order_qt_format_number_6 "select format_number(12345678901);"
    order_qt_format_number_7 "select format_number(12399999999);"
    order_qt_format_number_8 "select format_number(1234.5);"
    order_qt_format_number_9 "select format_number(1239.9);"
    order_qt_format_number_10 "select format_number(1234567.8);"
    order_qt_format_number_11 "select format_number(1239999.9);"
    order_qt_format_number_12 "select format_number(1234567890.1);"
    order_qt_format_number_13 "select format_number(1239999999.9);"
    order_qt_format_number_14 "select format_number(-999);"
    order_qt_format_number_15 "select format_number(-1000);"
    order_qt_format_number_16 "select format_number(-999999);"
    order_qt_format_number_17 "select format_number(-1000000);"
    order_qt_format_number_18 "select format_number(-999999999);"
    order_qt_format_number_19 "select format_number(-1000000000);"
    order_qt_format_number_20 "select format_number(-999999999999);"
    order_qt_format_number_21 "select format_number(-1000000000000);"
    order_qt_format_number_22 "select format_number(-999999999999999);"
    order_qt_format_number_23 "select format_number(-1000000000000000);"
    order_qt_format_number_24 "select format_number(-9223372036854775808);"
    order_qt_format_number_25 "select format_number(0);"
    order_qt_format_number_26 "select format_number(999999);"
    order_qt_format_number_27 "select format_number(1000000);"

    //format
    order_qt_format_1 "select format('{}', 123);"
    order_qt_format_2 "select format('{} of {}', 123, 456);"
    order_qt_format_3 "select format('{0}{1}', pi(),123);"
    order_qt_format_4 "select format('{:05}', 8);"
    order_qt_format_5 "select format('{1}{0}', 'hello', 'world');"
    order_qt_format_6 "select format('{:.3}', pi());"
    order_qt_format_7 "select format('{:e}', e());"
    test {
         sql """ select format('{asdasdsa}',"asd"); """
         exception "Invalid Input argument"
    }

    sql """ insert into test_format_functions values (1, '2.2kB', '2.2kB',12345678,12345678); """
    sql """ insert into test_format_functions values (2, '8YB', '8YB',1234567890.1,1234567890.1); """
    sql """ insert into test_format_functions values (3, '4TB', '4TB',-1000000000000000,-1000000000000000); """
    sql """ insert into test_format_functions values (4, '2.234kB', '2.234kB', 1234.5, 1234.5); """
    sql """ insert into test_format_functions values (5, '6917529027641081856EB', '6917529027641081856EB',123,123); """
    sql """ insert into test_format_functions values (6, '0B', '0B',999999,999999); """
    sql """ insert into test_format_functions values (7, "1B", "1B",0,NULL); """

    order_qt_nullable1 "select id,s2,parse_data_size(s2) from test_format_functions order by id"
    order_qt_nullable2 "select id,k2,format_number(k2) from test_format_functions order by id"

    order_qt_not_nullable1 "select id,s1,parse_data_size(s1) from test_format_functions order by id"
    order_qt_not_nullable2 "select id,k1,format_number(k1) from test_format_functions order by id"

    sql """ insert into test_format_functions values (8, '{:>6}', '{:>6}',1234567890.1234,1234567890.1234); """
    sql """ insert into test_format_functions values (9, '{}', '{}',-1000000000000000,-1000000000000000); """
    sql """ insert into test_format_functions values (10, '{:06}', '{:06}', pi(), pi()); """
    order_qt_nullable3 "select id,s2,format(s2,k1) from test_format_functions where id in (7,8,9,10) order by id"
    order_qt_not_nullable3 "select id,s1,format(s1,1234.4) from test_format_functions where id in (7,8,9,10) order by id"

}
