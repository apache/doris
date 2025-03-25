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

suite("test_conv") {
    qt_select "SELECT CONV(15,10,2)"
    qt_select2 "select conv('ffffffffffffff', 24, 2);"
    qt_select3 "select conv('-ff', 24, 2);"
    qt_select4 "select conv('fffffffffffffffffffffffffffffffff', 24, 10);"
    
    sql """DROP TABLE IF EXISTS `test_tb_with_null`; """
    sql """ create table test_tb_with_null(int_1 int, float_2 float, nullable_val varchar(16)) PROPERTIES (
            "replication_num" = "1"
            ); 
    """
    
    sql """ insert into test_tb_with_null values(1, 1.464868, '100'), 
                                                 (2, null, null), 
                                                 (3, 2.789, '200'), 
                                                 (4, 3.14159, null); """

    qt_select5 """ select conv(nullable_val, 10, 2), nullable_val from test_tb_with_null; """

    qt_select6 """ select conv(float_2,10,2), float_2 from test_tb_with_null; """

    check_fold_consistency "conv(null, null, null)"
    check_fold_consistency "conv(15, 10, 2)"
    check_fold_consistency "conv(null, 10, 2)"
    check_fold_consistency "conv(15, null, 2)"
    check_fold_consistency "conv(15, 10, null)"
    check_fold_consistency "conv('123', 10, 2)"
}