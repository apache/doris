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

suite("test_gamma") {
    // Test with constant folding enabled
    sql "set debug_skip_fold_constant=false"
    qt_select_const "select gamma(5), gamma(0.5), gamma(1)"
    qt_select_null "select gamma(null)"
    qt_select_zero "select gamma(0)"
    qt_select_neg "select gamma(-1)"

    // Test with constant folding disabled
    sql "set debug_skip_fold_constant=true"
    qt_select_const_no_fold "select gamma(5), gamma(0.5), gamma(1)"
    qt_select_null_no_fold "select gamma(null)"
    qt_select_zero_no_fold "select gamma(0)"
    qt_select_neg_no_fold "select gamma(-1)"

    // Test with table data
    sql "drop table if exists test_gamma_tbl"
    sql """
        create table test_gamma_tbl (
            k1 int,
            v1 double
        ) distributed by hash(k1) properties("replication_num" = "1");
    """

    sql """
        insert into test_gamma_tbl values 
        (1, 1.0),
        (2, 2.0),
        (3, 3.0),
        (4, null);
    """

    qt_select_table "select k1, v1, gamma(v1) from test_gamma_tbl order by k1"

    sql "drop table test_gamma_tbl"
}
