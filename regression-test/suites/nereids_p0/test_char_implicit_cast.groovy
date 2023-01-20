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

suite("test_char_implicit_cast") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    qt_test_dayofweek_varchar 'select dayofweek("2012-12-01");'
    qt_test_dayofweek_char 'select dayofweek(cast("2012-12-01" as char(16)));'
    qt_test_timediff_varchar 'select timediff("2010-01-01 01:00:00", "2010-01-02 01:00:00");'
    qt_test_timediff_char 'select timediff("2010-01-01 01:00:00", cast("2010-01-02 01:00:00" as char));'
    qt_test_money_format_varchar 'select money_format("123456");'
    qt_test_money_format_char 'select  money_format(cast("123456" as char));'
}
