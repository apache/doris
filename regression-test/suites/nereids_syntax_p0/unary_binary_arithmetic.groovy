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

suite("unary_binary_arithmetic") {
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    qt_select "select 8 div 2";

    qt_select "select 8 div 0";

    qt_select "select 7 & 1";
    qt_select "select 4 & 1";
    
    qt_select "select 7 ^ 7"
    qt_select "select 7 ^ 8"

    qt_select "select 3 | 4"
    qt_select "select 7 | 0"

    qt_select "select 4 % 2"
    qt_select "select 99 % 2"

    qt_select "select -1"
    qt_select "select -(2+3)"
    qt_select "select +(1+1)"
    qt_select "select ~7"
    qt_select "select ~8"
    
    qt_select "select ~'a';"
    qt_select "select ~cast('2000-01-01' as date)"


}
