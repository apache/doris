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

suite("test_user_var") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET @a1=1, @a2=0, @a3=-1"
    sql "SET @b1=1.1, @b2=0.0, @b3=-1.1"
    sql "SET @c1='H', @c2=''"
    sql "SET @d1=true, @d2=false"
    sql "SET @f1=null"
    sql "set @func_1=(abs(1) + 1) * 2"

    qt_integer 'select @a1, @a2, @a3;'
    qt_decimal 'select @b1, @b2, @b3;'
    qt_string 'select @c1, @c2;'
    qt_boolean 'select @d1, @d2;'
    qt_null_literal 'select @f1, @f2;'
    qt_function 'select @func_1'
}