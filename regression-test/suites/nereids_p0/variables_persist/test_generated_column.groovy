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

suite("test_generated_column") {
    // 打开enable_decimal256建表
    multi_sql """
        set enable_decimal256=true;
        drop table if exists t_gen_col_multi_decimalv3;
        create table t_gen_col_multi_decimalv3(a decimal(20,5),b decimal(21,6),c decimal(38,11) generated always as (a*b) not null)
        DISTRIBUTED BY HASH(a)
        PROPERTIES("replication_num" = "1");
    """
    // 关闭enable_decimal256，插入数据
    sql "set enable_decimal256=false;"
    sql "insert into t_gen_col_multi_decimalv3 values(1.12343,1.123457,default);"

    // 查询数据,预期column c的scale为11
    qt_c_scale_is_11 "select * from t_gen_col_multi_decimalv3;"

}