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

suite("test_partition_sort") {
    sql "use regression_test_ssb_unique_sql_zstd_p0"
    qt_select0 """ select count() from part; """
    qt_select1 """ select count(distinct p_name), count(distinct p_color),count(distinct rn),count(distinct rk),count(distinct dr) from (
                    select p_name, p_color , row_number() over(partition by p_name order by p_color) as rn,rank() over(partition by p_name order by p_color) as rk, 
                    dense_rank() over(partition by p_name order by p_color) as dr from part) as t; """

    qt_select2 """ select * from (
                    select p_name, p_color , row_number() over(partition by p_name order by p_color) as rn from part) as t where rn=14 order by 1,2,3; """

    qt_select3 """ select * from ( select p_name, p_color , rank() over(partition by p_name order by p_color) as rn from part) as t where rn=11 order by 1,2,3; """

    qt_select4 """ select * from ( select p_name, p_color , dense_rank() over(partition by p_name order by p_color) as rn from part) as t where rn=10 order by 1,2,3; """

  

}