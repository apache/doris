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

suite("test_cast_function") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    qt_sql """ select cast (1 as BIGINT) """
    qt_sql """ select cast(cast ("11.2" as double) as bigint) """
    qt_sql """ select cast ("0.0101031417" as datetime) """
    qt_sql """ select cast ("0.0000031417" as datetime) """
    qt_sql """ select cast (NULL AS CHAR(1)); """
    qt_sql """ select cast ('20190101' AS CHAR(2)); """
    qt_sql_null_cast_bitmap """ select cast (case when BITMAP_EMPTY() is NULL then null else null end as bitmap) is NULL; """
    qt_sql_to_tiny """ select cast('1212.31' as tinyint);""" 
    qt_sql_to_small """ select cast('1212.31' as smallint);""" 
    qt_sql_to_int """ select cast('1212.31' as int);""" 
    qt_sql_to_big """ select cast('1212.31' as bigint);""" 
}

