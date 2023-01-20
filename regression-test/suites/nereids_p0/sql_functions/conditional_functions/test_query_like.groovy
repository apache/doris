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

suite("test_query_like", "query,p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    sql "use test_query_db"

    def tableName = "test"

    qt_like1 """select * from ${tableName} where k6 like "%" order by k1, k2, k3, k4"""
    qt_like2 """select * from ${tableName} where k6 like "____" order by k1, k2, k3, k4"""
    qt_like3 """select * from ${tableName} where lower(k7) like "%lnv%" order by k1, k2, k3, k4"""
    qt_like4 """select * from ${tableName} where lower(k7) like "%lnv%" order by k1, k2, k3, k4"""
    qt_like5 """select * from ${tableName} where lower(k7) like "wangjuoo4" order by k1, k2, k3, k4"""
    qt_like6 """select * from ${tableName} where lower(k6) like "%t_u%" order by k1, k2, k3, k4"""
    qt_like7 """select * from ${tableName} where k6 not like "%" order by k1, k2, k3, k4"""
    qt_like8 """select * from ${tableName} where k6 not like "____" order by k1, k2, k3, k4"""
    qt_like9 """select * from ${tableName} where lower(k7) not like "%lnv%" order by k1, k2, k3, k4"""
    qt_like10 """select * from ${tableName} where lower(k7) not like "%lnv%" order by k1, k2, k3, k4"""
    qt_like11 """select * from ${tableName} where lower(k7) not like "wangjuoo4" \
		    order by k1, k2, k3, k4"""
    qt_like12 """select * from ${tableName} where lower(k6) not like "%t_u%" order by k1, k2, k3, k4"""
    qt_like13 """select "abcd%%1" like "abcd%", "abcd%%1" not like "abcd%" """
    qt_like14 """select "abcd%%1" like "abcd%1", "abcd%%1" not like "abcd%1" """
    qt_like15 """select "abcd%%1" like "abcd%1%", "abcd%%1" not like "abcd%1%" """
    qt_like16 """select "abcd%%1" like "abcd\\%%", "abcd%%1" not like "abcd\\%%" """
    qt_like17 """select "abcd%%1" like "abcd\\%\\%%", "abcd%%1" not like "abcd\\%\\%%" """
    qt_like18 """select "abcd%%1" like "abcd\\%\\%1", "abcd%%1" not like "abcd\\%\\%1" """
    qt_like19 """select "abcd%%1" like "abcd\\%\\%\1%", "abcd%%1" not like "abcd\\%\\%\1%" """
    qt_like20 """select "abcd%%1" like "abcd_%1", "abcd%%1" not like "abcd_%1" """
    qt_like21 """select "abcd%%1" like "abcd_\\%1", "abcd%%1" not like "abcd_\\%1" """
    qt_like22 """select "abcd%%1" like "abcd__1", "abcd%%1" not like "abcd__1" """
    qt_like23 """select "abcd%%1" like "abcd_%_", "abcd%%1" not like "abcd_%_" """
    qt_like24 """select "abcd%%1" like "abcd\\_%1", "abcd%%1" not like "abcd\\_%1" """
}
