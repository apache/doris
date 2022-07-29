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

suite("basic_agg_test") {
    def tables=["bitmap_basic_agg","hll_basic_agg"]

    for (String table in tables) {
        sql """drop table if exists ${table};"""
        sql new File("""regression-test/common/table/${table}.sql""").text
        sql new File("""regression-test/common/load/${table}.sql""").text
    }

    qt_sql_bitmap """select * from bitmap_basic_agg;"""

    qt_sql_hll """select * from hll_basic_agg;"""

    qt_sql_hll_cardinality """select k1, hll_cardinality(hll_union(k2)) from hll_basic_agg group by k1 order by k1;"""
}
