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

suite("test_concat_extreme_input") {
    // there is need to check the result, the following query would return error due to
    // concat output exceeds the UINT size. This case just tests whether the BE could cover
    // such occasion without crash
    test {
        sql ''' select
  concat(
    cast(substr(
      cast(ref_1.`cp_type` as varchar),
      cast(
        max(
          cast(ref_1.`cp_catalog_page_number` as int)) over (partition by ref_1.`cp_end_date_sk` order by ref_1.`cp_catalog_page_number`) as int),
      cast(ref_1.`cp_end_date_sk` as int)) as varchar),
    cast(substring(
      cast(ref_1.`cp_department` as varchar),
      cast(ref_1.`cp_end_date_sk` as int),
      cast(ref_1.`cp_end_date_sk` as int)) as varchar),
    cast(rpad(
      cast(ref_1.`cp_type` as varchar),
      cast(ref_1.`cp_start_date_sk` as int),
      cast(ref_1.`cp_description` as varchar)) as varchar)) as c1
from
  regression_test_tpcds_sf1_p1.catalog_page as ref_1 '''
        exception "concat output is too large to allocate"
    }
}

