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

suite("test_if") {
    qt_select "select if(id=1,count,hll_empty()) from (select 1 as id, hll_hash(1) as count) t"

    qt_select "select if(job_d is null, array(), job_d) as test from (select array('1970-01-01', '1970-01-01') as job_d) t"
    qt_select "select if(job_d is null, array('1970-01-01'), job_d) as test from (select array('1970-01-01', '1970-01-01') as job_d) t"
    qt_select "select if(job_d is null, job_d, array()) as test from (select array('1970-01-01', '1970-01-01') as job_d) t"

    // user case https://github.com/apache/doris/issues/25644
    qt_select "SELECT NOT ISNULL(CASE WHEN IFNULL ((t1.region IN ('US')),0) THEN t1.region ELSE 'other' END) AS account_id, count(*) FROM (select 'US' AS region) as t1 group by 1"
    qt_select "SELECT NOT ISNULL(CASE WHEN IFNULL (('US' IN ('US')),0) THEN 'US' ELSE 'other' END);"
}
