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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import org.apache.doris.regression.action.ProfileAction

suite('rf_merge_context_profile', 'nonConcurrent') {
    sql 'drop table if exists rf_merge_profile_probe'
    sql 'drop table if exists rf_merge_profile_build'
    sql '''create table rf_merge_profile_probe (k int, v int) duplicate key(k)
            distributed by hash(k) buckets 4 properties ("replication_num" = "1")'''
    sql '''create table rf_merge_profile_build (k int) duplicate key(k)
            distributed by hash(k) buckets 4 properties ("replication_num" = "1")'''
    sql 'insert into rf_merge_profile_probe values (1, 10), (2, 20), (3, 30), (4, 40)'
    sql 'insert into rf_merge_profile_build values (2), (4)'

    sql 'set enable_profile=true'
    sql "set runtime_filter_mode='LOCAL'"
    String localPlan = sql('''explain select p.k, p.v from rf_merge_profile_probe p
                              join [broadcast] rf_merge_profile_build b on p.k = b.k order by p.k''').toString()
    assertTrue(localPlan.contains('runtime filters: RF'))
    order_qt_join '''select p.k, p.v from rf_merge_profile_probe p
                     join [broadcast] rf_merge_profile_build b on p.k = b.k order by p.k'''

    // The profile check is tied to this query text, not to an unrelated latest profile.
    // Ten seconds is the FE's normal completion window; the helper's 30-second default
    // would hide a delayed final report.
    String token = UUID.randomUUID().toString()
    sql """select '${token}' as profile_marker, p.k, p.v from rf_merge_profile_probe p
           join [broadcast] rf_merge_profile_build b on p.k = b.k
           where p.k > 0 order by p.k"""
    new ProfileAction(context).getProfileBySql(token, ['Fragment'], 10000, 200)

    // The shuffle hint puts the probe scan across an Exchange from the join, so
    // these RFs exercise remote merge/publish even on a one-BE test cluster. Only query
    // correctness is checked: remote RF contexts keep their original delayed lifetime.
    sql "set runtime_filter_mode='GLOBAL'"
    String remotePlan = sql('''explain select p.k, p.v from rf_merge_profile_probe p
                               join [shuffle] rf_merge_profile_build b on p.k = b.k order by p.k''').toString()
    assertTrue(remotePlan.contains('INNER JOIN('), remotePlan)
    assertTrue(remotePlan.contains('VEXCHANGE'), remotePlan)
    assertTrue(remotePlan.contains('runtime filters: RF'), remotePlan)
    order_qt_remote '''select p.k, p.v from rf_merge_profile_probe p
                       join [shuffle] rf_merge_profile_build b on p.k = b.k order by p.k'''
}
