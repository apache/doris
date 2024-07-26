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

suite("test_timev2_fold") {
    // FE
    sql """ set enable_fold_constant_by_be=false """
    qt_select10 """
        select timediff( convert_tz("2020-05-05 00:00:00", 'UTC', 'America/Los_Angeles'), "2020-05-05 00:00:00");
    """
    qt_select11 """
        select convert_tz('2020-02-01 12:00:00', 'America/Los_Angeles', '+08:00');
    """
    qt_select12 """
        select convert_tz('2020-05-01 12:00:00', 'America/Los_Angeles', '+08:00');
    """
    qt_select13 """
        select CONVERT_TZ('9999-12-31 23:59:59.999999', 'Pacific/Galapagos', 'Pacific/GalapaGoS');
    """
    // FE + BE
    sql """ set enable_fold_constant_by_be=true """
    qt_select20 """
        select timediff( convert_tz("2020-05-05 00:00:00", 'UTC', 'America/Los_Angeles'), "2020-05-05 00:00:00");
    """
    qt_select21 """
        select convert_tz('2020-02-01 12:00:00', 'America/Los_Angeles', '+08:00');
    """
    qt_select22 """
        select convert_tz('2020-05-01 12:00:00', 'America/Los_Angeles', '+08:00');
    """
    qt_select23 """
        select CONVERT_TZ('9999-12-31 23:59:59.999999', 'Pacific/Galapagos', 'Pacific/GalapaGoS');
    """
    // BE
    sql """ set debug_skip_fold_constant=true """
    qt_select20 """
        select timediff( convert_tz("2020-05-05 00:00:00", 'UTC', 'America/Los_Angeles'), "2020-05-05 00:00:00");
    """
    qt_select21 """
        select convert_tz('2020-02-01 12:00:00', 'America/Los_Angeles', '+08:00');
    """
    qt_select22 """
        select convert_tz('2020-05-01 12:00:00', 'America/Los_Angeles', '+08:00');
    """
    qt_select23 """
        select CONVERT_TZ('9999-12-31 23:59:59.999999', 'Pacific/Galapagos', 'Pacific/GalapaGoS');
    """
}
