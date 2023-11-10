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
    sql """ set enable_nereids_planner=false,enable_fold_constant_by_be=false """
    qt_select1 """
        select timediff( convert_tz("2020-05-05 00:00:00", 'UTC', 'America/Los_Angeles'), "2020-05-05 00:00:00");
    """
    sql """ set enable_nereids_planner=true,enable_fold_constant_by_be=false """
    qt_select2 """
        select timediff( convert_tz("2020-05-05 00:00:00", 'UTC', 'America/Los_Angeles'), "2020-05-05 00:00:00");
    """
    sql """ set enable_nereids_planner=false,enable_fold_constant_by_be=true """
    qt_select3 """
        select timediff( convert_tz("2020-05-05 00:00:00", 'UTC', 'America/Los_Angeles'), "2020-05-05 00:00:00");
    """
    sql """ set enable_nereids_planner=true,enable_fold_constant_by_be=true """
    qt_select4 """
        select timediff( convert_tz("2020-05-05 00:00:00", 'UTC', 'America/Los_Angeles'), "2020-05-05 00:00:00");
    """
}