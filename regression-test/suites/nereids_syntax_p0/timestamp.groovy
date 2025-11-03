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

suite("timestamp") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'

    qt_select_1 '''
        SELECT extract(day from TIMESTAMP '2001-08-22 03:04:05.321')
    '''

    qt_select_2 '''
        SELECT  date '2012-08-08' + interval '2' day,
            timestamp '2012-08-08 01:00' + interval '29' hour,
            timestamp '2012-10-31 01:00' + interval '1' month,
            date '2012-08-08' - interval '2' day,
            timestamp '2012-08-08 01:00' - interval '29' hour,
            timestamp '2012-10-31 01:00' - interval '1' month
    '''
}
