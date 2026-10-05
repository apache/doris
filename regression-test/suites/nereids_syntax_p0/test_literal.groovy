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

suite("test_literal") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'


    // test map and array empty literal
    sql """
        select {}, [], [[[null]], [[1, 2, 3]]], {1:[null], 3:[3]};
    """

    qt_date1 """
        select cast('20160702235959.9999999' as date);
    """

    qt_date2 """
        select cast('2016-07-02 23:59:59.9999999' as date);
    """

    qt_datetime1 """
        select timestamp'2016-07-02 23:59:59.99';
    """

    qt_datetime2 """
        select cast('20160702235959.9999999' as datetime);
    """

    qt_datetime3 """
        select cast('2016-07-02 23:59:59.9999999' as datetime);
    """

    qt_datetime4 """
        select timestamp'20200219010101.0000001';
    """

    qt_datetime5 """
        select CONVERT('2021-01-30 00:00:00.0000001', DATETIME(6))
    """
}
