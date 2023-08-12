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

suite("test_time_function") {
    sql """
        set enable_nereids_planner=true,enable_fallback_to_original_planner=false
    """
    qt_select1 """
        select sec_to_time(time_to_sec(cast('16:32:18' as time)));
    """
    qt_select2 """
        select sec_to_time(59538);
    """

    sql """
        set enable_nereids_planner=false
    """

    qt_select3 """
        select sec_to_time(time_to_sec(cast('16:32:18' as time)));
    """
    qt_select4 """
        select sec_to_time(59538);
    """


    
}