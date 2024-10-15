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
    qt_select """
        select sec_to_time(time_to_sec(cast('16:32:18' as time)));
    """
    qt_select """
        select sec_to_time(59538);
    """
    // "HHMMSS"
    qt_select """
        select cast("123456" as TIME);
    """
    qt_select """
        select cast("3456" as TIME);    
    """
    qt_select """
        select cast("56" as TIME);
    """
    
    // "HH:MM:SS"
    qt_select """
        select cast("12:34:56" as TIME);
    """
    qt_select """
        select cast("34:56" as TIME);    
    """
    qt_select """
        select cast(":56" as TIME);
    """

    // HHMMSS
    qt_select """
        select cast(123456 as TIME);
    """
    qt_select """
        select cast(3456 as TIME);
    """
    qt_select """
        select cast(56 as TIME);
    """

    // Invalid value in seconds part.
    qt_select """
        select cast(":61" as TIME);
    """
    qt_select """
        select cast("61" as TIME);
    """
    qt_select """
        select cast(61 as TIME);
    """

    qt_select """
        select sec_to_time(time_to_sec(cast("61" as time)));
    """

    qt_select """
        select time_to_sec(timediff('2024-01-22', '2024-01-15')) as seconds;
    """
}
