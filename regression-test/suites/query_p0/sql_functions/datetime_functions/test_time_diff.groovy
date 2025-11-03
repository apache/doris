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

suite("test_time_diff") {
    qt_sql """SELECT minutes_diff('2020-02-02 15:30:00', '1951-02-16 15:27:00'); """
    qt_sql """SELECT minutes_diff('2020-02-02 15:30:00', '1952-02-16 15:27:00'); """
    qt_sql """SELECT minutes_diff('2020-02-02 15:30:00', '1900-02-16 15:27:00'); """
    qt_sql """SELECT hours_diff('9020-02-02 15:30:00', '1900-02-16 15:27:00');   """
    qt_sql """SELECT seconds_diff('3000-02-02 15:30:00', '1900-02-16 15:27:00'); """
    qt_sql """SELECT seconds_diff('3000-01-01 00:00:00', '1000-01-01 00:00:00'); """
    qt_sql """SELECT milliseconds_diff('2020-02-02 15:30:00', '1951-02-16 15:27:00'); """
    qt_sql """SELECT microseconds_diff('2020-02-02 15:30:00', '1951-02-16 15:27:00'); """

}