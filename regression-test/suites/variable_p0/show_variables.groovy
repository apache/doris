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

suite("show_variables") {
    qt_cmd """UNSET VARIABLE ALL"""
    qt_cmd """set wait_timeout = 2014"""
    qt_cmd """show variables like 'wait_timeout'"""
    qt_cmd """show variables where variable_name='wait_timeout'"""
    qt_cmd """show variables where changed='1';"""
    qt_cmd """select sum(cast (changed as int)) from information_schema.SESSION_VARIABLES where changed=1"""
}