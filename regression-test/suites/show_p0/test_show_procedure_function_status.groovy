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

suite("test_show_procedure_function_status") {
    qt_show_procedure_status """SHOW PROCEDURE STATUS;"""
    qt_show_procedure_status_where """SHOW PROCEDURE STATUS WHERE Db = 'data_warehouse_ods';"""
    qt_show_procedure_status_like """SHOW PROCEDURE STATUS LIKE 'test';"""
    qt_show_function_status """SHOW FUNCTION STATUS;"""
    qt_show_function_status_where """SHOW FUNCTION STATUS WHERE Db = 'data_warehouse_ods';"""
    qt_show_function_status_like """SHOW FUNCTION STATUS LIKE 'test';"""
    qt_routines """SELECT * FROM information_schema.routines;"""
}
