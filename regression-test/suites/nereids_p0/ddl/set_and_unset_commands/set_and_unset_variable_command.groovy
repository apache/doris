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

suite("set_and_unset_variable_command") {
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    checkNereidsExecute("UNSET VARIABLE ALL")
    checkNereidsExecute("UNSET global VARIABLE ALL")

    checkNereidsExecute("set wait_timeout = 1000")
    qt_cmd """show variables like 'wait_timeout'"""

    checkNereidsExecute("UNSET VARIABLE wait_timeout")
    qt_cmd """show variables like 'wait_timeout'"""

    checkNereidsExecute("set runtime_filter_type='BLOOM_FILTER'")
    qt_cmd """show session variables like 'runtime_filter_type'"""
    qt_cmd """show global variables like 'runtime_filter_type'"""
    checkNereidsExecute("UNSET VARIABLE runtime_filter_type")
    qt_cmd """show session variables like 'runtime_filter_type'"""
    qt_cmd """show global variables like 'runtime_filter_type'"""

    checkNereidsExecute("set global runtime_filter_type='BLOOM_FILTER'")
    qt_cmd """show session variables like 'runtime_filter_type'"""
    qt_cmd """show global variables like 'runtime_filter_type'"""
    checkNereidsExecute("UNSET global VARIABLE runtime_filter_type")
    qt_cmd """show session variables like 'runtime_filter_type'"""
    qt_cmd """show global variables like 'runtime_filter_type'"""

    // test variables with experimental_ prefix in session scope
    checkNereidsExecute("set experimental_enable_agg_state='true'")
    qt_cmd """show session variables like 'experimental_enable_agg_state'"""
    qt_cmd """show global variables like 'experimental_enable_agg_state'"""
    checkNereidsExecute("UNSET VARIABLE experimental_enable_agg_state")
    qt_cmd """show session variables like 'experimental_enable_agg_state'"""
    qt_cmd """show global variables like 'experimental_enable_agg_state'"""

    // test variables with experimental_ prefix in global scope
    checkNereidsExecute("set global experimental_enable_agg_state='true'")
    qt_cmd """show session variables like 'experimental_enable_agg_state'"""
    qt_cmd """show global variables like 'experimental_enable_agg_state'"""
    checkNereidsExecute("UNSET global VARIABLE experimental_enable_agg_state")
    qt_cmd """show session variables like 'experimental_enable_agg_state'"""
    qt_cmd """show global variables like 'experimental_enable_agg_state'"""

    // test variables with deprecated_ prefix
    checkNereidsExecute("set deprecated_enable_local_exchange = false")
    qt_cmd """show session variables like 'deprecated_enable_local_exchange'"""
    qt_cmd """show global variables like 'deprecated_enable_local_exchange'"""
    checkNereidsExecute("UNSET global VARIABLE deprecated_enable_local_exchange")
    qt_cmd """show session variables like 'deprecated_enable_local_exchange'"""
    qt_cmd """show global variables like 'deprecated_enable_local_exchange'"""

    // test UNSET VARIABLE ALL
    checkNereidsExecute("set runtime_filter_type='BLOOM_FILTER'")
    checkNereidsExecute("set experimental_enable_agg_state='true'")
    checkNereidsExecute("set deprecated_enable_local_exchange = false")
    checkNereidsExecute("set show_hidden_columns=true")
    checkNereidsExecute("UNSET VARIABLE ALL")
    qt_cmd """show session variables like 'runtime_filter_type'"""
    qt_cmd """show session variables like 'experimental_enable_agg_state'"""
    qt_cmd """show session variables like 'deprecated_enable_local_exchange'"""
    qt_cmd """show session variables like 'show_hidden_columns'"""

    qt_cmd """select * from information_schema.session_variables where variable_name = 'show_hidden_columns'"""

    // test UNSET GLOBAL VARIABLE ALL
    checkNereidsExecute("set global runtime_filter_type='BLOOM_FILTER'")
    checkNereidsExecute("set global experimental_enable_agg_state='true'")
    checkNereidsExecute("set global deprecated_enable_local_exchange = false")
    checkNereidsExecute("set show_hidden_columns=true")
    checkNereidsExecute("UNSET global VARIABLE ALL")
    qt_cmd """show global variables like 'runtime_filter_type'"""
    qt_cmd """show global variables like 'experimental_enable_agg_state'"""
    qt_cmd """show global variables like 'deprecated_enable_local_exchange'"""
    qt_cmd """show global variables like 'show_hidden_columns'"""

    qt_cmd """select * from information_schema.global_variables where variable_name = 'show_hidden_columns'"""

    // test read_only
    qt_cmd """show variables like 'read_only'"""
    test {
        sql "set read_only=true"
        exception "should be set with SET GLOBAL"
    }
    qt_cmd "set global read_only=true"
    qt_cmd """show global variables like 'read_only'"""
    qt_cmd """show variables like 'read_only'"""
    sql "set global read_only=false"

    // test super_read_only
    qt_cmd """show variables like 'super_read_only'"""
    test {
        sql "set super_read_only=true"
        exception "should be set with SET GLOBAL"
    }
    qt_cmd "set global super_read_only=true"
    qt_cmd """show global variables like 'super_read_only'"""
    qt_cmd """show variables like 'super_read_only'"""
    sql "set global super_read_only=false"
}
