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

suite("test_set_session_var_exception") {
    test {
        sql """
        set query_timeout=0;
        """
        exception "query_timeout can not be set to 0, it must be greater than 0"
    }
    test {
        sql """
        set max_execution_time=999;
        """
        exception "max_execution_time can not be set to 999"
    }

    sql """
    set max_execution_time=1000;
    """

    sql """
    UNSET VARIABLE max_execution_time;
    """
}




