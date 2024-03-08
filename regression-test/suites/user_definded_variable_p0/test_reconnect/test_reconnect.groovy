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
suite("test_reconnect", "description") {

    // 1. Set the value of variable @A to 3 before starting execution
    sql """set @A = 3"""

    // 2. Disconnect the current connection
    disconnect()

    // 3. Reconnect and execute select @A to query the value of the variable
    def result = connect(user = 'root', password = '', url = context.config.jdbcUrl) {
        sql 'select @A'
    }

    // 4. Check if the result is NULL
    test {
        assert result[0][0] == null, "Variable @A should be NULL"
    }
}
