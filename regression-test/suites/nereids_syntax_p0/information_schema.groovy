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

import java.util.regex.Pattern;

suite("information_schema") {
    List<List<Object>> table =  sql """ select * from backends(); """
    assertTrue(table.size() > 0)
    assertTrue(table[0].size() == 25)

    sql "SELECT DATABASE();"
    sql "select USER();"
    sql "SELECT CONNECTION_ID();"
    sql "SELECT CURRENT_USER();"

    // INFORMATION_SCHEMA
    sql "SELECT table_name FROM INFORMATION_SCHEMA.TABLES where table_schema=\"test%\" and TABLE_TYPE = \"BASE TABLE\" order by table_name"
    sql "SELECT COLUMNS.COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = \"test%\" AND column_name LIKE \"k%\""

    def check_user_with_ip = { user_ret ->
        logger.info("$user_ret")
        String user_ret_str = user_ret[0][0]
        String[] user_ret_array = user_ret_str.split("@");
        if (user_ret_array.length != 2) {
            Assert.fail()
        }
        String username = user_ret_array[0].replaceAll("'", "")
        String user_ip = user_ret_array[1].replaceAll("'", "")

        Pattern IPV4_PATTERN = Pattern.compile("^((25[0-5]|2[0-4]\\d|1\\d{2}|[1-9]?\\d)(\\.|\$)){4}\$")
        Pattern IPV6_PATTERN = Pattern.compile("^((?:[\\da-fA-F]{1,4}(?::[\\da-fA-F]{1,4})*)?)::((?:[\\da-fA-F]{1,4}(?::[\\da-fA-F]{1,4})*)?)\$")
        if (!IPV4_PATTERN.matcher(user_ip).matches() && !IPV6_PATTERN.matcher(user_ip).matches()) {
            Assert.fail()
        }

        if (IPV4_PATTERN.matcher(username).matches() || IPV6_PATTERN.matcher(username).matches()) {
            Assert.fail()
        }
    }
    // check select user
    def user_ret = sql "SELECT USER()"
    check_user_with_ip.call(user_ret)

    // check select session_user
    def user_session_ret = sql "SELECT SESSION_USER()"
    check_user_with_ip.call(user_session_ret)

    // check select user
    def current_user = sql "select CURRENT_USER()"
    logger.info("$current_user")
    String current_user_str = current_user[0][0]
    String[] current_user_str_arr = current_user_str.split("@")
    if (current_user_str_arr.length != 2) {
        Assert.fail()
    }
    if (!"'%'".equals(current_user_str_arr[1])) {
        Assert.fail()
    }
}
