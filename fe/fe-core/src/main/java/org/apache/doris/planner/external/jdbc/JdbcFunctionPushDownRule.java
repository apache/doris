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

package org.apache.doris.planner.external.jdbc;

import org.apache.doris.thrift.TOdbcTableType;

import java.util.TreeSet;

public class JdbcFunctionPushDownRule {
    private static final TreeSet<String> UNSUPPORTED_MYSQL_FUNCTIONS = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    static {
        UNSUPPORTED_MYSQL_FUNCTIONS.add("date_trunc");
        UNSUPPORTED_MYSQL_FUNCTIONS.add("money_format");
    }

    public static boolean isUnsupportedFunctions(TOdbcTableType tableType, String filter) {
        if (tableType.equals(TOdbcTableType.MYSQL)) {
            return isMySQLUnsupportedFunctions(filter);
        } else {
            return false;
        }
    }

    private static boolean isMySQLUnsupportedFunctions(String filter) {
        for (String func : UNSUPPORTED_MYSQL_FUNCTIONS) {
            if (filter.contains(func)) {
                return true;
            }
        }
        return false;
    }
}

