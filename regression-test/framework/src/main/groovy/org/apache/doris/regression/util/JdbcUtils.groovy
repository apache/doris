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

package org.apache.doris.regression.util

import com.google.common.collect.ImmutableList
import groovy.lang.Tuple2

import java.sql.Connection
import java.sql.ResultSet
import java.sql.ResultSetMetaData

class JdbcUtils {
    static Tuple2<List<List<Object>>, ResultSetMetaData> executeToList(Connection conn, String sql) {
        conn.prepareStatement(sql).withCloseable { stmt ->
            boolean hasResultSet = stmt.execute()
            if (!hasResultSet) {
                return [ImmutableList.of(ImmutableList.of(stmt.getUpdateCount())), null]
            } else {
                return toList(stmt.resultSet)
            }
        }
    }

    static Tuple2<List<List<Object>>, ResultSetMetaData> executeToStringList(Connection conn, String sql) {
        conn.prepareStatement(sql).withCloseable { stmt ->
            boolean hasResultSet = stmt.execute()
            if (!hasResultSet) {
                return [ImmutableList.of(ImmutableList.of(stmt.getUpdateCount())), null]
            } else {
                return toStringList(stmt.resultSet)
            }
        }
    }

    static Tuple2<List<List<Object>>, ResultSetMetaData> toList(ResultSet resultSet) {
        resultSet.withCloseable {
            List<List<Object>> rows = new ArrayList<>()
            def columnCount = resultSet.metaData.columnCount
            while (resultSet.next()) {
                def row = new ArrayList<>()
                for (int i = 1; i <= columnCount; ++i) {
                    row.add(resultSet.getObject(i))
                }
                rows.add(row)
            }
            return [rows, resultSet.metaData]
        }
    }

    static Tuple2<List<List<Object>>, ResultSetMetaData> toStringList(ResultSet resultSet) {
        resultSet.withCloseable {
            List<List<Object>> rows = new ArrayList<>()
            def columnCount = resultSet.metaData.columnCount
            while (resultSet.next()) {
                def row = new ArrayList<>()
                for (int i = 1; i <= columnCount; ++i) {
                    try {
                        row.add(resultSet.getObject(i))
                    } catch (Throwable t) {
                        if(resultSet.getBytes(i) != null){
                            row.add(new String(resultSet.getBytes(i)))
                        } else {
                            row.add(resultSet.getObject(i))
                        }
                    }
                }
                rows.add(row)
            }
            return [rows, resultSet.metaData]
        }
    }
}
