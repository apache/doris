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
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLFeatureNotSupportedException
import java.sql.Types
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class JdbcUtils {
    private static final Logger log = LoggerFactory.getLogger(JdbcUtils.class)
    static String replaceHostUrl(String originUri, String newHost) {
        def prefix = originUri.substring(0, originUri.indexOf("://") + 3)
        def postIndex = originUri.indexOf(":", originUri.indexOf("://") + 3)
        if (postIndex == -1) {
            postIndex = originUri.indexOf("/", originUri.indexOf("://") + 3)
        }
        if (postIndex == -1) {
            postIndex = originUri.length()
        }
        return prefix + newHost + originUri.substring(postIndex)
    }

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

    static Tuple2<List<List<Object>>, ResultSetMetaData> executeQueryToList(Connection conn, String sql) {
        conn.createStatement().withCloseable { stmt ->
            return toList(stmt.executeQuery(sql))
        }
    }

    static PreparedStatement prepareStatement(Connection conn, String sql) {
        return conn.prepareStatement(sql);
    }

    static List<Map<String, Object>> executeToMapArray(Connection conn, String sql) {
        def (result, meta) = JdbcUtils.executeToList(conn, sql)

        // get all column names as list
        List<String> columnNames = new ArrayList<>()
        for (int i = 0; i < meta.getColumnCount(); i++) {
            columnNames.add(meta.getColumnName(i + 1))
        }

        // add result to res map list, each row is a map with key is column name
        List<Map<String, Object>> res = new ArrayList<>()
        for (int i = 0; i < result.size(); i++) {
            Map<String, Object> row = new HashMap<>()
            for (int j = 0; j < columnNames.size(); j++) {
                row.put(columnNames.get(j), result.get(i).get(j))
            }
            res.add(row)
        }
        return res;
    }

    static Tuple2<List<List<Object>>, ResultSetMetaData> executeToList(Connection conn, PreparedStatement stmt) {
        boolean hasResultSet = stmt.execute()
        if (!hasResultSet) {
            return [ImmutableList.of(ImmutableList.of(stmt.getUpdateCount())), null]
        } else {
            return toList(stmt.resultSet)
        } 
    }

    static Tuple2<List<List<Object>>, ResultSetMetaData> executeToStringList(Connection conn, PreparedStatement stmt) {
        return toStringList(stmt.executeQuery(), true)
    }

    // static Tuple2<List<List<Object>>, ResultSetMetaData> executeToStringList(Connection conn, PreparedStatement stmt) {
    //     boolean hasResultSet = stmt.execute()
    //     if (!hasResultSet) {
    //         return [ImmutableList.of(ImmutableList.of(stmt.getUpdateCount())), null]
    //     } else {
    //         return toStringList(stmt.resultSet)
    //     }
    // }

    static Tuple2<List<List<Object>>, ResultSetMetaData> executeToStringList(Connection conn, String sql) {
        conn.prepareStatement(sql).withCloseable { stmt ->
            boolean hasResultSet = stmt.execute()
            if (!hasResultSet) {
                return [ImmutableList.of(ImmutableList.of(stmt.getUpdateCount())), null]
            } else {
                return toStringList(stmt.resultSet, false)
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
                    int jdbcType = resultSet.metaData.getColumnType(i)
                    if (isBinaryJdbcType(jdbcType)) {
                        try {
                            byte[] bytes = resultSet.getBytes(i)
                            row.add(bytes == null ? null : bytesToHex(bytes))
                        } catch (SQLFeatureNotSupportedException ignored) {
                            row.add(resultSet.getObject(i))
                        }
                    } else {
                        row.add(materializeObject(resultSet.getObject(i)))
                    }
                }
                rows.add(row)
            }
            return [rows, resultSet.metaData]
        }
    }

    static Tuple2<List<List<Object>>, ResultSetMetaData> toStringList(ResultSet resultSet, boolean isPreparedStatement) {
        resultSet.withCloseable {
            List<List<Object>> rows = new ArrayList<>()
            def columnCount = resultSet.metaData.columnCount
            while (resultSet.next()) {
                def row = new ArrayList<>()
                for (int i = 1; i <= columnCount; ++i) {
                    try {
                        if (isPreparedStatement) {
                            // For prepared statements, use getObject to get the value
                            row.add(materializeObject(resultSet.getObject(i)))
                        } else {
                            int jdbcType = resultSet.metaData.getColumnType(i)
                            if (jdbcType == Types.TIME
                                || jdbcType == Types.FLOAT
                                || jdbcType == Types.DOUBLE) {
                                // For normal statements, use the string representation
                                // to keep the original format returned by Doris
                                /*
                                 * For time types, there are three ways to save the results returned by Doris:
                                 *   1. Default behavior: row.add(resultSet.getObject(i))
                                 *      which will return a Time object.
                                 *      Use the Time type will lose the fractional precision of the time.
                                 *   2. Use LocalTime: row.add(resultSet.getColumn(i, LocalTime.class))
                                 *      which will lose the padding zeros of the fractional precision.
                                 *      For example, 0:0:0.123000 can only retain 0:0:0.123.
                                 *   3. Use a string: row.add(new String(resultSet.getBytes(i)))
                                 *      This can preserve the full precision, so the third solution is preferred.
                                */
                                row.add(new String(resultSet.getBytes(i)))
                            } else if (isBinaryJdbcType(jdbcType)) {
                                byte[] bytes = resultSet.getBytes(i)
                                row.add(bytes == null ? null : bytesToHex(bytes))
                            } else {
                                row.add(materializeObject(resultSet.getObject(i)))
                            }
                        }
                    } catch (Throwable t) {
                        try {
                            if(resultSet.getBytes(i) != null){
                                row.add(new String(resultSet.getBytes(i)))
                            } else {
                                row.add(materializeObject(resultSet.getObject(i)))
                            }
                        } catch (Throwable t2) {
                            /**
                            suites/external_table_p0/export/hive_read/orc/test_hive_read_orc_complex_type.groovy failed
                            java.sql.SQLFeatureNotSupportedException: Method not supported
                            at org.apache.hive.jdbc.HiveBaseResultSet.getBytes(HiveBaseResultSet.java:221)
                            at org.codehaus.groovy.vmplugin.v8.IndyInterface.fromCache(IndyInterface.java:321)
                            at org.apache.doris.regression.util.JdbcUtils$_toStringList_closure5.doCall(JdbcUtils.groovy:163)
                            */
                            row.add(materializeObject(resultSet.getObject(i)))
                        }
                    }
                }
                rows.add(row)
            }
            return [rows, resultSet.metaData]
        }
    }

    private static Object materializeObject(Object value) {
        if (value instanceof java.sql.Array) {
            return renderComplexValue(((java.sql.Array) value).getArray())
        }
        if (value instanceof java.sql.Struct) {
            return renderComplexValue(((java.sql.Struct) value).getAttributes())
        }
        if (isArrowJsonStringContainer(value)) {
            return renderComplexValue(value)
        }
        if (value instanceof byte[]) {
            return bytesToHex((byte[]) value)
        }
        if (isGenericComplexContainer(value)) {
            return renderComplexValue(value)
        }
        return value
    }

    private static boolean isGenericComplexContainer(Object value) {
        return value != null && (value instanceof Map
                || value.getClass().isArray()
                || value instanceof Iterable)
    }

    private static boolean isArrowJsonStringContainer(Object value) {
        if (value == null) {
            return false
        }
        String className = value.getClass().getName()
        return className.endsWith(".JsonStringHashMap") || className.endsWith(".JsonStringArrayList")
    }

    private static String renderComplexValue(Object value) {
        if (value == null) {
            return "null"
        }
        if (value instanceof java.sql.Array) {
            return renderComplexValue(((java.sql.Array) value).getArray())
        }
        if (value instanceof java.sql.Struct) {
            return renderComplexValue(((java.sql.Struct) value).getAttributes())
        }
        if (value instanceof Map) {
            StringBuilder sb = new StringBuilder()
            sb.append("{")
            boolean first = true
            ((Map<?, ?>) value).each { Object key, Object mapValue ->
                if (!first) {
                    sb.append(", ")
                }
                sb.append(renderComplexKey(key)).append(":").append(renderComplexValue(mapValue))
                first = false
            }
            sb.append("}")
            return sb.toString()
        }
        if (value instanceof byte[]) {
            return bytesToHex((byte[]) value)
        }
        if (value.getClass().isArray()) {
            StringBuilder sb = new StringBuilder()
            sb.append("[")
            int length = java.lang.reflect.Array.getLength(value)
            for (int i = 0; i < length; i++) {
                if (i > 0) {
                    sb.append(", ")
                }
                sb.append(renderComplexValue(java.lang.reflect.Array.get(value, i)))
            }
            sb.append("]")
            return sb.toString()
        }
        if (value instanceof Iterable) {
            StringBuilder sb = new StringBuilder()
            sb.append("[")
            boolean first = true
            ((Iterable<?>) value).each { Object item ->
                if (!first) {
                    sb.append(", ")
                }
                sb.append(renderComplexValue(item))
                first = false
            }
            sb.append("]")
            return sb.toString()
        }
        if (isStringLikeComplexValue(value)) {
            return "\"" + escapeComplexString(value.toString()) + "\""
        }
        return value.toString()
    }

    private static String renderComplexKey(Object key) {
        if (key == null) {
            return "null"
        }
        if (isStringLikeComplexValue(key)) {
            return "\"" + escapeComplexString(key.toString()) + "\""
        }
        return renderComplexValue(key)
    }

    private static boolean isStringLikeComplexValue(Object value) {
        return value instanceof CharSequence
                || isTextLikeObject(value)
                || value instanceof java.time.temporal.TemporalAccessor
                || value instanceof java.util.Date
    }

    private static boolean isTextLikeObject(Object value) {
        return value != null && value.getClass().getName().endsWith(".Text")
    }

    private static String escapeComplexString(String value) {
        if (value == null) {
            return null
        }
        return value
    }

    // Detect if a JDBC column type is binary-like
    private static boolean isBinaryJdbcType(int jdbcType) {
        return jdbcType == Types.BINARY
                || jdbcType == Types.VARBINARY
                || jdbcType == Types.LONGVARBINARY
                || jdbcType == Types.BLOB
    }

    // Convert byte array to upper-case hex string with 0x prefix
    private static String bytesToHex(byte[] bytes) {
        if (bytes == null) return null
        StringBuilder sb = new StringBuilder(2 + bytes.length * 2)
        sb.append("0x")
        for (byte b : bytes) {
            sb.append(String.format("%02X", b & 0xFF))
        }
        return sb.toString()
    }
}
