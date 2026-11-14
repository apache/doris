/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.shyiko.mysql.binlog.event.deserialization;

import java.util.HashMap;
import java.util.Map;

/**
 * Copied from Flink CDC 3.6.0-1.20 (flink-connector-mysql-cdc).
 *
 * <p>Line 57: add VECTOR with type code 242; retain the existing TYPED_ARRAY type codes.
 */
public enum ColumnType {
    DECIMAL(0),
    TINY(1),
    SHORT(2),
    LONG(3),
    FLOAT(4),
    DOUBLE(5),
    NULL(6),
    TIMESTAMP(7),
    LONGLONG(8),
    INT24(9),
    DATE(10),
    TIME(11),
    DATETIME(12),
    YEAR(13),
    NEWDATE(14),
    VARCHAR(15),
    BIT(16),
    // (TIMESTAMP|DATETIME|TIME)_V2 data types appeared in MySQL 5.6.4
    // @see http://dev.mysql.com/doc/internals/en/date-and-time-data-type-representation.html
    TIMESTAMP_V2(17),
    DATETIME_V2(18),
    TIME_V2(19),
    TYPED_ARRAY(20),
    // TYPED_ARRAY enum value has been changed from 244 to 20 in MySQL 8.0.18. Since the JSON_ARRAY
    // cast syntax was not added before MySQL 8.0.16, and the TYPED_ARRAY enum has been fixed in
    // MySQL 8.0.18, so the only affected version is 8.0.17.
    // https://github.com/mysql/mysql-server/commit/9082b6a820f3948fd563cc32a050f5e8775f2855#diff-b9bac49e04a17ad0503e56a4c53d979c90eb64618387d20b9ea2cf1dbf47e5e7L25
    TYPED_ARRAY_OLD(244),
    VECTOR(242),
    JSON(245),
    NEWDECIMAL(246),
    ENUM(247),
    SET(248),
    TINY_BLOB(249),
    MEDIUM_BLOB(250),
    LONG_BLOB(251),
    BLOB(252),
    VAR_STRING(253),
    STRING(254),
    GEOMETRY(255);

    private int code;

    private ColumnType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    private static final Map<Integer, ColumnType> INDEX_BY_CODE;

    static {
        INDEX_BY_CODE = new HashMap<Integer, ColumnType>();
        for (ColumnType columnType : values()) {
            INDEX_BY_CODE.put(columnType.code, columnType);
        }
    }

    public static ColumnType byCode(int code) {
        return INDEX_BY_CODE.get(code);
    }
}
