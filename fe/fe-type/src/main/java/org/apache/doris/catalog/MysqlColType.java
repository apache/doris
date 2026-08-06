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

package org.apache.doris.catalog;

import java.util.HashMap;
import java.util.Map;

// MySQL column type
// TYPE codes are defined in the file 'mysql/include/mysql_com.h' enum enum_field_types
// which is also demostrated in
// http://dev.mysql.com/doc/internals/en/com-query-response.html
public enum MysqlColType {
    MYSQL_TYPE_DECIMAL(0, "DECIMAL", "DECIMAL"),
    MYSQL_TYPE_TINY(1, "TINY INT", "TINYINT"),
    MYSQL_TYPE_SHORT(2, "SMALL INT", "SMALLINT"),
    MYSQL_TYPE_LONG(3, "INT", "INTEGER"),
    MYSQL_TYPE_FLOAT(4, "FLOAT", "FLOAT"),
    MYSQL_TYPE_DOUBLE(5, "DOUBLE", "DOUBLE"),
    MYSQL_TYPE_NULL(6, "NULL", "NULL"),
    MYSQL_TYPE_TIMESTAMP(7, "TIMESTAMP", "TIMESTAMP"),
    MYSQL_TYPE_LONGLONG(8, "LONGLONG", "BIGINT"),
    MYSQL_TYPE_INT24(9, "INT24", "INT24"),
    MYSQL_TYPE_DATE(10, "DATE", "DATE"),
    MYSQL_TYPE_TIME(11, "TIME", "TIME"),
    MYSQL_TYPE_DATETIME(12, "DATETIME", "DATETIME"),
    MYSQL_TYPE_YEAR(13, "YEAR", "YEAR"),
    MYSQL_TYPE_NEWDATE(14, "NEWDATE", "NEWDATE"),
    MYSQL_TYPE_VARCHAR(15, "VARCHAR", "VARCHAR"),
    MYSQL_TYPE_BIT(16, "BIT", "BIT"),
    MYSQL_TYPE_TIMESTAMP2(17, "TIMESTAMP2", "TIMESTAMP2"),
    MYSQL_TYPE_DATETIME2(18, "DATETIME2", "DATETIME2"),
    MYSQL_TYPE_TIME2(19, "TIME2", "TIME2"),
    MYSQL_TYPE_JSON(245, "JSON", "JSON"),
    MYSQL_TYPE_NEWDECIMAL(246, "NEW DECIMAL", "NEWDECIMAL"),
    MYSQL_TYPE_ENUM(247, "ENUM", "CHAR"),
    MYSQL_TYPE_SET(248, "SET", "CHAR"),
    MYSQL_TYPE_TINY_BLOB(249, "TINY BLOB", "TINYBLOB"),
    MYSQL_TYPE_MEDIUM_BLOB(250, "MEDIUM BLOB", "MEDIUMBLOB"),
    MYSQL_TYPE_LONG_BLOB(251, "LONG BLOB", "LONGBLOB"),
    MYSQL_TYPE_BLOB(252, "BLOB", "BLOB"),
    MYSQL_TYPE_VARSTRING(253, "VAR STRING", "VARSTRING"),
    MYSQL_TYPE_STRING(254, "STRING", "CHAR"),
    MYSQL_TYPE_GEOMETRY(255, "GEOMETRY", "GEOMETRY"),
    MYSQL_TYPE_MAP(400, "MAP", "MAP");

    private static final Map<Integer, MysqlColType> CODE_MAP = new HashMap<>();

    public static final int MYSQL_CODE_MASK = 0xFF;

    public static final int UNSIGNED_MASK = 0x8000;

    static {
        for (MysqlColType type : MysqlColType.values()) {
            CODE_MAP.put(type.code, type);
        }
    }

    private MysqlColType(int code, String desc, String jdbcColumnTypeName) {
        this.code = code;
        this.desc = desc;
        this.jdbcColumnTypeName = jdbcColumnTypeName;
    }

    // used in network
    private int code;

    // for debug, string description of mysql column type code.
    private String desc;

    // MysqlTypeName to JdbcColumnTypeName, refer to:
    // https://dev.mysql.com/doc/connector-j/en/connector-j-reference-type-conversions.html
    // In plsql/Var.defineType(), Plsql Var type will be found through the Mysql type name string.
    // TODO, supports the correspondence between Doris type and Plsql Var.
    private final String jdbcColumnTypeName;

    public int getCode() {
        return code;
    }

    public static MysqlColType fromCode(int code) {
        // Use the lower 8 bits of the code.
        return CODE_MAP.get(code & MYSQL_CODE_MASK);
    }

    public static boolean isUnsigned(int code) {
        return (code & MysqlColType.UNSIGNED_MASK) != 0;
    }

    public String getJdbcColumnTypeName() {
        return jdbcColumnTypeName;
    }

    @Override
    public String toString() {
        return desc;
    }
}
