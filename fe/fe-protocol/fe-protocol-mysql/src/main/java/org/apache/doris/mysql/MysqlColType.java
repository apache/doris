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

package org.apache.doris.mysql;

/**
 * MySQL column type codes.
 *
 * <p>These codes are used in the column definition packets to indicate
 * the data type of a column. They correspond to the MySQL field types
 * as defined in mysql_com.h.
 *
 * <p>Reference: https://dev.mysql.com/doc/dev/mysql-server/latest/field__types_8h.html
 */
public enum MysqlColType {
    MYSQL_TYPE_DECIMAL(0),
    MYSQL_TYPE_TINY(1),
    MYSQL_TYPE_SHORT(2),
    MYSQL_TYPE_LONG(3),
    MYSQL_TYPE_FLOAT(4),
    MYSQL_TYPE_DOUBLE(5),
    MYSQL_TYPE_NULL(6),
    MYSQL_TYPE_TIMESTAMP(7),
    MYSQL_TYPE_LONGLONG(8),
    MYSQL_TYPE_INT24(9),
    MYSQL_TYPE_DATE(10),
    MYSQL_TYPE_TIME(11),
    MYSQL_TYPE_DATETIME(12),
    MYSQL_TYPE_YEAR(13),
    MYSQL_TYPE_NEWDATE(14),
    MYSQL_TYPE_VARCHAR(15),
    MYSQL_TYPE_BIT(16),
    MYSQL_TYPE_TIMESTAMP2(17),
    MYSQL_TYPE_DATETIME2(18),
    MYSQL_TYPE_TIME2(19),
    MYSQL_TYPE_JSON(245),
    MYSQL_TYPE_NEWDECIMAL(246),
    MYSQL_TYPE_ENUM(247),
    MYSQL_TYPE_SET(248),
    MYSQL_TYPE_TINY_BLOB(249),
    MYSQL_TYPE_MEDIUM_BLOB(250),
    MYSQL_TYPE_LONG_BLOB(251),
    MYSQL_TYPE_BLOB(252),
    MYSQL_TYPE_VAR_STRING(253),
    MYSQL_TYPE_STRING(254),
    MYSQL_TYPE_GEOMETRY(255);

    private final int code;

    MysqlColType(int code) {
        this.code = code;
    }

    /**
     * Gets the type code.
     *
     * @return MySQL type code
     */
    public int getCode() {
        return code;
    }

    /**
     * Gets a type by its code.
     *
     * @param code MySQL type code
     * @return MysqlColType enum, or null if not found
     */
    public static MysqlColType fromCode(int code) {
        for (MysqlColType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        return null;
    }
}
