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

// MySQL column type
// TYPE codes are defined in the file 'mysql/include/mysql_com.h' enum enum_field_types
// which is also demostrated in
// http://dev.mysql.com/doc/internals/en/com-query-response.html
// typeName from https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-type-conversions.html
public enum MysqlColType {
    MYSQL_TYPE_DECIMAL(0, "DECIMAL", "DECIMAL"),
    MYSQL_TYPE_TINY(1, "TINYINT", "TINY INT"),
    MYSQL_TYPE_SHORT(2, "SMALLINT", "SMALL INT"),
    MYSQL_TYPE_LONG(3, "INTEGER", "INT"),
    MYSQL_TYPE_FLOAT(4, "FLOAT", "FLOAT"),
    MYSQL_TYPE_DOUBLE(5, "DOUBLE", "DOUBLE"),
    MYSQL_TYPE_NULL(6, "NULL", "NULL"),
    MYSQL_TYPE_TIMESTAMP(7, "TIMESTAMP", "TIMESTAMP"),
    MYSQL_TYPE_LONGLONG(8, "BIGINT", "LONGLONG"),
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
    MYSQL_TYPE_NEWDECIMAL(246, "NEWDECIMAL", "NEW DECIMAL"),
    MYSQL_TYPE_ENUM(247, "CHAR", "ENUM"),
    MYSQL_TYPE_SET(248, "CHAR", "SET"),
    MYSQL_TYPE_TINY_BLOB(249, "TINYBLOB", "TINY BLOB"),
    MYSQL_TYPE_MEDIUM_BLOB(250, "MEDIUMBLOB", "MEDIUM BLOB"),
    MYSQL_TYPE_LONG_BLOB(251, "LONGBLOB", "LONG BLOB"),
    MYSQL_TYPE_BLOB(252, "BLOB", "BLOB"),
    MYSQL_TYPE_VARSTRING(253, "VARSTRING", "VAR STRING"),
    MYSQL_TYPE_STRING(254, "CHAR", "STRING"),
    MYSQL_TYPE_GEOMETRY(255, "GEOMETRY", "GEOMETRY"),
    MYSQL_TYPE_MAP(400, "MAP", "MAP");

    private MysqlColType(int code, String typeName, String desc) {
        this.code = code;
        this.typeName = typeName;
        this.desc = desc;
    }

    // used in network
    private int code;

    private String typeName;

    private String desc;

    public int getCode() {
        return code;
    }

    public String getTypeName() {
        return typeName;
    }

    @Override
    public String toString() {
        return desc;
    }
}
