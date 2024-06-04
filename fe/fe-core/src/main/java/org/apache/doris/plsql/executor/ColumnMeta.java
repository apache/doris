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
// This file is copied from
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/executor/ColumnMeta.java
// and modified by Doris

package org.apache.doris.plsql.executor;

import org.apache.doris.catalog.Type;

public class ColumnMeta {
    private final String columnName;
    private final String typeName;
    private final int jdbcType;
    private final Type dorisType;

    public ColumnMeta(String columnName, String typeName, int jdbcType) {
        this(columnName, typeName, jdbcType, Type.INVALID);
    }

    public ColumnMeta(String columnName, String typeName, int jdbcType, Type dorisType) {
        this.columnName = columnName;
        this.typeName = typeName;
        this.jdbcType = jdbcType;
        this.dorisType = dorisType;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getTypeName() {
        return typeName;
    }

    public int getJdbcType() {
        return jdbcType;
    }

    public Type getDorisType() {
        return dorisType;
    }
}
