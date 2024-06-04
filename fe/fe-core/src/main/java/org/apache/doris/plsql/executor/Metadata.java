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
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/executor/Metadata.java
// and modified by Doris

package org.apache.doris.plsql.executor;

import org.apache.doris.catalog.Type;

import java.util.List;

public class Metadata {
    private final List<ColumnMeta> columnMetas;

    public Metadata(List<ColumnMeta> columnMetas) {
        this.columnMetas = columnMetas;
    }

    public int columnCount() {
        return columnMetas.size();
    }

    public int jdbcType(int columnIndex) {
        return at(columnIndex).getJdbcType();
    }

    public String columnName(int columnIndex) {
        return at(columnIndex).getColumnName();
    }

    public String columnTypeName(int columnIndex) {
        return at(columnIndex).getTypeName();
    }

    public Type dorisType(int columnIndex) {
        return at(columnIndex).getDorisType();
    }

    private ColumnMeta at(int columnIndex) {
        return columnMetas.get(columnIndex);
    }
}
