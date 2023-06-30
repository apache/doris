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
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/executor/QueryResult.java
// and modified by Doris

package org.apache.doris.hplsql.executor;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

public class QueryResult {
    private final RowResult rows;
    private final Supplier<Metadata> metadata;
    private final Exception exception;

    public QueryResult(RowResult rows, Supplier<Metadata> metadata, Exception exception) {
        this.rows = rows;
        this.metadata = memoize(metadata);
        this.exception = exception;
    }

    public boolean next() {
        return rows.next();
    }

    public int columnCount() {
        return metadata().columnCount();
    }

    /**
     * Get the nth column from the row result.
     * The index is 0 based unlike in JDBC.
     */
    public <T> T column(int columnIndex, Class<T> type) {
        return rows.get(columnIndex, type);
    }

    public ByteBuffer mysqlRow() {
        return rows.getMysqlRow();
    }

    public boolean error() {
        return exception != null;
    }

    public void printStackTrace() {
        if (exception != null) {
            exception.printStackTrace();
        }
    }

    public Exception exception() {
        return exception;
    }

    public Metadata metadata() {
        return metadata.get();
    }

    public int jdbcType(int columnIndex) {
        return metadata().jdbcType(columnIndex);
    }

    public void close() {
        if (rows != null) {
            rows.close();
        }
    }

    private static <T> Supplier<T> memoize(Supplier<? extends T> supplier) {
        return com.google.common.base.Suppliers.memoize(supplier::get)::get; // cache the supplier result
    }

    public String errorText() {
        if (exception != null) {
            if (exception instanceof ClassNotFoundException) {
                return "ClassNotFoundException: " + exception.getMessage();
            }
            return exception.getMessage();
        }
        return "";
    }
}
