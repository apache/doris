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

package org.apache.doris.nereids.parser;

import javax.annotation.Nullable;

/**
 * ParseDialect enum, maybe support other dialect.
 */
public enum Dialect {
    /**
     * Doris parser dialect
     */
    DORIS("doris"),
    /**
     * Trino parser dialect
     */
    TRINO("trino"),
    /**
     * Presto parser dialect
     */
    PRESTO("presto"),
    /**
     * Spark3 sql parser dialect
     */
    SPARK("spark"),
    /**
     * Spark2 sql parser dialect
     */
    SPARK2("spark2"),
    /**
     * Flink sql parser dialect
     */
    FLINK("flink"),
    /**
     * Hive parser dialect
     */
    HIVE("hive"),
    /**
     * Postgresql parser dialect
     */
    POSTGRES("postgres"),
    /**
     * Sqlserver parser dialect
     */
    SQLSERVER("sqlserver"),
    /**
     * Clickhouse parser dialect
     */
    CLICKHOUSE("clickhouse"),
    /**
     * oracle parser dialect
     */
    ORACLE("oracle");

    public static final int MAX_DIALECT_SIZE = Dialect.values().length;

    private final String dialectName;

    Dialect(String dialectName) {
        this.dialectName = dialectName;
    }

    public String getDialectName() {
        return dialectName;
    }

    /**
     * Get dialect by name
     */
    public static @Nullable Dialect getByName(String dialectName) {
        if (dialectName == null) {
            return null;
        }
        for (Dialect dialect : Dialect.values()) {
            if (dialect.getDialectName().equals(dialectName.toLowerCase())) {
                return dialect;
            }
        }
        return null;
    }
}
