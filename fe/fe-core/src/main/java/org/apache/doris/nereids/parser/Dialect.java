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
     * Spark sql parser dialect
     */
    SPARK_SQL("spark_sql"),
    /**
     * Hive parser dialect
     */
    HIVE("hive"),
    /**
     * Iceberg parser dialect
     */
    ICEBERG("iceberg"),
    /**
     * Hudi parser dialect
     */
    HUDI("hudi"),
    /**
     * Paimon parser dialect
     */
    PAIMON("paimon"),
    /**
     * Alibaba dlf parser dialect
     */
    DLF("dlf"),
    /**
     * Alibaba max compute parser dialect
     */
    MAX_COMPUTE("max_compute"),
    /**
     * Mysql parser dialect
     */
    MYSQL("mysql"),
    /**
     * Postgresql parser dialect
     */
    POSTGRESQL("postgresql"),
    /**
     * Sqlserver parser dialect
     */
    SQLSERVER("sqlserver"),
    /**
     * Clickhouse parser dialect
     */
    CLICKHOUSE("clickhouse"),
    /**
     * Sap hana parser dialect
     */
    SAP_HANA("sap_hana"),
    /**
     * OceanBase parser dialect
     */
    OCEANBASE("oceanbase");

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
