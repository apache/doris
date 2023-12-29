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
public enum ParseDialect {

    /**
     * Trino parser and it's version is 395.
     */
    TRINO_395(Dialect.TRINO, Version.TRINO_395),
    /**
     * Doris parser and it's version is 2.0.0.
     */
    DORIS_2_ALL(Dialect.DORIS, Version.DORIS_2_ALL),
    /**
     * Spark parser and it's version is 3.x.
     */
    SPARK_SQL_3_ALL(Dialect.SPARK_SQL, Version.SPARK_SQL_3_ALL);

    private final Dialect dialect;
    private final Version version;

    ParseDialect(Dialect dialect, Version version) {
        this.dialect = dialect;
        this.version = version;
    }

    public Version getVersion() {
        return version;
    }

    public Dialect getDialect() {
        return dialect;
    }

    /**
     * The version of parse dialect.
     */
    public enum Version {
        /**
         * Trino parser and it's version is 395.
         */
        TRINO_395("395"),
        /**
         * Doris parser and it's version is 2.0.0.
         */
        DORIS_2_ALL("2.*"),
        /**
         * Spark sql parser and it's version is 3.x.
         */
        SPARK_SQL_3_ALL("3.*");
        private final String version;

        Version(String version) {
            this.version = version;
        }

        public String getVersionName() {
            return version;
        }
    }

    /**
     * The dialect name of parse dialect.
     */
    public enum Dialect {
        /**
         * Trino parser dialect
         */
        TRINO("trino"),
        /**
         * Presto parser dialect
         */
        PRESTO("presto"),
        /**
         * Doris parser dialect
         */
        DORIS("doris"),
        /**
         * Spark sql parser dialect
         */
        SPARK_SQL("spark_sql");

        private String dialectName;

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
}
