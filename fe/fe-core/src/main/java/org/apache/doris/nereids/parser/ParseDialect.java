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
    DORIS_2_(Dialect.DORIS, Version.DORIS_2_);

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
        DORIS_2_("2.*");
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
         * Doris parser dialect
         */
        DORIS("doris");

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
        public static Dialect getByName(String dialectName) {
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
