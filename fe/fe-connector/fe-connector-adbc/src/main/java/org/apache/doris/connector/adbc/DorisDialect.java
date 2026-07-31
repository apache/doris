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

package org.apache.doris.connector.adbc;

/**
 * Standard SQL as {@link AnsiDialect} renders it, with the one thing Doris spells differently: identifiers.
 *
 * <p>Doris reads a double-quoted name as a string literal, so ANSI quoting against a Doris source does not
 * merely look foreign -- {@code SELECT "id" FROM "db"."t1"} does not parse at all, which is how the first
 * Flight SQL scan against another Doris failed. Everything else ANSI produces (literals, the two-part table
 * name, {@code LIMIT n}) Doris accepts unchanged, so this dialect changes one method.
 *
 * <p>Its scope is Doris alone. Backticks are the whole MySQL family's spelling and MySQL or StarRocks would
 * very likely work, but neither has been run against, and a dialect that claims a source on family
 * resemblance is a guess about someone else's SQL -- the failure mode being a syntax error at scan time, or
 * a predicate that quietly matches different rows. Such a source can still ask for this dialect by name.
 */
public final class DorisDialect extends AnsiDialect {

    public static final String NAME = "doris";

    /**
     * What a Doris source calls itself through {@code getInfo(VENDOR_NAME)}. The value is its Flight SQL
     * server name, {@code DorisFE} (see {@code SqlInfoBuilder.withFlightSqlServerName}), so matching the
     * dialect's own name -- the default -- would never claim it. Matched as a prefix, case-insensitively,
     * so a source that reports plain {@code Doris} is not left on a dialect it cannot parse.
     */
    private static final String VENDOR_PREFIX = "doris";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public boolean matchesVendor(String vendorName) {
        return vendorName != null
                && vendorName.regionMatches(true, 0, VENDOR_PREFIX, 0, VENDOR_PREFIX.length());
    }

    @Override
    public String quoteIdentifier(String name) {
        return '`' + name.replace("`", "``") + '`';
    }
}
