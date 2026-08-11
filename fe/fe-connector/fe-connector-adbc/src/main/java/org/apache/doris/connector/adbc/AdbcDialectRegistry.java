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

import org.apache.doris.connector.spi.DorisConnectorException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * The dialects this connector can speak, by name.
 *
 * <p><b>The point of the registry is the cost of the next dialect.</b> Adding one must be a class plus a
 * {@link #register} call, with no edit to {@link AdbcQueryBuilder} and none to the selection code here --
 * an ADBC source is any driver implementing the C ABI, so the set cannot be closed and a switch over it
 * would have to be reopened for every source anyone connects. A test drives the query builder with a
 * dialect it defines itself to keep that true.
 *
 * <p>Two ship: {@link AnsiDialect} and {@link DorisDialect}, one per source actually run against. The
 * others named in the design wait for a source to verify them against: an unverified dialect is a set of
 * guesses about someone else's SQL, and a wrong guess is a syntax error at scan time or, worse, a predicate
 * that quietly selects different rows.
 */
public final class AdbcDialectRegistry {

    /** Case-insensitive so a name is matched the way a user or a driver spells it. */
    private static final Map<String, AdbcDialect> DIALECTS =
            new ConcurrentSkipListMap<>(String.CASE_INSENSITIVE_ORDER);

    private static final AdbcDialect DEFAULT = new AnsiDialect();

    static {
        register(DEFAULT);
        register(new DorisDialect());
    }

    private AdbcDialectRegistry() {
    }

    /** Registers {@code dialect} under its own name, replacing any dialect already using that name. */
    public static void register(AdbcDialect dialect) {
        DIALECTS.put(dialect.name(), dialect);
    }

    /** The dialect used when nothing else identifies the source. */
    public static AdbcDialect defaultDialect() {
        return DEFAULT;
    }

    /**
     * Looks up an explicitly requested dialect, failing with the registered names when there is no such one.
     *
     * <p>Fails rather than falling back to the default: the user named a dialect because the default was
     * wrong for their source, so silently using it anyway would produce SQL their source rejects, at scan
     * time, with a message pointing at the SQL rather than at the misspelled property.
     */
    public static AdbcDialect require(String name) {
        AdbcDialect dialect = DIALECTS.get(name);
        if (dialect == null) {
            throw new DorisConnectorException("Unknown '" + AdbcCatalogProperties.SQL_DIALECT + "' value '"
                    + name + "' for an adbc catalog. Registered dialects: " + registeredNames());
        }
        return dialect;
    }

    /**
     * Finds the dialect claiming {@code vendorName}, as an ADBC driver reports it through
     * {@code getInfo(VENDOR_NAME)}. Empty when no dialect claims it, which is the normal outcome.
     */
    public static Optional<AdbcDialect> forVendor(String vendorName) {
        if (vendorName == null || vendorName.trim().isEmpty()) {
            return Optional.empty();
        }
        String trimmed = vendorName.trim();
        for (AdbcDialect dialect : DIALECTS.values()) {
            if (dialect.matchesVendor(trimmed)) {
                return Optional.of(dialect);
            }
        }
        return Optional.empty();
    }

    /** The registered names, sorted, for error messages. */
    public static List<String> registeredNames() {
        return new ArrayList<>(DIALECTS.keySet());
    }
}
