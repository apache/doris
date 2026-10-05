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

import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;

/**
 * The SQL spelling differences between the sources an ADBC driver can point at.
 *
 * <p><b>Why an interface rather than an enum.</b> The JDBC connector can enumerate its dialects because it
 * enumerates its drivers; ADBC cannot -- any driver implementing the ADBC C ABI is a valid source, including
 * ones written after this code ships. Adding one must therefore cost a class and a registration, and nothing
 * else: {@link AdbcQueryBuilder} is written against this interface and must never grow a per-dialect branch.
 * A test pins that by driving the builder with a dialect it defines itself.
 *
 * <p>The surface is deliberately only what generating {@code SELECT ... FROM ... WHERE ... LIMIT} needs. A
 * method with no caller could not be gotten right, because nothing would show when it was wrong.
 */
public interface AdbcDialect {

    /**
     * The name this dialect is selected by -- matched against the {@code sql_dialect} property and against
     * the vendor an ADBC driver reports. Compared case-insensitively.
     */
    String name();

    /**
     * Whether {@code vendorName}, as reported by the driver through {@code getInfo(VENDOR_NAME)}, means this
     * dialect. The default matches the dialect's own name, which is what makes registration a one-liner for
     * a dialect named after its vendor; a dialect covering several vendor spellings overrides it.
     */
    default boolean matchesVendor(String vendorName) {
        return name().equalsIgnoreCase(vendorName);
    }

    /** Renders {@code name} as a quoted identifier, escaping whatever the quote character is. */
    String quoteIdentifier(String name);

    /**
     * Renders the remote table name for a {@code FROM} clause.
     *
     * <p>Takes the handle rather than a joined string because the Doris database name is a display key that
     * is never parsed back into remote levels ({@link AdbcNamespace}); how many of the three remote levels a
     * source will accept in one name is exactly a dialect question (MySQL rejects a three-part name).
     */
    String qualifiedTableName(AdbcTableHandle handle);

    /**
     * Renders {@code literal} as SQL, or returns {@code null} when this dialect cannot render that type.
     *
     * <p><b>{@code null} is a real answer, not a failure.</b> A predicate is pushed down whole or not at all,
     * so an unrenderable literal has to be reportable without throwing -- the caller drops that conjunct and
     * leaves it to Doris. Throwing would turn "we cannot speed this up" into a failed query.
     */
    String renderLiteral(ConnectorLiteral literal);

    /**
     * Whether {@code LIMIT n} is accepted. A dialect answering {@code false} gets no row limit pushed at all
     * rather than a guessed alternative spelling: emitting {@code FETCH FIRST}/{@code ROWNUM} to a source
     * that wanted the other one produces a syntax error at scan time, while not pushing it only costs rows
     * over the wire.
     */
    default boolean supportsLimitClause() {
        return true;
    }
}
