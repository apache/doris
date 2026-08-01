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

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * The fallback dialect: standard SQL, and nothing a particular source added on top.
 *
 * <p>It is what an ADBC catalog gets when the driver's vendor is unknown or unrecognized, which is the
 * common case -- any driver implementing the ADBC C ABI is a valid source. So this class is not a lowest
 * common denominator chosen for tidiness; it is what most catalogs will actually run, and every choice in it
 * is made for a source nobody has identified yet.
 *
 * <p>That is why {@link #renderLiteral} refuses more than it accepts. A literal it cannot render only costs
 * a predicate that stays in Doris, whereas one it renders WRONGLY -- a type whose text spelling the source
 * reads differently -- silently changes which rows come back.
 */
public class AnsiDialect implements AdbcDialect {

    public static final String NAME = "ansi";

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /**
     * Fractional seconds are emitted only when present, and never beyond microseconds: Doris stores at most
     * microsecond precision, and a source that parses fewer digits would round rather than reject, moving
     * a boundary comparison onto the wrong side.
     */
    private static final DateTimeFormatter TIMESTAMP_FORMAT = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
            .optionalEnd()
            .toFormatter();

    /**
     * The Doris types whose literal value arrives as a {@link String} AND that a source will read back as
     * character data. The gate matters because the engine's expression converter falls back to
     * {@code getStringValue()} for every type it has no branch for -- LARGEINT, IPV4, JSON -- so "the value
     * is a String" on its own would quote a 128-bit integer and compare it as text.
     */
    private static final Set<String> CHARACTER_TYPE_NAMES =
            new HashSet<>(Arrays.asList("CHAR", "VARCHAR", "STRING"));

    /** {@code PrimitiveType.TIMESTAMPTZ}'s name, as {@code ConnectorType} spells it. */
    private static final String TIMESTAMPTZ_TYPE_NAME = "TIMESTAMPTZ";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String quoteIdentifier(String name) {
        return '"' + name.replace("\"", "\"\"") + '"';
    }

    /**
     * Two parts at most, and never the catalog alongside the schema.
     *
     * <p>The {@code uri} property is required to pin one remote catalog, so every table reachable through
     * this Doris catalog shares it and naming it again adds nothing -- while a three-part name is outright
     * rejected by some sources. A source that has a catalog but no schema (SQLite reports {@code main} and
     * an empty schema) uses the catalog as the one qualifier.
     */
    @Override
    public String qualifiedTableName(AdbcTableHandle handle) {
        StringBuilder sb = new StringBuilder();
        if (!handle.getRemoteDbSchema().isEmpty()) {
            sb.append(quoteIdentifier(handle.getRemoteDbSchema())).append('.');
        } else if (!handle.getRemoteCatalog().isEmpty()) {
            sb.append(quoteIdentifier(handle.getRemoteCatalog())).append('.');
        }
        return sb.append(quoteIdentifier(handle.getRemoteTable())).toString();
    }

    @Override
    public String renderLiteral(ConnectorLiteral literal) {
        Object value = literal.getValue();
        if (value == null) {
            // A NULL literal is renderable in principle, and both sides would agree that a comparison
            // against it matches nothing. It is refused anyway: the agreement rests on the source treating
            // three-valued logic exactly as Doris does, which is not a property an unknown source has been
            // shown to have, and the planner folds these away long before they reach a scan.
            return null;
        }
        if (value instanceof Boolean) {
            return ((Boolean) value) ? "TRUE" : "FALSE";
        }
        if (value instanceof Long || value instanceof Integer || value instanceof Short
                || value instanceof Byte || value instanceof BigDecimal) {
            return value.toString();
        }
        if (value instanceof Double || value instanceof Float) {
            double d = ((Number) value).doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d)) {
                // No portable spelling; the source would either reject it or read it as an identifier.
                return null;
            }
            return value.toString();
        }
        if (value instanceof LocalDate) {
            return "DATE '" + DATE_FORMAT.format((LocalDate) value) + "'";
        }
        if (value instanceof LocalDateTime) {
            if (TIMESTAMPTZ_TYPE_NAME.equals(literal.getType().getTypeName())) {
                // A zoned literal reaches here already converted to UTC, and standard SQL's
                // TIMESTAMP 'yyyy-MM-dd HH:mm:ss' carries no zone -- so the source would read the UTC
                // wall clock as ITS OWN local time and compare that against its column. On a source
                // east of UTC that only widens the match and Doris re-filters what comes back; on one
                // west of UTC it NARROWS it, and the rows the source drops are rows the query wanted.
                // There is no portable spelling that says which instant is meant, so the comparison
                // stays with Doris.
                return null;
            }
            return "TIMESTAMP '" + TIMESTAMP_FORMAT.format((LocalDateTime) value) + "'";
        }
        if (value instanceof String && CHARACTER_TYPE_NAMES.contains(literal.getType().getTypeName())) {
            return renderStringLiteral((String) value);
        }
        return null;
    }

    private static String renderStringLiteral(String value) {
        if (value.indexOf('\0') >= 0) {
            // Standard SQL has no escape for it inside a quoted string, and sources disagree on whether it
            // terminates the value. Leave the predicate to Doris rather than send a string that may be
            // silently truncated at the source.
            return null;
        }
        return '\'' + value.replace("'", "''") + '\'';
    }
}
