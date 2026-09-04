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

import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * What the default dialect will and will not say, and how one gets chosen.
 *
 * <p>The refusals carry the weight here. ANSI is what an ADBC catalog gets whenever the source is not
 * recognized -- the common case -- so a literal it renders wrongly does not fail, it returns a different set
 * of rows from a source nobody has tested against.
 */
class AdbcDialectTest {

    private static final AdbcDialect ANSI = AdbcDialectRegistry.defaultDialect();

    private static ConnectorLiteral literal(String typeName, Object value) {
        return new ConnectorLiteral(ConnectorType.of(typeName), value);
    }

    private static AdbcTableHandle handle(String catalog, String schema, String table) {
        return new AdbcTableHandle(new AdbcNamespace(catalog, schema), table);
    }

    // ---------- identifiers ----------

    @Test
    void quotesIdentifiersAndDoublesAnEmbeddedQuote() {
        Assertions.assertEquals("\"orders\"", ANSI.quoteIdentifier("orders"));
        // Without doubling, a name containing a quote would close the identifier early and turn the rest of
        // the name into stray SQL -- the injection shape, reached through a column name from the source.
        Assertions.assertEquals("\"we\"\"ird\"", ANSI.quoteIdentifier("we\"ird"));
    }

    @Test
    void qualifiesWithTheSchemaWhenThereIsOne() {
        Assertions.assertEquals("\"public\".\"orders\"",
                ANSI.qualifiedTableName(handle("mydb", "public", "orders")));
    }

    @Test
    void qualifiesWithTheCatalogOnlyWhenThereIsNoSchema() {
        // SQLite's shape: catalog "main", empty schema.
        Assertions.assertEquals("\"main\".\"t1\"", ANSI.qualifiedTableName(handle("main", "", "t1")));
    }

    @Test
    void neverEmitsThreeParts() {
        // The uri pins one remote catalog, so repeating it adds nothing -- and some sources reject a
        // three-part name outright.
        String name = ANSI.qualifiedTableName(handle("MYDB", "PUBLIC", "ORDERS"));
        Assertions.assertEquals("\"PUBLIC\".\"ORDERS\"", name);
    }

    @Test
    void neverHasToNameAnUnqualifiedTable() {
        // There is no "neither level" case to qualify: a namespace with neither is refused when it is
        // built, because it has no Doris database name to be shown under either. So the dialect is never
        // asked to render a bare table name, and does not have to decide what that would mean.
        Assertions.assertThrows(DorisConnectorException.class, () -> handle("", "", "t1"));
    }

    // ---------- literals it renders ----------

    @Test
    void rendersNumbersBooleansAndCharacterData() {
        Assertions.assertEquals("42", ANSI.renderLiteral(literal("INT", 42L)));
        Assertions.assertEquals("-1.5", ANSI.renderLiteral(literal("DOUBLE", -1.5d)));
        Assertions.assertEquals("3.140", ANSI.renderLiteral(
                literal("DECIMAL64", new BigDecimal("3.140"))));
        Assertions.assertEquals("TRUE", ANSI.renderLiteral(literal("BOOLEAN", Boolean.TRUE)));
        Assertions.assertEquals("FALSE", ANSI.renderLiteral(literal("BOOLEAN", Boolean.FALSE)));
        Assertions.assertEquals("'abc'", ANSI.renderLiteral(literal("VARCHAR", "abc")));
    }

    @Test
    void doublesAnEmbeddedQuoteInAStringLiteral() {
        Assertions.assertEquals("'O''Brien'", ANSI.renderLiteral(literal("STRING", "O'Brien")));
    }

    @Test
    void rendersDatesAndTimestampsWithTheirTypeKeyword() {
        // Bare '2024-01-31' is read as character data by some sources and compared as text against a date
        // column, which orders differently. The keyword is what makes the comparison a date comparison.
        Assertions.assertEquals("DATE '2024-01-31'",
                ANSI.renderLiteral(literal("DATEV2", LocalDate.of(2024, 1, 31))));
        Assertions.assertEquals("TIMESTAMP '2024-01-31 12:30:05'",
                ANSI.renderLiteral(literal("DATETIMEV2",
                        LocalDateTime.of(2024, 1, 31, 12, 30, 5))));
    }

    @Test
    void keepsSubSecondPrecisionButNoFurtherThanMicroseconds() {
        Assertions.assertEquals("TIMESTAMP '2024-01-31 12:30:05.000123'",
                ANSI.renderLiteral(literal("DATETIMEV2",
                        LocalDateTime.of(2024, 1, 31, 12, 30, 5, 123_000))));
    }

    // ---------- literals it refuses ----------

    @Test
    void refusesANullLiteral() {
        Assertions.assertNull(ANSI.renderLiteral(ConnectorLiteral.ofNull(ConnectorType.of("INT"))));
    }

    @Test
    void refusesAZonedTimestampLiteral() {
        // Same Java value as the DATETIMEV2 case above and a different answer, because the TYPE changes
        // what the value means: the engine hands a TIMESTAMPTZ literal over already converted to UTC,
        // and TIMESTAMP '...' has no zone in it, so the source would compare its own local wall clock
        // against a UTC one. West of UTC that drops rows the query asked for -- rows a scan cannot get
        // back, since the source never sends them -- so the comparison is left to Doris.
        Assertions.assertNull(ANSI.renderLiteral(literal("TIMESTAMPTZ",
                LocalDateTime.of(2024, 1, 31, 12, 30, 5))));
        Assertions.assertEquals("TIMESTAMP '2024-01-31 12:30:05'",
                ANSI.renderLiteral(literal("DATETIMEV2",
                        LocalDateTime.of(2024, 1, 31, 12, 30, 5))));
    }

    @Test
    void refusesAStringValueWhoseTypeIsNotCharacterData() {
        // The engine's converter falls back to the text form for every type it has no branch for. Quoting
        // that would compare a 128-bit integer, an IP or a JSON document as text, which orders and matches
        // differently from how the source stores it.
        Assertions.assertNull(ANSI.renderLiteral(literal("LARGEINT", "170141183460469231731687303715884105727")));
        Assertions.assertNull(ANSI.renderLiteral(literal("IPV4", "10.0.0.1")));
        Assertions.assertNull(ANSI.renderLiteral(literal("JSON", "{\"a\":1}")));
    }

    @Test
    void refusesNonFiniteFloatingPointValues() {
        Assertions.assertNull(ANSI.renderLiteral(literal("DOUBLE", Double.NaN)));
        Assertions.assertNull(ANSI.renderLiteral(literal("DOUBLE", Double.POSITIVE_INFINITY)));
    }

    @Test
    void refusesAStringContainingANulCharacter() {
        // Standard SQL cannot escape it inside a quoted string and sources disagree on whether it ends the
        // value, so sending it risks a silently truncated comparison.
        Assertions.assertNull(ANSI.renderLiteral(literal("STRING", "a\0b")));
    }

    // ---------- selection ----------

    @Test
    void selectsAnsiWhenNothingIdentifiesTheSource() {
        AdbcDialectSelector selector = new AdbcDialectSelector("");
        // The probe cannot run without a client; supplying one that fails stands in for a driver that
        // refuses getInfo, which must land on ANSI rather than fail the query.
        Assertions.assertSame(ANSI, selector.select(() -> {
            throw new DorisConnectorException("driver unavailable");
        }));
    }

    @Test
    void selectsTheConfiguredDialectWithoutAskingTheSource() {
        AdbcDialect fake = new FakeDialect("fake-for-selection");
        AdbcDialectRegistry.register(fake);
        AdbcDialectSelector selector = new AdbcDialectSelector("fake-for-selection");

        Assertions.assertSame(fake, selector.select(() -> {
            // A probe here would make the property advisory, and would also cost a remote call on a
            // question the user already answered.
            throw new AssertionError("the source must not be probed when sql_dialect is set");
        }));
    }

    @Test
    void matchesTheConfiguredNameCaseInsensitively() {
        AdbcDialectSelector selector = new AdbcDialectSelector("ANSI");
        Assertions.assertSame(ANSI, selector.select(() -> {
            throw new AssertionError("not probed");
        }));
    }

    @Test
    void rejectsAnUnknownConfiguredDialectAndNamesTheRegisteredOnes() {
        AdbcDialectSelector selector = new AdbcDialectSelector("postgres-typo");

        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                selector::validateConfiguredName);
        Assertions.assertTrue(failure.getMessage().contains("postgres-typo"), failure.getMessage());
        Assertions.assertTrue(failure.getMessage().contains(AnsiDialect.NAME), failure.getMessage());
    }

    @Test
    void findsARegisteredDialectByTheVendorNameADriverReports() {
        AdbcDialectRegistry.register(new FakeDialect("fake-vendor-match"));
        Assertions.assertEquals("fake-vendor-match",
                AdbcDialectRegistry.forVendor("Fake-Vendor-Match").map(AdbcDialect::name).orElse(null));
        Assertions.assertFalse(AdbcDialectRegistry.forVendor("Nothing-Claims-This").isPresent());
        Assertions.assertFalse(AdbcDialectRegistry.forVendor("").isPresent());
        Assertions.assertFalse(AdbcDialectRegistry.forVendor(null).isPresent());
    }

    /** A dialect defined outside the shipped set, to show that being one is all it takes. */
    private static final class FakeDialect implements AdbcDialect {

        private final String name;

        FakeDialect(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String quoteIdentifier(String identifier) {
            return "`" + identifier + "`";
        }

        @Override
        public String qualifiedTableName(AdbcTableHandle handle) {
            return quoteIdentifier(handle.getRemoteTable());
        }

        @Override
        public String renderLiteral(ConnectorLiteral value) {
            return String.valueOf(value.getValue());
        }
    }
}
