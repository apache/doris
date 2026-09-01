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
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

/**
 * The dialect a Doris source gets, written from a real failure.
 *
 * <p>The first scan against another Doris over Flight SQL sent {@code SELECT "id" FROM "db"."t1"} and the
 * source answered {@code no viable alternative at input 'FROM "db"'}. Doris reads a double-quoted name as a
 * string literal, so ANSI quoting there is not merely unidiomatic: the statement does not parse at all, and
 * the connector cannot read the one source phase one exists to replace.
 *
 * <p>These tests are written against the registry rather than the class, because being reachable is the
 * half that was broken: an unclaimed vendor name falls back to ANSI silently.
 */
class DorisDialectTest {

    private static final String NAME = "doris";

    private static AdbcDialect doris() {
        return AdbcDialectRegistry.require(NAME);
    }

    private static AdbcTableHandle handle(String catalog, String schema, String table) {
        return new AdbcTableHandle(new AdbcNamespace(catalog, schema), table);
    }

    private static ConnectorLiteral literal(String typeName, Object value) {
        return new ConnectorLiteral(ConnectorType.of(typeName), value);
    }

    // ---------- being chosen ----------

    @Test
    void isClaimedByTheVendorNameDorisActuallyReports() {
        // Measured, not assumed: Doris answers getInfo(VENDOR_NAME) with "DorisFE", from
        // SqlInfoBuilder.withFlightSqlServerName in DorisFlightSqlProducer. Matching only the dialect's own
        // name -- the default -- would leave "DorisFE" unclaimed, which is exactly how the ANSI fallback
        // produced unparseable SQL.
        Assertions.assertEquals(NAME,
                AdbcDialectRegistry.forVendor("DorisFE").map(AdbcDialect::name).orElse(null));
        Assertions.assertEquals(NAME,
                AdbcDialectRegistry.forVendor("Doris").map(AdbcDialect::name).orElse(null));
    }

    @Test
    void doesNotClaimSourcesItWasNeverVerifiedAgainst() {
        // Backticks are the MySQL family's spelling, but only Doris has been run against. Claiming a vendor
        // on family resemblance would hand unverified sources a default they never asked for; they can still
        // ask for this dialect by name.
        Assertions.assertFalse(AdbcDialectRegistry.forVendor("SQLite").isPresent());
        Assertions.assertFalse(AdbcDialectRegistry.forVendor("MySQL").isPresent());
    }

    // ---------- what it spells differently ----------

    @Test
    void quotesIdentifiersWithBackticks() {
        Assertions.assertEquals("`id`", doris().quoteIdentifier("id"));
    }

    @Test
    void qualifiesTheTableWithBackticksToo() {
        // The exact token the parser choked on in the failing run.
        Assertions.assertEquals("`test_db`.`t1`",
                doris().qualifiedTableName(handle("internal", "test_db", "t1")));
    }

    @Test
    void doublesAnEmbeddedBacktick() {
        // Without doubling, a backtick inside a name closes the identifier early and turns the rest into
        // stray SQL -- the injection shape, reached through a column name coming from the source.
        Assertions.assertEquals("`we``ird`", doris().quoteIdentifier("we`ird"));
    }

    // ---------- what it must not change ----------

    @Test
    void rendersLiteralsExactlyAsAnsiDoes() {
        // Only the quoting character differs. A literal rendered differently here would not fail loudly like
        // a syntax error does; it would return a different set of rows.
        AdbcDialect ansi = AdbcDialectRegistry.defaultDialect();
        Assertions.assertEquals(ansi.renderLiteral(literal("STRING", "O'Brien")),
                doris().renderLiteral(literal("STRING", "O'Brien")));
        Assertions.assertEquals(ansi.renderLiteral(literal("DATEV2", LocalDate.of(2024, 1, 31))),
                doris().renderLiteral(literal("DATEV2", LocalDate.of(2024, 1, 31))));
        Assertions.assertEquals(ansi.renderLiteral(literal("INT", 42L)),
                doris().renderLiteral(literal("INT", 42L)));
        // Including the refusals: a type whose text spelling the source reads differently stays in Doris.
        Assertions.assertNull(doris().renderLiteral(literal("LARGEINT", "170141183460469231731687303715884105727")));
    }
}
