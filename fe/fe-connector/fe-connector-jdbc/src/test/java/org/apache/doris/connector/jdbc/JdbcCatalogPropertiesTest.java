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

package org.apache.doris.connector.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Unit tests for {@link JdbcCatalogProperties}.
 *
 * <p>The split this file mostly exists to defend: {@code of()} enforces what a <i>stored</i> catalog
 * must satisfy, {@code checkCreateTimeOnlyRules()} enforces what a <i>statement</i> must satisfy. This
 * connector validated far more at CREATE than any reader ever did, so moving those rules into of() —
 * which runs on every rebuild, including on an FE replaying the edit log — would make catalogs that
 * work today unbuildable tomorrow. Each pair of cases below pins one rule on the correct side of that
 * line.
 */
class JdbcCatalogPropertiesTest {

    private static Map<String, String> minimal() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put(JdbcCatalogProperties.JDBC_URL, "jdbc:mysql://host:3306/db");
        m.put(JdbcCatalogProperties.DRIVER_URL, "mysql-connector-j.jar");
        m.put(JdbcCatalogProperties.DRIVER_CLASS, "com.mysql.cj.jdbc.Driver");
        return m;
    }

    private static Map<String, String> with(String key, String value) {
        Map<String, String> m = minimal();
        m.put(key, value);
        return m;
    }

    @Test
    void bindsEveryKeyAndDefaults() {
        JdbcCatalogProperties p = JdbcCatalogProperties.of(minimal());
        Assertions.assertEquals("jdbc:mysql://host:3306/db", p.getJdbcUrl());
        Assertions.assertEquals("mysql-connector-j.jar", p.getDriverUrl());
        Assertions.assertEquals("com.mysql.cj.jdbc.Driver", p.getDriverClass());
        Assertions.assertEquals("", p.getUser());
        Assertions.assertEquals("", p.getPassword());
        Assertions.assertEquals("", p.getDriverChecksum());
        Assertions.assertEquals(1, p.getConnectionPoolMinSize());
        Assertions.assertEquals(30, p.getConnectionPoolMaxSize());
        Assertions.assertEquals(5000, p.getConnectionPoolMaxWaitTime());
        Assertions.assertEquals(1800000, p.getConnectionPoolMaxLifeTime());
        Assertions.assertFalse(p.isConnectionPoolKeepAlive());
        Assertions.assertFalse(p.isOnlySpecifiedDatabase());
        Assertions.assertEquals("", p.getIncludeDatabaseList());
        Assertions.assertEquals("", p.getExcludeDatabaseList());
        Assertions.assertFalse(p.isEnableMappingVarbinary());
        Assertions.assertFalse(p.isEnableMappingTimestampTz());
        Assertions.assertEquals("", p.getFunctionRules());
        Assertions.assertFalse(p.isLowerCaseMetaNames());
        Assertions.assertEquals("", p.getMetaNamesMapping());

        Map<String, String> m = minimal();
        m.put(JdbcCatalogProperties.USER, "u");
        m.put(JdbcCatalogProperties.PASSWORD, "secret-p");
        m.put(JdbcCatalogProperties.CONNECTION_POOL_MIN_SIZE, "2");
        m.put(JdbcCatalogProperties.CONNECTION_POOL_MAX_SIZE, "40");
        m.put(JdbcCatalogProperties.CONNECTION_POOL_MAX_WAIT_TIME, "6000");
        m.put(JdbcCatalogProperties.CONNECTION_POOL_MAX_LIFE_TIME, "200000");
        m.put(JdbcCatalogProperties.CONNECTION_POOL_KEEP_ALIVE, "true");
        m.put(JdbcCatalogProperties.ONLY_SPECIFIED_DATABASE, "true");
        m.put(JdbcCatalogProperties.LOWER_CASE_META_NAMES, "true");
        JdbcCatalogProperties set = JdbcCatalogProperties.of(m);
        Assertions.assertEquals("u", set.getUser());
        Assertions.assertEquals("secret-p", set.getPassword());
        Assertions.assertEquals(2, set.getConnectionPoolMinSize());
        Assertions.assertEquals(40, set.getConnectionPoolMaxSize());
        Assertions.assertEquals(6000, set.getConnectionPoolMaxWaitTime());
        Assertions.assertEquals(200000, set.getConnectionPoolMaxLifeTime());
        Assertions.assertTrue(set.isConnectionPoolKeepAlive());
        Assertions.assertTrue(set.isOnlySpecifiedDatabase());
        Assertions.assertTrue(set.isLowerCaseMetaNames());
    }

    @Test
    void missingJdbcUrlFailsNamingTheKey() {
        Map<String, String> m = minimal();
        m.remove(JdbcCatalogProperties.JDBC_URL);
        Assertions.assertEquals("Required property '" + JdbcCatalogProperties.JDBC_URL + "' is missing",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> JdbcCatalogProperties.of(m)).getMessage());
    }

    // --- the of() / create-time split ---

    // The one numeric change of this migration: a malformed pool size used to be swallowed by the
    // runtime readers, which fell back to the default and left the catalog usable. It is now a bound
    // int, so the catalog cannot be built until ALTER CATALOG overwrites the value. CREATE and ALTER
    // already refused it, so only a catalog stored by an older version can be affected.
    @Test
    void malformedPoolSizeIsRefusedWhenBuilding() {
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> JdbcCatalogProperties.of(
                        with(JdbcCatalogProperties.CONNECTION_POOL_MAX_SIZE, "thirty")));
        Assertions.assertTrue(e.getMessage().contains(JdbcCatalogProperties.CONNECTION_POOL_MAX_SIZE),
                "the error must name the offending key, got: " + e.getMessage());
    }

    // The pool bounds are the other half of that story: no reader has ever enforced them, so a stored
    // catalog outside them runs today and must go on building. Only a statement is held to them.
    @Test
    void poolBoundsAreCheckedForAStatementButNotForAStoredCatalog() {
        Map<String, String> tooShortLifeTime =
                with(JdbcCatalogProperties.CONNECTION_POOL_MAX_LIFE_TIME, "1000");
        Assertions.assertDoesNotThrow(() -> JdbcCatalogProperties.of(tooShortLifeTime),
                "a stored catalog outside the bounds must still build");
        Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class,
                        () -> JdbcCatalogProperties.of(tooShortLifeTime).checkCreateTimeOnlyRules())
                .getMessage().contains("connection_pool_max_life_time"));

        Map<String, String> maxBelowMin = minimal();
        maxBelowMin.put(JdbcCatalogProperties.CONNECTION_POOL_MIN_SIZE, "10");
        maxBelowMin.put(JdbcCatalogProperties.CONNECTION_POOL_MAX_SIZE, "5");
        Assertions.assertDoesNotThrow(() -> JdbcCatalogProperties.of(maxBelowMin));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> JdbcCatalogProperties.of(maxBelowMin).checkCreateTimeOnlyRules());
    }

    // Same shape for the spelled-out booleans: the binder reads anything that is not "true" as false,
    // as every reader here always has, so a stored catalog spelling one "yes" keeps working; a
    // statement spelling it "yes" is refused, because that is how a typo silently disables an option.
    @Test
    void booleanSpellingIsCheckedForAStatementButNotForAStoredCatalog() {
        Map<String, String> m = with(JdbcCatalogProperties.ONLY_SPECIFIED_DATABASE, "yes");
        Assertions.assertFalse(JdbcCatalogProperties.of(m).isOnlySpecifiedDatabase());
        Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class,
                        () -> JdbcCatalogProperties.of(m).checkCreateTimeOnlyRules())
                .getMessage().contains("must be true or false"));
    }

    @Test
    void createTimeRulesRejectTheDeprecatedLowerCaseTableNames() {
        Map<String, String> shortForm = with(JdbcCatalogProperties.LOWER_CASE_TABLE_NAMES, "1");
        Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class,
                        () -> JdbcCatalogProperties.of(shortForm).checkCreateTimeOnlyRules())
                .getMessage().contains("lower_case_meta_names"));

        // The prefixed spelling was rejected too, and still is: it is stripped on the way in.
        Map<String, String> prefixed = with(
                JdbcCatalogProperties.JDBC_PROPERTIES_PREFIX + JdbcCatalogProperties.LOWER_CASE_TABLE_NAMES, "1");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> JdbcCatalogProperties.of(prefixed).checkCreateTimeOnlyRules());
    }

    @Test
    void createTimeRulesRejectADatabaseListWithoutOnlySpecifiedDatabase() {
        Map<String, String> m = with(JdbcCatalogProperties.INCLUDE_DATABASE_LIST, "db1");
        Assertions.assertDoesNotThrow(() -> JdbcCatalogProperties.of(m));
        Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class,
                        () -> JdbcCatalogProperties.of(m).checkCreateTimeOnlyRules())
                .getMessage().contains("cannot be set when only_specified_database is false"));

        m.put(JdbcCatalogProperties.ONLY_SPECIFIED_DATABASE, "true");
        Assertions.assertDoesNotThrow(
                () -> JdbcCatalogProperties.of(m).checkCreateTimeOnlyRules());
    }

    @Test
    void createTimeRulesRequireTheDriverAndRejectAnUnusableNameMapping() {
        Map<String, String> noDriver = minimal();
        noDriver.remove(JdbcCatalogProperties.DRIVER_CLASS);
        Assertions.assertDoesNotThrow(() -> JdbcCatalogProperties.of(noDriver));
        Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class,
                        () -> JdbcCatalogProperties.of(noDriver).checkCreateTimeOnlyRules())
                .getMessage().contains(JdbcCatalogProperties.DRIVER_CLASS));

        // An unclosed array is the shape JdbcIdentifierMapper rejects; it is checked for a statement,
        // and a catalog already storing one still builds (its own reader raises it at query time).
        Map<String, String> badMapping = with(JdbcCatalogProperties.META_NAMES_MAPPING,
                "{\"databases\":[{\"remoteDatabase\":\"a\",\"mapping\":\"b\"}");
        Assertions.assertDoesNotThrow(() -> JdbcCatalogProperties.of(badMapping));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> JdbcCatalogProperties.of(badMapping).checkCreateTimeOnlyRules());
    }

    // --- the jdbc. prefix ---

    @Test
    void theJdbcPrefixIsAcceptedAndTheShortSpellingWins() {
        Map<String, String> prefixed = new LinkedHashMap<>();
        prefixed.put(JdbcCatalogProperties.JDBC_PROPERTIES_PREFIX + JdbcCatalogProperties.JDBC_URL,
                "jdbc:mysql://prefixed:3306/db");
        prefixed.put(JdbcCatalogProperties.JDBC_PROPERTIES_PREFIX + JdbcCatalogProperties.USER, "u");
        JdbcCatalogProperties p = JdbcCatalogProperties.of(prefixed);
        Assertions.assertEquals("jdbc:mysql://prefixed:3306/db", p.getJdbcUrl());
        Assertions.assertEquals("u", p.getUser());

        Map<String, String> both = minimal();
        both.put(JdbcCatalogProperties.JDBC_PROPERTIES_PREFIX + JdbcCatalogProperties.JDBC_URL,
                "jdbc:mysql://prefixed:3306/db");
        Assertions.assertEquals("jdbc:mysql://host:3306/db",
                JdbcCatalogProperties.of(both).getJdbcUrl(),
                "the short spelling won in both places that used to resolve these keys");
    }

    // --- URL normalization is the caller's, not this class's ---

    // The normalization depends on a per-FE setting, so a holder of per-catalog properties must not do
    // it itself. The validation doors get the URL as written; the connector passes the function.
    @Test
    void theUrlIsNormalizedOnlyByTheCallerThatSuppliesTheFunction() {
        Assertions.assertEquals("jdbc:mysql://host:3306/db",
                JdbcCatalogProperties.of(minimal()).getJdbcUrl());
        Assertions.assertEquals("JDBC:MYSQL://HOST:3306/DB",
                JdbcCatalogProperties.of(minimal(), url -> url.toUpperCase(Locale.ROOT)).getJdbcUrl());
    }

    // --- the three rules an of() has to obey ---

    // Guards DESIGN D3(2): the map also carries engine keys and storage keys, and ALTER CATALOG merges
    // properties -- it can overwrite a key but never remove one, so refusing an unrecognized name would
    // leave a catalog that no statement could repair.
    @Test
    void unknownKeysAreTolerated() {
        Map<String, String> m = minimal();
        m.put("some_future_key", "x");
        m.put(JdbcCatalogProperties.JDBC_PROPERTIES_PREFIX + "some_future_key", "y");
        m.put("type", "jdbc");
        Assertions.assertDoesNotThrow(() -> JdbcCatalogProperties.of(m).checkCreateTimeOnlyRules());
    }

    // Guards DESIGN D3(1): of() runs at CREATE, again on the merged candidate at ALTER, and once more
    // every time the connector is rebuilt, so it must be a pure function of its input.
    @Test
    void ofIsPureAndRepeatable() {
        Map<String, String> m = minimal();
        m.put(JdbcCatalogProperties.JDBC_PROPERTIES_PREFIX + JdbcCatalogProperties.USER, "u");
        Map<String, String> before = new LinkedHashMap<>(m);

        JdbcCatalogProperties first = JdbcCatalogProperties.of(m);
        JdbcCatalogProperties second = JdbcCatalogProperties.of(m);

        Assertions.assertEquals(before, m, "of() must not mutate the caller's map");
        Assertions.assertEquals(first.getJdbcUrl(), second.getJdbcUrl());
        Assertions.assertEquals(first.getUser(), second.getUser());
        Assertions.assertEquals(first.getRaw(), second.getRaw());
    }

    // Guards DESIGN D5: toString() is what a log line renders, and the raw map behind this object holds
    // the password.
    @Test
    void toStringMasksThePassword() {
        String rendered =
                JdbcCatalogProperties.of(with(JdbcCatalogProperties.PASSWORD, "secret-p")).toString();
        Assertions.assertFalse(rendered.contains("secret-p"), "got: " + rendered);
        Assertions.assertTrue(rendered.contains("password=***"), "got: " + rendered);
    }
}
