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

package org.apache.doris.connector.es;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Unit tests for {@link EsCatalogProperties}.
 *
 * <p>The bulk of this file is the compatibility matrix. This connector's keys reached Doris first as
 * "Doris On ES" table properties, so an existing catalog may spell any of them with an
 * {@code elasticsearch.} prefix and four of them by an older name — and for two of those four the
 * older name wins over the current one while for the other two it loses. That asymmetry is not a
 * design, it is what the code has always done, and a catalog created years ago depends on it. Each
 * cell below pins one spelling, so a future tidy-up of the alias order fails here rather than silently
 * changing which value a stored catalog resolves to.
 */
class EsCatalogPropertiesTest {

    private static Map<String, String> minimal() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put(EsCatalogProperties.HOSTS, "http://es-1:9200,http://es-2:9200");
        return m;
    }

    private static EsCatalogProperties with(String key, String value) {
        Map<String, String> m = minimal();
        m.put(key, value);
        return EsCatalogProperties.of(m);
    }

    @Test
    void bindsEveryKeyAndDefaults() {
        EsCatalogProperties p = EsCatalogProperties.of(minimal());
        Assertions.assertEquals("", p.getUser());
        Assertions.assertEquals("", p.getPassword());
        Assertions.assertFalse(p.isHttpSslEnabled());
        Assertions.assertTrue(p.isDocValuesMode());
        Assertions.assertTrue(p.isKeywordSniff());
        Assertions.assertTrue(p.isNodesDiscovery());
        Assertions.assertFalse(p.isMappingEsId());
        Assertions.assertTrue(p.isLikePushDown());
        Assertions.assertFalse(p.isIncludeHiddenIndex());
        Assertions.assertEquals(20, p.getMaxDocValueFields());

        Map<String, String> m = minimal();
        m.put(EsCatalogProperties.USER, "u");
        m.put(EsCatalogProperties.PASSWORD, "secret-p");
        m.put(EsCatalogProperties.DOC_VALUE_SCAN, "false");
        m.put(EsCatalogProperties.KEYWORD_SNIFF, "false");
        m.put(EsCatalogProperties.NODES_DISCOVERY, "false");
        m.put(EsCatalogProperties.MAPPING_ES_ID, "true");
        m.put(EsCatalogProperties.LIKE_PUSH_DOWN, "false");
        m.put(EsCatalogProperties.INCLUDE_HIDDEN_INDEX, "true");
        m.put(EsCatalogProperties.MAPPING_TYPE, "_doc");
        m.put(EsCatalogProperties.MAX_DOCVALUE_FIELDS, "5");
        EsCatalogProperties set = EsCatalogProperties.of(m);
        Assertions.assertEquals("u", set.getUser());
        Assertions.assertEquals("secret-p", set.getPassword());
        Assertions.assertFalse(set.isDocValuesMode());
        Assertions.assertFalse(set.isKeywordSniff());
        Assertions.assertFalse(set.isNodesDiscovery());
        Assertions.assertTrue(set.isMappingEsId());
        Assertions.assertFalse(set.isLikePushDown());
        Assertions.assertTrue(set.isIncludeHiddenIndex());
        Assertions.assertEquals("_doc", set.getMappingType());
        Assertions.assertEquals(5, set.getMaxDocValueFields());
    }

    // mapping_type absent is not "the default value", it selects the type-less (ES 7+) mapping shape:
    // EsMappingUtils.getRootSchema branches on null, and EsScanRange leaves the type out of the BE
    // payload entirely. An empty-string default would silently take a catalog down the typed branch.
    @Test
    void mappingTypeIsNullWhenAbsent() {
        Assertions.assertNull(EsCatalogProperties.of(minimal()).getMappingType());
    }

    @Test
    void missingHostsFailsNamingTheKey() {
        Map<String, String> m = new LinkedHashMap<>();
        Assertions.assertEquals("Required property '" + EsCatalogProperties.HOSTS + "' is missing",
                Assertions.assertThrows(IllegalArgumentException.class,
                        () -> EsCatalogProperties.of(m)).getMessage());

        Map<String, String> blank = minimal();
        blank.put(EsCatalogProperties.HOSTS, "   ");
        Assertions.assertThrows(IllegalArgumentException.class, () -> EsCatalogProperties.of(blank));
    }

    // --- compatibility matrix: the elasticsearch. prefix ---

    @Test
    void everyKeyIsAlsoAcceptedWithTheLegacyPrefix() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put(EsCatalogProperties.ES_PROPERTIES_PREFIX + EsCatalogProperties.HOSTS, "http://es:9200");
        m.put(EsCatalogProperties.ES_PROPERTIES_PREFIX + EsCatalogProperties.USER, "u");
        m.put(EsCatalogProperties.ES_PROPERTIES_PREFIX + EsCatalogProperties.PASSWORD, "p");
        m.put(EsCatalogProperties.ES_PROPERTIES_PREFIX + EsCatalogProperties.HTTP_SSL_ENABLED, "true");
        m.put(EsCatalogProperties.ES_PROPERTIES_PREFIX + EsCatalogProperties.DOC_VALUE_SCAN, "false");
        m.put(EsCatalogProperties.ES_PROPERTIES_PREFIX + EsCatalogProperties.KEYWORD_SNIFF, "false");
        m.put(EsCatalogProperties.ES_PROPERTIES_PREFIX + EsCatalogProperties.NODES_DISCOVERY, "false");
        m.put(EsCatalogProperties.ES_PROPERTIES_PREFIX + EsCatalogProperties.MAPPING_ES_ID, "true");
        m.put(EsCatalogProperties.ES_PROPERTIES_PREFIX + EsCatalogProperties.LIKE_PUSH_DOWN, "false");
        m.put(EsCatalogProperties.ES_PROPERTIES_PREFIX + EsCatalogProperties.INCLUDE_HIDDEN_INDEX, "true");
        m.put(EsCatalogProperties.ES_PROPERTIES_PREFIX + EsCatalogProperties.MAPPING_TYPE, "_doc");
        m.put(EsCatalogProperties.ES_PROPERTIES_PREFIX + EsCatalogProperties.MAX_DOCVALUE_FIELDS, "7");

        EsCatalogProperties p = EsCatalogProperties.of(m);
        Assertions.assertEquals("http://es:9200", p.getHosts());
        Assertions.assertEquals("u", p.getUser());
        Assertions.assertEquals("p", p.getPassword());
        Assertions.assertTrue(p.isHttpSslEnabled());
        Assertions.assertFalse(p.isDocValuesMode());
        Assertions.assertFalse(p.isKeywordSniff());
        Assertions.assertFalse(p.isNodesDiscovery());
        Assertions.assertTrue(p.isMappingEsId());
        Assertions.assertFalse(p.isLikePushDown());
        Assertions.assertTrue(p.isIncludeHiddenIndex());
        Assertions.assertEquals("_doc", p.getMappingType());
        Assertions.assertEquals(7, p.getMaxDocValueFields());
    }

    @Test
    void theLegacyPrefixStacksWithALegacyName() {
        Map<String, String> m = minimal();
        m.put(EsCatalogProperties.ES_PROPERTIES_PREFIX + "ssl", "true");
        m.put(EsCatalogProperties.ES_PROPERTIES_PREFIX + "username", "old-user");
        m.put(EsCatalogProperties.ES_PROPERTIES_PREFIX + "enable_docvalue_scan", "false");
        m.put(EsCatalogProperties.ES_PROPERTIES_PREFIX + "enable_keyword_sniff", "false");
        EsCatalogProperties p = EsCatalogProperties.of(m);
        Assertions.assertTrue(p.isHttpSslEnabled());
        Assertions.assertEquals("old-user", p.getUser());
        Assertions.assertFalse(p.isDocValuesMode());
        Assertions.assertFalse(p.isKeywordSniff());
    }

    @Test
    void theUnprefixedSpellingWinsWhenACatalogCarriesBoth() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put(EsCatalogProperties.ES_PROPERTIES_PREFIX + EsCatalogProperties.HOSTS, "http://prefixed:9200");
        m.put(EsCatalogProperties.HOSTS, "http://plain:9200");
        Assertions.assertEquals("http://plain:9200", EsCatalogProperties.of(m).getHosts());
    }

    // --- compatibility matrix: the four renamed keys ---
    //
    // Two of them win over the current name and two lose to it. Before this class that came from how
    // the rename was written -- "ssl" and "username" overwrote their target unconditionally, while
    // "enable_docvalue_scan" and "enable_keyword_sniff" only filled in an absent one -- and it is now
    // the order of the aliases on each field. A catalog that carries both spellings must keep
    // resolving to the same value it resolves to today.

    @Test
    void sslIsAnAliasOfHttpSslEnabledAndWinsOverIt() {
        Assertions.assertTrue(with("ssl", "true").isHttpSslEnabled(), "legacy name alone");
        Assertions.assertTrue(with(EsCatalogProperties.HTTP_SSL_ENABLED, "true").isHttpSslEnabled(),
                "current name alone");

        Map<String, String> both = minimal();
        both.put("ssl", "true");
        both.put(EsCatalogProperties.HTTP_SSL_ENABLED, "false");
        Assertions.assertTrue(EsCatalogProperties.of(both).isHttpSslEnabled(),
                "the legacy name has always overwritten the current one; it must go on winning");
    }

    @Test
    void usernameIsAnAliasOfUserAndWinsOverIt() {
        Assertions.assertEquals("old", with("username", "old").getUser(), "legacy name alone");
        Assertions.assertEquals("new", with(EsCatalogProperties.USER, "new").getUser(),
                "current name alone");

        Map<String, String> both = minimal();
        both.put("username", "old");
        both.put(EsCatalogProperties.USER, "new");
        Assertions.assertEquals("old", EsCatalogProperties.of(both).getUser(),
                "the legacy name has always overwritten the current one; it must go on winning");
    }

    @Test
    void enableDocvalueScanIsAnAliasOfDocValuesModeAndLosesToIt() {
        Assertions.assertFalse(with("enable_docvalue_scan", "false").isDocValuesMode(),
                "legacy name alone");
        Assertions.assertFalse(with(EsCatalogProperties.DOC_VALUE_SCAN, "false").isDocValuesMode(),
                "current name alone");

        Map<String, String> both = minimal();
        both.put("enable_docvalue_scan", "false");
        both.put(EsCatalogProperties.DOC_VALUE_SCAN, "true");
        Assertions.assertTrue(EsCatalogProperties.of(both).isDocValuesMode(),
                "the rename only ever filled in an absent current name; the current name must go on winning");
    }

    @Test
    void enableKeywordSniffIsAnAliasOfKeywordSniffAndLosesToIt() {
        Assertions.assertFalse(with("enable_keyword_sniff", "false").isKeywordSniff(),
                "legacy name alone");
        Assertions.assertFalse(with(EsCatalogProperties.KEYWORD_SNIFF, "false").isKeywordSniff(),
                "current name alone");

        Map<String, String> both = minimal();
        both.put("enable_keyword_sniff", "false");
        both.put(EsCatalogProperties.KEYWORD_SNIFF, "true");
        Assertions.assertTrue(EsCatalogProperties.of(both).isKeywordSniff(),
                "the rename only ever filled in an absent current name; the current name must go on winning");
    }

    // --- derived host lists ---

    @Test
    void hostsGetTheSchemeOfTheSslFlagUnlessTheyCarryOne() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put(EsCatalogProperties.HOSTS, " es-1:9200 , https://es-2:9200 ");
        Assertions.assertArrayEquals(new String[] {"http://es-1:9200", "https://es-2:9200"},
                EsCatalogProperties.of(m).getHostUrls());

        m.put(EsCatalogProperties.HTTP_SSL_ENABLED, "true");
        Assertions.assertArrayEquals(new String[] {"https://es-1:9200", "https://es-2:9200"},
                EsCatalogProperties.of(m).getHostUrls());
    }

    // The discovery seeds are the plain split, deliberately NOT the scheme-filled list: that is what
    // the node-discovery path has always been handed, and it parses host:port itself.
    @Test
    void seedsAreThePlainSplitAndBothListsAreCopies() {
        EsCatalogProperties p = EsCatalogProperties.of(minimal());
        Assertions.assertEquals(Arrays.asList("http://es-1:9200", "http://es-2:9200"),
                Arrays.asList(p.getSeeds()));

        p.getHostUrls()[0] = "mutated";
        p.getSeeds()[0] = "mutated";
        Assertions.assertEquals("http://es-1:9200", p.getHostUrls()[0]);
        Assertions.assertEquals("http://es-1:9200", p.getSeeds()[0]);
    }

    // --- the three rules an of() has to obey ---

    // Guards DESIGN D3(2): the map also carries engine keys and storage keys, and ALTER CATALOG merges
    // properties -- it can overwrite a key but never remove one, so refusing an unrecognized name would
    // leave a catalog that no statement could repair.
    @Test
    void unknownKeysAreTolerated() {
        Map<String, String> m = minimal();
        m.put("some_future_key", "x");
        m.put(EsCatalogProperties.ES_PROPERTIES_PREFIX + "some_future_key", "y");
        m.put("type", "es");
        Assertions.assertDoesNotThrow(() -> EsCatalogProperties.of(m));
    }

    // Guards DESIGN D3(1): of() runs at CREATE, again on the merged candidate at ALTER, and once more
    // every time the connector is rebuilt -- including on an FE replaying the edit log -- so it must be
    // a pure function of its input. The prefix strip makes the "does not mutate its input" half of that
    // worth checking: it is the one connector whose of() rewrites keys.
    @Test
    void ofIsPureAndRepeatable() {
        Map<String, String> m = minimal();
        m.put(EsCatalogProperties.ES_PROPERTIES_PREFIX + "ssl", "true");
        Map<String, String> before = new LinkedHashMap<>(m);

        EsCatalogProperties first = EsCatalogProperties.of(m);
        EsCatalogProperties second = EsCatalogProperties.of(m);

        Assertions.assertEquals(before, m, "of() must not mutate the caller's map");
        Assertions.assertEquals(first.getHosts(), second.getHosts());
        Assertions.assertEquals(first.isHttpSslEnabled(), second.isHttpSslEnabled());
        Assertions.assertArrayEquals(first.getHostUrls(), second.getHostUrls());
    }

    // Guards DESIGN D5: toString() is what a log line renders.
    @Test
    void toStringMasksThePassword() {
        String rendered = with(EsCatalogProperties.PASSWORD, "secret-p").toString();
        Assertions.assertFalse(rendered.contains("secret-p"), "got: " + rendered);
        Assertions.assertTrue(rendered.contains("password=***"), "got: " + rendered);
    }
}
