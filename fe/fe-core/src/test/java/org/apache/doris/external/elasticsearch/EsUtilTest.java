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

package org.apache.doris.external.elasticsearch;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.EsTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.ExceptionChecker;

import com.fasterxml.jackson.databind.node.ObjectNode;
import mockit.Expectations;
import mockit.Injectable;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Test for es util.
 **/
public class EsUtilTest extends EsTestCase {

    private List<Column> columns = new ArrayList<>();

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    /**
     * Init columns.
     **/
    @Before
    public void setUp() {
        Column k1 = new Column("k1", PrimitiveType.BIGINT);
        Column k2 = new Column("k2", PrimitiveType.VARCHAR);
        Column k3 = new Column("k3", PrimitiveType.VARCHAR);
        Column k4 = new Column("k4", PrimitiveType.VARCHAR);
        Column k5 = new Column("k5", PrimitiveType.VARCHAR);
        Column k6 = new Column("k6", PrimitiveType.DATE);
        columns.add(k1);
        columns.add(k2);
        columns.add(k3);
        columns.add(k4);
        columns.add(k5);
        columns.add(k6);
    }

    @Test
    public void testExtractFieldsNormal() throws Exception {

        // ES version < 7.0
        EsTable esTableBefore7X = fakeEsTable("fake", "test", "doc", columns);
        SearchContext searchContext = new SearchContext(esTableBefore7X);
        MappingPhase.resolveFields(searchContext, loadJsonFromFile("data/es/test_index_mapping.json"));
        Assertions.assertEquals("k3.keyword", searchContext.fetchFieldsContext().get("k3"));
        Assertions.assertEquals("k3.keyword", searchContext.docValueFieldsContext().get("k3"));
        Assertions.assertEquals("k1", searchContext.docValueFieldsContext().get("k1"));
        Assertions.assertEquals("k2", searchContext.docValueFieldsContext().get("k2"));

        // ES version >= 7.0
        EsTable esTableAfter7X = fakeEsTable("fake", "test", "_doc", columns);
        SearchContext searchContext1 = new SearchContext(esTableAfter7X);
        MappingPhase.resolveFields(searchContext1, loadJsonFromFile("data/es/test_index_mapping_after_7x.json"));
        Assertions.assertEquals("k3.keyword", searchContext1.fetchFieldsContext().get("k3"));
        Assertions.assertEquals("k3.keyword", searchContext1.docValueFieldsContext().get("k3"));
        Assertions.assertEquals("k1", searchContext1.docValueFieldsContext().get("k1"));
        Assertions.assertEquals("k2", searchContext1.docValueFieldsContext().get("k2"));
    }

    @Test
    public void testWorkFlow(@Injectable EsRestClient client) throws Exception {
        EsTable table = fakeEsTable("fake", "test", "doc", columns);
        SearchContext searchContext1 = new SearchContext(table);
        String jsonMapping = loadJsonFromFile("data/es/test_index_mapping.json");
        new Expectations(client) {
            {
                client.getMapping(anyString);
                minTimes = 0;
                result = jsonMapping;
            }
        };
        MappingPhase mappingPhase = new MappingPhase(client);
        ExceptionChecker.expectThrowsNoException(() -> mappingPhase.execute(searchContext1));
        ExceptionChecker.expectThrowsNoException(() -> mappingPhase.postProcess(searchContext1));
        Assertions.assertEquals("k3.keyword", searchContext1.fetchFieldsContext().get("k3"));
        Assertions.assertEquals("k3.keyword", searchContext1.docValueFieldsContext().get("k3"));
        Assertions.assertEquals("k1", searchContext1.docValueFieldsContext().get("k1"));
        Assertions.assertEquals("k2", searchContext1.docValueFieldsContext().get("k2"));
        Assertions.assertNull(searchContext1.docValueFieldsContext().get("k4"));
        Assertions.assertNull(searchContext1.docValueFieldsContext().get("k5"));

    }

    @Test
    public void testMultTextFields() throws Exception {
        EsTable esTableAfter7X = fakeEsTable("fake", "test", "_doc", columns);
        SearchContext searchContext = new SearchContext(esTableAfter7X);
        MappingPhase.resolveFields(searchContext,
                loadJsonFromFile("data/es/test_index_mapping_field_mult_analyzer.json"));
        Assertions.assertFalse(searchContext.docValueFieldsContext().containsKey("k3"));
    }

    @Test
    public void testEs6Mapping() throws IOException, URISyntaxException {
        ObjectNode testAliases = EsUtil.getMappingProps("test", loadJsonFromFile("data/es/es6_aliases_mapping.json"),
                "doc");
        Assertions.assertEquals(
                "{\"test1\":{\"type\":\"keyword\"},\"test2\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"test3\":{\"type\":\"double\"},\"test4\":{\"type\":\"date\"}}",
                testAliases.toString());

        ObjectNode testAliasesNoType = EsUtil.getMappingProps("test",
                loadJsonFromFile("data/es/es6_aliases_mapping.json"), null);
        Assertions.assertEquals(
                "{\"test1\":{\"type\":\"keyword\"},\"test2\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"test3\":{\"type\":\"double\"},\"test4\":{\"type\":\"date\"}}",
                testAliasesNoType.toString());

        ObjectNode testIndex = EsUtil.getMappingProps("test", loadJsonFromFile("data/es/es6_index_mapping.json"),
                "doc");
        Assertions.assertEquals(
                "{\"test1\":{\"type\":\"keyword\"},\"test2\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"test3\":{\"type\":\"double\"},\"test4\":{\"type\":\"date\"}}",
                testIndex.toString());

        ObjectNode testDynamicTemplates = EsUtil.getMappingProps("test",
                loadJsonFromFile("data/es/es6_dynamic_templates_mapping.json"), "doc");
        Assertions.assertEquals(
                "{\"test1\":{\"type\":\"keyword\"},\"test2\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"test3\":{\"type\":\"double\"},\"test4\":{\"type\":\"date\"}}",
                testDynamicTemplates.toString());

        expectedEx.expect(DorisEsException.class);
        expectedEx.expectMessage("Do not support index without explicit mapping.");
        EsUtil.getMappingProps("test", loadJsonFromFile("data/es/es6_only_dynamic_templates_mapping.json"), "doc");

        expectedEx.expect(DorisEsException.class);
        expectedEx.expectMessage("Do not support index without explicit mapping.");
        EsUtil.getMappingProps("test", loadJsonFromFile("data/es/es6_only_dynamic_templates_mapping.json"), null);
    }

    @Test
    public void testEs7Mapping() throws IOException, URISyntaxException {
        ObjectNode testAliases = EsUtil.getMappingProps("test", loadJsonFromFile("data/es/es7_aliases_mapping.json"),
                null);
        Assertions.assertEquals(
                "{\"test1\":{\"type\":\"keyword\"},\"test2\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"test3\":{\"type\":\"double\"},\"test4\":{\"type\":\"date\"}}",
                testAliases.toString());

        ObjectNode testAliasesErrorType = EsUtil.getMappingProps("test",
                loadJsonFromFile("data/es/es7_aliases_mapping.json"), "doc");
        Assertions.assertEquals(
                "{\"test1\":{\"type\":\"keyword\"},\"test2\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"test3\":{\"type\":\"double\"},\"test4\":{\"type\":\"date\"}}",
                testAliasesErrorType.toString());

        ObjectNode testIndex = EsUtil.getMappingProps("test", loadJsonFromFile("data/es/es7_index_mapping.json"),
                "doc");
        Assertions.assertEquals(
                "{\"test1\":{\"type\":\"keyword\"},\"test2\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"test3\":{\"type\":\"double\"},\"test4\":{\"type\":\"date\"}}",
                testIndex.toString());

        ObjectNode testDynamicTemplates = EsUtil.getMappingProps("test",
                loadJsonFromFile("data/es/es7_dynamic_templates_mapping.json"), null);
        Assertions.assertEquals(
                "{\"test1\":{\"type\":\"keyword\"},\"test2\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"test3\":{\"type\":\"double\"},\"test4\":{\"type\":\"date\"}}",
                testDynamicTemplates.toString());

        expectedEx.expect(DorisEsException.class);
        expectedEx.expectMessage("Do not support index without explicit mapping.");
        EsUtil.getMappingProps("test", loadJsonFromFile("data/es/es7_only_dynamic_templates_mapping.json"), null);
    }

    @Test
    public void testEs8Mapping() throws IOException, URISyntaxException {
        ObjectNode testAliases = EsUtil.getMappingProps("test", loadJsonFromFile("data/es/es8_aliases_mapping.json"),
                null);
        Assertions.assertEquals(
                "{\"test1\":{\"type\":\"keyword\"},\"test2\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"test3\":{\"type\":\"double\"},\"test4\":{\"type\":\"date\"}}",
                testAliases.toString());

        ObjectNode testAliasesErrorType = EsUtil.getMappingProps("test",
                loadJsonFromFile("data/es/es8_aliases_mapping.json"), "doc");
        Assertions.assertEquals(
                "{\"test1\":{\"type\":\"keyword\"},\"test2\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"test3\":{\"type\":\"double\"},\"test4\":{\"type\":\"date\"}}",
                testAliasesErrorType.toString());

        ObjectNode testIndex = EsUtil.getMappingProps("test", loadJsonFromFile("data/es/es8_index_mapping.json"),
                "doc");
        Assertions.assertEquals(
                "{\"test1\":{\"type\":\"keyword\"},\"test2\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"test3\":{\"type\":\"double\"},\"test4\":{\"type\":\"date\"}}",
                testIndex.toString());

        ObjectNode testDynamicTemplates = EsUtil.getMappingProps("test",
                loadJsonFromFile("data/es/es8_dynamic_templates_mapping.json"), "doc");
        Assertions.assertEquals(
                "{\"test1\":{\"type\":\"keyword\"},\"test2\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"test3\":{\"type\":\"double\"},\"test4\":{\"type\":\"date\"}}",
                testDynamicTemplates.toString());

        expectedEx.expect(DorisEsException.class);
        expectedEx.expectMessage("Do not support index without explicit mapping.");
        EsUtil.getMappingProps("test", loadJsonFromFile("data/es/es8_only_dynamic_templates_mapping.json"), "doc");
    }

    @Test
    public void testDateType() throws IOException, URISyntaxException {
        ObjectNode testDateFormat = EsUtil.getRootSchema(
                EsUtil.getMapping(loadJsonFromFile("data/es/test_date_format.json")), null, new ArrayList<>());
        List<Column> parseColumns = EsUtil.genColumnsFromEs("test_date_format", null, testDateFormat, false, new ArrayList<>());
        Assertions.assertEquals(8, parseColumns.size());
        for (Column column : parseColumns) {
            String name = column.getName();
            String type = column.getType().toSql();
            if ("test2".equals(name)) {
                Assertions.assertEquals("datetimev2(0)", type);
            }
            if ("test3".equals(name)) {
                Assertions.assertEquals("datetimev2(0)", type);
            }
            if ("test4".equals(name)) {
                Assertions.assertEquals("datev2", type);
            }
            if ("test5".equals(name)) {
                Assertions.assertEquals("datetimev2(0)", type);
            }
            if ("test6".equals(name)) {
                Assertions.assertEquals("datev2", type);
            }
            if ("test7".equals(name)) {
                Assertions.assertEquals("datetimev2(0)", type);
            }
            if ("test8".equals(name)) {
                Assertions.assertEquals("bigint(20)", type);
            }
        }
    }

    @Test
    public void testFieldAlias() throws IOException, URISyntaxException {
        ObjectNode testFieldAlias = EsUtil.getRootSchema(
                EsUtil.getMapping(loadJsonFromFile("data/es/test_field_alias.json")), null, new ArrayList<>());
        List<Column> parseColumns = EsUtil.genColumnsFromEs("test_field_alias", null, testFieldAlias, true, new ArrayList<>());
        Assertions.assertEquals("datetimev2(0)", parseColumns.get(2).getType().toSql());
        Assertions.assertEquals("text", parseColumns.get(4).getType().toSql());
    }

    @Test
    public void testComplexType() throws IOException, URISyntaxException {
        ObjectNode testFieldAlias = EsUtil.getRootSchema(
                EsUtil.getMapping(loadJsonFromFile("data/es/es6_dynamic_complex_type.json")), null, new ArrayList<>());
        List<Column> columns = EsUtil.genColumnsFromEs("test_complex_type", "complex_type", testFieldAlias, true,
                new ArrayList<>());
        Assertions.assertEquals(3, columns.size());
    }

    @Test
    public void testDynamicMapping() throws IOException, URISyntaxException {

        ObjectNode testAliases = EsUtil.getMappingProps("test", loadJsonFromFile("data/es/dynamic_mappings.json"),
                null);
        Assertions.assertEquals("{\"time\":{\"type\":\"long\"},\"type\":{\"type\":\"keyword\"},\"userId\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\"}}}}",
                testAliases.toString());

    }

    @Test
    public void testDefaultMapping() throws IOException, URISyntaxException {
        ObjectNode testAliases = EsUtil.getMappingProps("test", loadJsonFromFile("data/es/default_mappings.json"),
                null);
        Assertions.assertEquals("{\"@timestamp\":{\"type\":\"date\"},\"@version\":{\"type\":\"keyword\"},\"act\":{\"type\":\"text\",\"norms\":false,\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"app\":{\"type\":\"text\",\"norms\":false,\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"tags\":{\"type\":\"text\",\"norms\":false,\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"type\":{\"type\":\"text\",\"norms\":false,\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"uid\":{\"type\":\"text\",\"norms\":false,\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}}}",
                testAliases.toString());
    }

}
