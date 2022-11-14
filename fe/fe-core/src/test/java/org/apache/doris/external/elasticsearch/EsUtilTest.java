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

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LikePredicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.EsTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.ExceptionChecker;

import mockit.Expectations;
import mockit.Injectable;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test for es util.
 **/
public class EsUtilTest extends EsTestCase {

    private List<Column> columns = new ArrayList<>();

    private String jsonStr = "{\"settings\": {\n" + "               \"index\": {\n" + "                  \"bpack\": {\n"
            + "                     \"partition\": {\n" + "                        \"upperbound\": \"12\"\n"
            + "                     }\n" + "                  },\n" + "                  \"number_of_shards\": \"5\",\n"
            + "                  \"provided_name\": \"indexa\",\n"
            + "                  \"creation_date\": \"1539328532060\",\n"
            + "                  \"number_of_replicas\": \"1\",\n"
            + "                  \"uuid\": \"plNNtKiiQ9-n6NpNskFzhQ\",\n" + "                  \"version\": {\n"
            + "                     \"created\": \"5050099\"\n" + "                  }\n" + "               }\n"
            + "            }}";

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
    public void testGetJsonObject() {
        JSONObject json = (JSONObject) JSONValue.parse(jsonStr);
        JSONObject upperBoundSetting = EsUtil.getJsonObject(json, "settings.index.bpack.partition", 0);
        Assertions.assertTrue(upperBoundSetting.containsKey("upperbound"));
        Assertions.assertEquals("12", (String) upperBoundSetting.get("upperbound"));

        JSONObject unExistKey = EsUtil.getJsonObject(json, "set", 0);
        Assertions.assertNull(unExistKey);

        JSONObject singleKey = EsUtil.getJsonObject(json, "settings", 0);
        Assertions.assertTrue(singleKey.containsKey("index"));
    }

    @Test(expected = ClassCastException.class)
    public void testGetJsonObjectWithException() {
        JSONObject json = (JSONObject) JSONValue.parse(jsonStr);
        // only support json object could not get string value directly from this api, exception will be threw
        EsUtil.getJsonObject(json, "settings.index.bpack.partition.upperbound", 0);
    }

    @Test
    public void testBinaryPredicateConvertEsDsl() {
        SlotRef k1 = new SlotRef(null, "k1");
        IntLiteral intLiteral = new IntLiteral(3);
        Expr eqExpr = new BinaryPredicate(Operator.EQ, k1, intLiteral);
        Expr neExpr = new BinaryPredicate(Operator.NE, k1, intLiteral);
        Expr leExpr = new BinaryPredicate(Operator.LE, k1, intLiteral);
        Expr geExpr = new BinaryPredicate(Operator.GE, k1, intLiteral);
        Expr ltExpr = new BinaryPredicate(Operator.LT, k1, intLiteral);
        Expr gtExpr = new BinaryPredicate(Operator.GT, k1, intLiteral);
        Expr efnExpr = new BinaryPredicate(Operator.EQ_FOR_NULL, new SlotRef(null, "k1"), new IntLiteral(3));
        Assertions.assertEquals("{\"term\":{\"k1\":3}}", EsUtil.toEsDsl(eqExpr).toJson());
        Assertions.assertEquals("{\"bool\":{\"must_not\":{\"term\":{\"k1\":3}}}}", EsUtil.toEsDsl(neExpr).toJson());
        Assertions.assertEquals("{\"range\":{\"k1\":{\"lte\":3}}}", EsUtil.toEsDsl(leExpr).toJson());
        Assertions.assertEquals("{\"range\":{\"k1\":{\"gte\":3}}}", EsUtil.toEsDsl(geExpr).toJson());
        Assertions.assertEquals("{\"range\":{\"k1\":{\"lt\":3}}}", EsUtil.toEsDsl(ltExpr).toJson());
        Assertions.assertEquals("{\"range\":{\"k1\":{\"gt\":3}}}", EsUtil.toEsDsl(gtExpr).toJson());
        Assertions.assertEquals("{\"term\":{\"k1\":3}}", EsUtil.toEsDsl(efnExpr).toJson());
    }

    @Test
    public void testCompoundPredicateConvertEsDsl() {
        SlotRef k1 = new SlotRef(null, "k1");
        IntLiteral intLiteral1 = new IntLiteral(3);
        SlotRef k2 = new SlotRef(null, "k2");
        IntLiteral intLiteral2 = new IntLiteral(5);
        BinaryPredicate binaryPredicate1 = new BinaryPredicate(Operator.EQ, k1, intLiteral1);
        BinaryPredicate binaryPredicate2 = new BinaryPredicate(Operator.GT, k2, intLiteral2);
        CompoundPredicate andPredicate = new CompoundPredicate(CompoundPredicate.Operator.AND, binaryPredicate1,
                binaryPredicate2);
        CompoundPredicate orPredicate = new CompoundPredicate(CompoundPredicate.Operator.OR, binaryPredicate1,
                binaryPredicate2);
        CompoundPredicate notPredicate = new CompoundPredicate(CompoundPredicate.Operator.NOT, binaryPredicate1, null);
        Assertions.assertEquals("{\"bool\":{\"must\":[{\"term\":{\"k1\":3}},{\"range\":{\"k2\":{\"gt\":5}}}]}}",
                EsUtil.toEsDsl(andPredicate).toJson());
        Assertions.assertEquals("{\"bool\":{\"should\":[{\"term\":{\"k1\":3}},{\"range\":{\"k2\":{\"gt\":5}}}]}}",
                EsUtil.toEsDsl(orPredicate).toJson());
        Assertions.assertEquals("{\"bool\":{\"must_not\":{\"term\":{\"k1\":3}}}}",
                EsUtil.toEsDsl(notPredicate).toJson());
    }

    @Test
    public void testIsNullPredicateConvertEsDsl() {
        SlotRef k1 = new SlotRef(null, "k1");
        IsNullPredicate isNullPredicate = new IsNullPredicate(k1, false);
        IsNullPredicate isNotNullPredicate = new IsNullPredicate(k1, true);
        Assertions.assertEquals("{\"bool\":{\"must_not\":{\"exists\":{\"field\":\"k1\"}}}}",
                EsUtil.toEsDsl(isNullPredicate).toJson());
        Assertions.assertEquals("{\"exists\":{\"field\":\"k1\"}}", EsUtil.toEsDsl(isNotNullPredicate).toJson());
    }

    @Test
    public void testLikePredicateConvertEsDsl() {
        SlotRef k1 = new SlotRef(null, "k1");
        StringLiteral stringLiteral1 = new StringLiteral("%1%");
        StringLiteral stringLiteral2 = new StringLiteral("*1*");
        StringLiteral stringLiteral3 = new StringLiteral("1_2");
        LikePredicate likePredicate1 = new LikePredicate(LikePredicate.Operator.LIKE, k1, stringLiteral1);
        LikePredicate regexPredicate = new LikePredicate(LikePredicate.Operator.REGEXP, k1, stringLiteral2);
        LikePredicate likePredicate2 = new LikePredicate(LikePredicate.Operator.LIKE, k1, stringLiteral3);
        Assertions.assertEquals("{\"wildcard\":{\"k1\":\"*1*\"}}", EsUtil.toEsDsl(likePredicate1).toJson());
        Assertions.assertEquals("{\"wildcard\":{\"k1\":\"*1*\"}}", EsUtil.toEsDsl(regexPredicate).toJson());
        Assertions.assertEquals("{\"wildcard\":{\"k1\":\"1?2\"}}", EsUtil.toEsDsl(likePredicate2).toJson());
    }

    @Test
    public void testInPredicateConvertEsDsl() {
        SlotRef k1 = new SlotRef(null, "k1");
        IntLiteral intLiteral1 = new IntLiteral(3);
        IntLiteral intLiteral2 = new IntLiteral(5);
        List<Expr> intLiterals = new ArrayList<>();
        intLiterals.add(intLiteral1);
        intLiterals.add(intLiteral2);
        InPredicate isInPredicate = new InPredicate(k1, intLiterals, false);
        InPredicate isNotInPredicate = new InPredicate(k1, intLiterals, true);
        Assertions.assertEquals("{\"terms\":{\"k1\":[3,5]}}", EsUtil.toEsDsl(isInPredicate).toJson());
        Assertions.assertEquals("{\"bool\":{\"must_not\":{\"terms\":{\"k1\":[3,5]}}}}",
                EsUtil.toEsDsl(isNotInPredicate).toJson());
    }

    @Test
    public void testFunctionCallConvertEsDsl() {
        SlotRef k1 = new SlotRef(null, "k1");
        String str = "{\"bool\":{\"must_not\":{\"terms\":{\"k1\":[3,5]}}}}";
        StringLiteral stringLiteral = new StringLiteral(str);
        List<Expr> exprs = new ArrayList<>();
        exprs.add(k1);
        exprs.add(stringLiteral);
        FunctionCallExpr functionCallExpr = new FunctionCallExpr("esquery", exprs);
        Assertions.assertEquals(str, EsUtil.toEsDsl(functionCallExpr).toJson());

        SlotRef k2 = new SlotRef(null, "k2");
        IntLiteral intLiteral = new IntLiteral(5);
        BinaryPredicate binaryPredicate = new BinaryPredicate(Operator.EQ, k2, intLiteral);
        CompoundPredicate compoundPredicate = new CompoundPredicate(CompoundPredicate.Operator.AND, binaryPredicate,
                functionCallExpr);
        Assertions.assertEquals(
                "{\"bool\":{\"must\":[{\"term\":{\"k2\":5}},{\"bool\":{\"must_not\":{\"terms\":{\"k1\":[3,5]}}}}]}}",
                EsUtil.toEsDsl(compoundPredicate).toJson());
    }

    @Test
    public void testCastConvertEsDsl() {
        FloatLiteral floatLiteral = new FloatLiteral(3.14);
        CastExpr castExpr = new CastExpr(Type.INT, floatLiteral);
        BinaryPredicate castPredicate = new BinaryPredicate(Operator.EQ, castExpr, new IntLiteral(3));
        List<Expr> notPushDownList = new ArrayList<>();
        Map<String, String> fieldsContext = new HashMap<>();
        Assertions.assertNull(EsUtil.toEsDsl(castPredicate, notPushDownList, fieldsContext));
        Assertions.assertEquals(1, notPushDownList.size());

        SlotRef k2 = new SlotRef(null, "k2");
        IntLiteral intLiteral = new IntLiteral(5);
        BinaryPredicate eqPredicate = new BinaryPredicate(Operator.EQ, k2, intLiteral);
        CompoundPredicate compoundPredicate = new CompoundPredicate(CompoundPredicate.Operator.OR, castPredicate,
                eqPredicate);
        EsUtil.toEsDsl(compoundPredicate, notPushDownList, fieldsContext);
        Assertions.assertEquals(3, notPushDownList.size());

        SlotRef k3 = new SlotRef(null, "k3");
        k3.setType(Type.FLOAT);
        CastExpr castDoubleExpr = new CastExpr(Type.DOUBLE, k3);
        BinaryPredicate castDoublePredicate = new BinaryPredicate(Operator.GE, castDoubleExpr,
                new FloatLiteral(3.0, Type.DOUBLE));
        EsUtil.toEsDsl(castDoublePredicate, notPushDownList, fieldsContext);
        Assertions.assertEquals(3, notPushDownList.size());
    }

    @Test
    public void testEs6Mapping() throws IOException, URISyntaxException {
        JSONObject testAliases = EsUtil.getMappingProps("test", loadJsonFromFile("data/es/es6_aliases_mapping.json"),
                "doc");
        Assertions.assertEquals("{\"test4\":{\"type\":\"date\"},\"test2\":{\"type\":\"text\","
                + "\"fields\":{\"keyword\":{\"ignore_above\":256,\"type\":\"keyword\"}}},"
                + "\"test3\":{\"type\":\"double\"},\"test1\":{\"type\":\"keyword\"}}", testAliases.toJSONString());
        JSONObject testAliasesNoType = EsUtil.getMappingProps("test",
                loadJsonFromFile("data/es/es6_aliases_mapping.json"), null);
        Assertions.assertEquals("{\"test4\":{\"type\":\"date\"},\"test2\":{\"type\":\"text\","
                        + "\"fields\":{\"keyword\":{\"ignore_above\":256,\"type\":\"keyword\"}}},"
                        + "\"test3\":{\"type\":\"double\"},\"test1\":{\"type\":\"keyword\"}}",
                testAliasesNoType.toJSONString());
        JSONObject testIndex = EsUtil.getMappingProps("test", loadJsonFromFile("data/es/es6_index_mapping.json"),
                "doc");
        Assertions.assertEquals("{\"test4\":{\"type\":\"date\"},\"test2\":{\"type\":\"text\","
                + "\"fields\":{\"keyword\":{\"ignore_above\":256,\"type\":\"keyword\"}}},"
                + "\"test3\":{\"type\":\"double\"},\"test1\":{\"type\":\"keyword\"}}", testIndex.toJSONString());
    }

    @Test
    public void testEs7Mapping() throws IOException, URISyntaxException {
        JSONObject testAliases = EsUtil.getMappingProps("test", loadJsonFromFile("data/es/es7_aliases_mapping.json"),
                null);
        Assertions.assertEquals("{\"test4\":{\"type\":\"date\"},\"test2\":{\"type\":\"text\","
                + "\"fields\":{\"keyword\":{\"ignore_above\":256,\"type\":\"keyword\"}}},"
                + "\"test3\":{\"type\":\"double\"},\"test1\":{\"type\":\"keyword\"}}", testAliases.toJSONString());
        JSONObject testAliasesErrorType = EsUtil.getMappingProps("test",
                loadJsonFromFile("data/es/es7_aliases_mapping.json"), "doc");
        Assertions.assertEquals("{\"test4\":{\"type\":\"date\"},\"test2\":{\"type\":\"text\","
                        + "\"fields\":{\"keyword\":{\"ignore_above\":256,\"type\":\"keyword\"}}},"
                        + "\"test3\":{\"type\":\"double\"},\"test1\":{\"type\":\"keyword\"}}",
                testAliasesErrorType.toJSONString());
        JSONObject testIndex = EsUtil.getMappingProps("test", loadJsonFromFile("data/es/es7_index_mapping.json"),
                "doc");
        Assertions.assertEquals("{\"test4\":{\"type\":\"date\"},\"test2\":{\"type\":\"text\","
                + "\"fields\":{\"keyword\":{\"ignore_above\":256,\"type\":\"keyword\"}}},"
                + "\"test3\":{\"type\":\"double\"},\"test1\":{\"type\":\"keyword\"}}", testIndex.toJSONString());
    }

    @Test
    public void testEs8Mapping() throws IOException, URISyntaxException {
        JSONObject testAliases = EsUtil.getMappingProps("test", loadJsonFromFile("data/es/es8_aliases_mapping.json"),
                null);
        Assertions.assertEquals("{\"test4\":{\"type\":\"date\"},\"test2\":{\"type\":\"text\","
                + "\"fields\":{\"keyword\":{\"ignore_above\":256,\"type\":\"keyword\"}}},"
                + "\"test3\":{\"type\":\"double\"},\"test1\":{\"type\":\"keyword\"}}", testAliases.toJSONString());
        JSONObject testAliasesErrorType = EsUtil.getMappingProps("test",
                loadJsonFromFile("data/es/es8_aliases_mapping.json"), "doc");
        Assertions.assertEquals("{\"test4\":{\"type\":\"date\"},\"test2\":{\"type\":\"text\","
                        + "\"fields\":{\"keyword\":{\"ignore_above\":256,\"type\":\"keyword\"}}},"
                        + "\"test3\":{\"type\":\"double\"},\"test1\":{\"type\":\"keyword\"}}",
                testAliasesErrorType.toJSONString());
        JSONObject testIndex = EsUtil.getMappingProps("test", loadJsonFromFile("data/es/es8_index_mapping.json"),
                "doc");
        Assertions.assertEquals("{\"test4\":{\"type\":\"date\"},\"test2\":{\"type\":\"text\","
                + "\"fields\":{\"keyword\":{\"ignore_above\":256,\"type\":\"keyword\"}}},"
                + "\"test3\":{\"type\":\"double\"},\"test1\":{\"type\":\"keyword\"}}", testIndex.toJSONString());
    }

}
