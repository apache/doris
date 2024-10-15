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
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.es.QueryBuilders;
import org.apache.doris.datasource.es.QueryBuilders.BuilderOptions;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Check that internal queries are correctly converted to ES search query (as JSON)
 */
public class QueryBuildersTest {

    private final ObjectMapper mapper = new ObjectMapper();

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
        Assertions.assertEquals("{\"term\":{\"k1\":3}}", QueryBuilders.toEsDsl(eqExpr).toJson());
        Assertions.assertEquals("{\"bool\":{\"must_not\":{\"term\":{\"k1\":3}}}}",
                QueryBuilders.toEsDsl(neExpr).toJson());
        Assertions.assertEquals("{\"range\":{\"k1\":{\"lte\":3}}}", QueryBuilders.toEsDsl(leExpr).toJson());
        Assertions.assertEquals("{\"range\":{\"k1\":{\"gte\":3}}}", QueryBuilders.toEsDsl(geExpr).toJson());
        Assertions.assertEquals("{\"range\":{\"k1\":{\"lt\":3}}}", QueryBuilders.toEsDsl(ltExpr).toJson());
        Assertions.assertEquals("{\"range\":{\"k1\":{\"gt\":3}}}", QueryBuilders.toEsDsl(gtExpr).toJson());
        Assertions.assertEquals("{\"term\":{\"k1\":3}}", QueryBuilders.toEsDsl(efnExpr).toJson());

        SlotRef k2 = new SlotRef(null, "k2");
        Expr dateTimeLiteral = new StringLiteral("2023-02-19 22:00:00");
        Expr dateTimeEqExpr = new BinaryPredicate(Operator.EQ, k2, dateTimeLiteral);
        Assertions.assertEquals("{\"term\":{\"k2\":\"2023-02-19T22:00:00.000+08:00\"}}",
                QueryBuilders.toEsDsl(dateTimeEqExpr, new ArrayList<>(), new HashMap<>(),
                        BuilderOptions.builder().needCompatDateFields(Lists.newArrayList("k2")).build()).toJson());
        SlotRef k3 = new SlotRef(null, "k3");
        Expr stringLiteral = new StringLiteral("");
        Expr stringNeExpr = new BinaryPredicate(Operator.NE, k3, stringLiteral);
        Assertions.assertEquals("{\"bool\":{\"must\":{\"exists\":{\"field\":\"k3\"}},\"must_not\":{\"term\":{\"k3\":\"\"}}}}",
                QueryBuilders.toEsDsl(stringNeExpr).toJson());
        stringLiteral = new StringLiteral("message");
        stringNeExpr = new BinaryPredicate(Operator.NE, k3, stringLiteral);
        Assertions.assertEquals("{\"bool\":{\"must_not\":{\"term\":{\"k3\":\"message\"}}}}",
                QueryBuilders.toEsDsl(stringNeExpr).toJson());
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
                QueryBuilders.toEsDsl(andPredicate).toJson());
        Assertions.assertEquals("{\"bool\":{\"should\":[{\"term\":{\"k1\":3}},{\"range\":{\"k2\":{\"gt\":5}}}]}}",
                QueryBuilders.toEsDsl(orPredicate).toJson());
        Assertions.assertEquals("{\"bool\":{\"must_not\":{\"term\":{\"k1\":3}}}}",
                QueryBuilders.toEsDsl(notPredicate).toJson());
    }

    @Test
    public void testIsNullPredicateConvertEsDsl() {
        SlotRef k1 = new SlotRef(null, "k1");
        IsNullPredicate isNullPredicate = new IsNullPredicate(k1, false);
        IsNullPredicate isNotNullPredicate = new IsNullPredicate(k1, true);
        Assertions.assertEquals("{\"bool\":{\"must_not\":{\"exists\":{\"field\":\"k1\"}}}}",
                QueryBuilders.toEsDsl(isNullPredicate).toJson());
        Assertions.assertEquals("{\"exists\":{\"field\":\"k1\"}}", QueryBuilders.toEsDsl(isNotNullPredicate).toJson());
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
        Assertions.assertEquals("{\"wildcard\":{\"k1\":\"*1*\"}}", QueryBuilders.toEsDsl(likePredicate1).toJson());
        Assertions.assertEquals("{\"wildcard\":{\"k1\":\"*1*\"}}", QueryBuilders.toEsDsl(regexPredicate).toJson());
        Assertions.assertEquals("{\"wildcard\":{\"k1\":\"1?2\"}}", QueryBuilders.toEsDsl(likePredicate2).toJson());
        List<Expr> notPushDownList = new ArrayList<>();
        Assertions.assertNull(QueryBuilders.toEsDsl(likePredicate2, notPushDownList, new HashMap<>(),
                BuilderOptions.builder().likePushDown(false).build()));
        Assertions.assertFalse(notPushDownList.isEmpty());
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
        Assertions.assertEquals("{\"terms\":{\"k1\":[3,5]}}", QueryBuilders.toEsDsl(isInPredicate).toJson());
        Assertions.assertEquals("{\"bool\":{\"must_not\":{\"terms\":{\"k1\":[3,5]}}}}",
                QueryBuilders.toEsDsl(isNotInPredicate).toJson());
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
        Assertions.assertEquals(str, QueryBuilders.toEsDsl(functionCallExpr).toJson());

        SlotRef k2 = new SlotRef(null, "k2");
        IntLiteral intLiteral = new IntLiteral(5);
        BinaryPredicate binaryPredicate = new BinaryPredicate(Operator.EQ, k2, intLiteral);
        CompoundPredicate compoundPredicate = new CompoundPredicate(CompoundPredicate.Operator.AND, binaryPredicate,
                functionCallExpr);
        Assertions.assertEquals(
                "{\"bool\":{\"must\":[{\"term\":{\"k2\":5}},{\"bool\":{\"must_not\":{\"terms\":{\"k1\":[3,5]}}}}]}}",
                QueryBuilders.toEsDsl(compoundPredicate).toJson());
    }

    @Test
    public void testCastConvertEsDsl() {
        FloatLiteral floatLiteral = new FloatLiteral(3.14);
        CastExpr castExpr = new CastExpr(Type.INT, floatLiteral);
        BinaryPredicate castPredicate = new BinaryPredicate(Operator.EQ, castExpr, new IntLiteral(3));
        List<Expr> notPushDownList = new ArrayList<>();
        Map<String, String> fieldsContext = new HashMap<>();
        BuilderOptions builderOptions = BuilderOptions.builder().likePushDown(true).build();
        Assertions.assertNull(QueryBuilders.toEsDsl(castPredicate, notPushDownList, fieldsContext, builderOptions));
        Assertions.assertEquals(1, notPushDownList.size());

        SlotRef k2 = new SlotRef(null, "k2");
        IntLiteral intLiteral = new IntLiteral(5);
        BinaryPredicate eqPredicate = new BinaryPredicate(Operator.EQ, k2, intLiteral);
        CompoundPredicate compoundPredicate = new CompoundPredicate(CompoundPredicate.Operator.OR, castPredicate,
                eqPredicate);

        QueryBuilders.toEsDsl(compoundPredicate, notPushDownList, fieldsContext, builderOptions);
        Assertions.assertEquals(3, notPushDownList.size());

        SlotRef k3 = new SlotRef(null, "k3");
        k3.setType(Type.FLOAT);
        CastExpr castDoubleExpr = new CastExpr(Type.DOUBLE, k3);
        BinaryPredicate castDoublePredicate = new BinaryPredicate(Operator.GE, castDoubleExpr,
                new FloatLiteral(3.0, Type.DOUBLE));
        QueryBuilders.toEsDsl(castDoublePredicate, notPushDownList, fieldsContext, builderOptions);
        Assertions.assertEquals(3, notPushDownList.size());

        SlotRef k4 = new SlotRef(null, "k4");
        k4.setType(Type.FLOAT);
        CastExpr castFloatExpr = new CastExpr(Type.FLOAT, k4);
        BinaryPredicate castFloatPredicate = new BinaryPredicate(Operator.GE, new FloatLiteral(3.0, Type.FLOAT),
                castFloatExpr);
        QueryBuilders.QueryBuilder queryBuilder = QueryBuilders.toEsDsl(castFloatPredicate, notPushDownList, fieldsContext, builderOptions);
        Assertions.assertEquals("{\"range\":{\"k4\":{\"lte\":3.0}}}", queryBuilder.toJson());
        Assertions.assertEquals(3, notPushDownList.size());

        castFloatPredicate = new BinaryPredicate(Operator.LE, new FloatLiteral(3.0, Type.FLOAT),
            castFloatExpr);
        queryBuilder = QueryBuilders.toEsDsl(castFloatPredicate, notPushDownList, fieldsContext, builderOptions);
        Assertions.assertEquals("{\"range\":{\"k4\":{\"gte\":3.0}}}", queryBuilder.toJson());
        Assertions.assertEquals(3, notPushDownList.size());

        castFloatPredicate = new BinaryPredicate(Operator.LT, new FloatLiteral(3.0, Type.FLOAT),
            castFloatExpr);
        queryBuilder = QueryBuilders.toEsDsl(castFloatPredicate, notPushDownList, fieldsContext, builderOptions);
        Assertions.assertEquals("{\"range\":{\"k4\":{\"gt\":3.0}}}", queryBuilder.toJson());
        Assertions.assertEquals(3, notPushDownList.size());

        castFloatPredicate = new BinaryPredicate(Operator.GT, new FloatLiteral(3.0, Type.FLOAT),
            castFloatExpr);
        queryBuilder = QueryBuilders.toEsDsl(castFloatPredicate, notPushDownList, fieldsContext, builderOptions);
        Assertions.assertEquals("{\"range\":{\"k4\":{\"lt\":3.0}}}", queryBuilder.toJson());
        Assertions.assertEquals(3, notPushDownList.size());
    }


    @Test
    public void testTermQuery() throws Exception {
        Assert.assertEquals("{\"term\":{\"k\":\"aaaa\"}}", toJson(QueryBuilders.termQuery("k", "aaaa")));
        Assert.assertEquals("{\"term\":{\"aaaa\":\"k\"}}", toJson(QueryBuilders.termQuery("aaaa", "k")));
        Assert.assertEquals("{\"term\":{\"k\":0}}", toJson(QueryBuilders.termQuery("k", (byte) 0)));
        Assert.assertEquals("{\"term\":{\"k\":123}}", toJson(QueryBuilders.termQuery("k", (long) 123)));
        Assert.assertEquals("{\"term\":{\"k\":41}}", toJson(QueryBuilders.termQuery("k", (short) 41)));
        Assert.assertEquals("{\"term\":{\"k\":128}}", toJson(QueryBuilders.termQuery("k", 128)));
        Assert.assertEquals("{\"term\":{\"k\":42.42}}", toJson(QueryBuilders.termQuery("k", 42.42D)));
        Assert.assertEquals("{\"term\":{\"k\":1.1}}", toJson(QueryBuilders.termQuery("k", 1.1F)));
        Assert.assertEquals("{\"term\":{\"k\":1}}", toJson(QueryBuilders.termQuery("k", new BigDecimal(1))));
        Assert.assertEquals("{\"term\":{\"k\":121}}", toJson(QueryBuilders.termQuery("k", new BigInteger("121"))));
        Assert.assertEquals("{\"term\":{\"k\":true}}", toJson(QueryBuilders.termQuery("k", new AtomicBoolean(true))));
    }

    @Test
    public void testTermsQuery() throws Exception {

        Assert.assertEquals("{\"terms\":{\"k\":[]}}", toJson(QueryBuilders.termsQuery("k", Collections.emptySet())));

        Assert.assertEquals("{\"terms\":{\"k\":[0]}}", toJson(QueryBuilders.termsQuery("k", Collections.singleton(0))));

        Assert.assertEquals("{\"terms\":{\"k\":[\"aaa\"]}}",
                toJson(QueryBuilders.termsQuery("k", Collections.singleton("aaa"))));

        Assert.assertEquals("{\"terms\":{\"k\":[\"aaa\",\"bbb\",\"ccc\"]}}",
                toJson(QueryBuilders.termsQuery("k", Arrays.asList("aaa", "bbb", "ccc"))));

        Assert.assertEquals("{\"terms\":{\"k\":[1,2,3]}}",
                toJson(QueryBuilders.termsQuery("k", Arrays.asList(1, 2, 3))));

        Assert.assertEquals("{\"terms\":{\"k\":[1.1,2.2,3.3]}}",
                toJson(QueryBuilders.termsQuery("k", Arrays.asList(1.1f, 2.2f, 3.3f))));

        Assert.assertEquals("{\"terms\":{\"k\":[1.1,2.2,3.3]}}",
                toJson(QueryBuilders.termsQuery("k", Arrays.asList(1.1d, 2.2d, 3.3d))));
    }

    @Test
    public void testBoolQuery() throws Exception {
        QueryBuilders.QueryBuilder q1 = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("k", "aaa"));

        Assert.assertEquals("{\"bool\":{\"must\":{\"term\":{\"k\":\"aaa\"}}}}", toJson(q1));

        QueryBuilders.QueryBuilder q2 = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("k1", "aaa"))
                .must(QueryBuilders.termQuery("k2", "bbb"));

        Assert.assertEquals("{\"bool\":{\"must\":[{\"term\":{\"k1\":\"aaa\"}},{\"term\":{\"k2\":\"bbb\"}}]}}",
                toJson(q2));

        QueryBuilders.QueryBuilder q3 = QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("k", "fff"));

        Assert.assertEquals("{\"bool\":{\"must_not\":{\"term\":{\"k\":\"fff\"}}}}", toJson(q3));

        QueryBuilders.QueryBuilder q4 = QueryBuilders.rangeQuery("k1").lt(200).gt(-200);
        QueryBuilders.QueryBuilder q5 = QueryBuilders.termsQuery("k2", Arrays.asList("aaa", "bbb", "ccc"));
        QueryBuilders.QueryBuilder q6 = QueryBuilders.boolQuery().must(q4).should(q5);
        Assert.assertEquals(
                "{\"bool\":{\"must\":{\"range\":{\"k1\":{\"gt\":-200,\"lt\":200}}},\"should\":{\"terms\":{\"k2\":[\"aaa\",\"bbb\",\"ccc\"]}}}}",
                toJson(q6));
        Assert.assertEquals(
                "{\"bool\":{\"filter\":[{\"range\":{\"k1\":{\"gt\":-200,\"lt\":200}}},{\"terms\":{\"k2\":[\"aaa\",\"bbb\",\"ccc\"]}}]}}",
                toJson(QueryBuilders.boolQuery().filter(q4).filter(q5)));
        Assert.assertEquals(
                "{\"bool\":{\"filter\":{\"range\":{\"k1\":{\"gt\":-200,\"lt\":200}}},\"must_not\":{\"terms\":{\"k2\":[\"aaa\",\"bbb\",\"ccc\"]}}}}",
                toJson(QueryBuilders.boolQuery().filter(q4).mustNot(q5)));

    }

    @Test
    public void testExistsQuery() throws Exception {
        Assert.assertEquals("{\"exists\":{\"field\":\"k\"}}", toJson(QueryBuilders.existsQuery("k")));
    }

    @Test
    public void testRangeQuery() throws Exception {
        Assert.assertEquals("{\"range\":{\"k\":{\"lt\":123}}}", toJson(QueryBuilders.rangeQuery("k").lt(123)));
        Assert.assertEquals("{\"range\":{\"k\":{\"gt\":123}}}", toJson(QueryBuilders.rangeQuery("k").gt(123)));
        Assert.assertEquals("{\"range\":{\"k\":{\"gte\":12345678}}}",
                toJson(QueryBuilders.rangeQuery("k").gte(12345678)));
        Assert.assertEquals("{\"range\":{\"k\":{\"lte\":12345678}}}",
                toJson(QueryBuilders.rangeQuery("k").lte(12345678)));
        Assert.assertEquals("{\"range\":{\"k\":{\"gt\":123,\"lt\":345}}}",
                toJson(QueryBuilders.rangeQuery("k").gt(123).lt(345)));
        Assert.assertEquals("{\"range\":{\"k\":{\"gt\":-456.6,\"lt\":12.3}}}",
                toJson(QueryBuilders.rangeQuery("k").lt(12.3f).gt(-456.6f)));
        Assert.assertEquals("{\"range\":{\"k\":{\"gt\":6789.33,\"lte\":9999.99}}}",
                toJson(QueryBuilders.rangeQuery("k").gt(6789.33f).lte(9999.99f)));
        Assert.assertEquals("{\"range\":{\"k\":{\"gte\":1,\"lte\":\"zzz\"}}}",
                toJson(QueryBuilders.rangeQuery("k").gte(1).lte("zzz")));
        Assert.assertEquals("{\"range\":{\"k\":{\"gte\":\"zzz\"}}}", toJson(QueryBuilders.rangeQuery("k").gte("zzz")));
        Assert.assertEquals("{\"range\":{\"k\":{\"gt\":\"aaa\",\"lt\":\"zzz\"}}}",
                toJson(QueryBuilders.rangeQuery("k").gt("aaa").lt("zzz")));
    }

    @Test
    public void testMatchAllQuery() throws IOException {
        Assert.assertEquals("{\"match_all\":{}}", toJson(QueryBuilders.matchAllQuery()));
    }

    @Test
    public void testWildCardQuery() throws IOException {
        Assert.assertEquals("{\"wildcard\":{\"k1\":\"?aa*\"}}", toJson(QueryBuilders.wildcardQuery("k1", "?aa*")));
    }

    private String toJson(QueryBuilders.QueryBuilder builder) throws IOException {
        StringWriter writer = new StringWriter();
        JsonGenerator gen = mapper.getFactory().createGenerator(writer);
        builder.toJson(gen);
        gen.flush();
        gen.close();
        return writer.toString();
    }
}
