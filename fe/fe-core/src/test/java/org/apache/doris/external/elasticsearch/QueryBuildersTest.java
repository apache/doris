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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Check that internal queries are correctly converted to ES search query (as JSON)
 */
public class QueryBuildersTest {

    private final ObjectMapper mapper = new ObjectMapper();


    @Test
    public void testTermQuery() throws Exception {
        Assert.assertEquals("{\"term\":{\"k\":\"aaaa\"}}",
                toJson(QueryBuilders.termQuery("k", "aaaa")));
        Assert.assertEquals("{\"term\":{\"aaaa\":\"k\"}}",
                toJson(QueryBuilders.termQuery("aaaa", "k")));
        Assert.assertEquals("{\"term\":{\"k\":0}}",
                toJson(QueryBuilders.termQuery("k", (byte) 0)));
        Assert.assertEquals("{\"term\":{\"k\":123}}",
                toJson(QueryBuilders.termQuery("k", (long) 123)));
        Assert.assertEquals("{\"term\":{\"k\":41}}",
                toJson(QueryBuilders.termQuery("k", (short) 41)));
        Assert.assertEquals("{\"term\":{\"k\":128}}",
                toJson(QueryBuilders.termQuery("k", 128)));
        Assert.assertEquals("{\"term\":{\"k\":42.42}}",
                toJson(QueryBuilders.termQuery("k", 42.42D)));
        Assert.assertEquals("{\"term\":{\"k\":1.1}}",
                toJson(QueryBuilders.termQuery("k", 1.1F)));
        Assert.assertEquals("{\"term\":{\"k\":1}}",
                toJson(QueryBuilders.termQuery("k", new BigDecimal(1))));
        Assert.assertEquals("{\"term\":{\"k\":121}}",
                toJson(QueryBuilders.termQuery("k", new BigInteger("121"))));
        Assert.assertEquals("{\"term\":{\"k\":true}}",
                toJson(QueryBuilders.termQuery("k", new AtomicBoolean(true))));
    }

    @Test
    public void testTermsQuery() throws Exception {

        Assert.assertEquals("{\"terms\":{\"k\":[]}}",
                toJson(QueryBuilders.termsQuery("k", Collections.emptySet())));

        Assert.assertEquals("{\"terms\":{\"k\":[0]}}",
                toJson(QueryBuilders.termsQuery("k", Collections.singleton(0))));

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
        QueryBuilders.QueryBuilder q1 = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("k", "aaa"));

        Assert.assertEquals("{\"bool\":{\"must\":{\"term\":{\"k\":\"aaa\"}}}}",
                toJson(q1));

        QueryBuilders.QueryBuilder q2 = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("k1", "aaa")).must(QueryBuilders.termQuery("k2", "bbb"));

        Assert.assertEquals("{\"bool\":{\"must\":[{\"term\":{\"k1\":\"aaa\"}},{\"term\":{\"k2\":\"bbb\"}}]}}",
                toJson(q2));

        QueryBuilders.QueryBuilder q3 = QueryBuilders.boolQuery()
                .mustNot(QueryBuilders.termQuery("k", "fff"));

        Assert.assertEquals("{\"bool\":{\"must_not\":{\"term\":{\"k\":\"fff\"}}}}",
                toJson(q3));

        QueryBuilders.QueryBuilder q4 = QueryBuilders.rangeQuery("k1").lt(200).gt(-200);
        QueryBuilders.QueryBuilder q5 = QueryBuilders.termsQuery("k2", Arrays.asList("aaa", "bbb", "ccc"));
        QueryBuilders.QueryBuilder q6 = QueryBuilders.boolQuery().must(q4).should(q5);
        Assert.assertEquals("{\"bool\":{\"must\":{\"range\":{\"k1\":{\"gt\":-200,\"lt\":200}}},\"should\":{\"terms\":{\"k2\":[\"aaa\",\"bbb\",\"ccc\"]}}}}", toJson(q6));
        Assert.assertEquals("{\"bool\":{\"filter\":[{\"range\":{\"k1\":{\"gt\":-200,\"lt\":200}}},{\"terms\":{\"k2\":[\"aaa\",\"bbb\",\"ccc\"]}}]}}", toJson(QueryBuilders.boolQuery().filter(q4).filter(q5)));
        Assert.assertEquals("{\"bool\":{\"filter\":{\"range\":{\"k1\":{\"gt\":-200,\"lt\":200}}},\"must_not\":{\"terms\":{\"k2\":[\"aaa\",\"bbb\",\"ccc\"]}}}}", toJson(QueryBuilders.boolQuery().filter(q4).mustNot(q5)));

    }

    @Test
    public void testExistsQuery() throws Exception {
        Assert.assertEquals("{\"exists\":{\"field\":\"k\"}}",
                toJson(QueryBuilders.existsQuery("k")));
    }

    @Test
    public void testRangeQuery() throws Exception {
        Assert.assertEquals("{\"range\":{\"k\":{\"lt\":123}}}",
                toJson(QueryBuilders.rangeQuery("k").lt(123)));
        Assert.assertEquals("{\"range\":{\"k\":{\"gt\":123}}}",
                toJson(QueryBuilders.rangeQuery("k").gt(123)));
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
        Assert.assertEquals("{\"range\":{\"k\":{\"gte\":\"zzz\"}}}",
                toJson(QueryBuilders.rangeQuery("k").gte("zzz")));
        Assert.assertEquals("{\"range\":{\"k\":{\"gt\":\"aaa\",\"lt\":\"zzz\"}}}",
                toJson(QueryBuilders.rangeQuery("k").gt("aaa").lt("zzz")));
    }

    @Test
    public void testMatchAllQuery() throws IOException {
        Assert.assertEquals("{\"match_all\":{}}",
                toJson(QueryBuilders.matchAllQuery()));
    }

    @Test
    public void testWildCardQuery() throws IOException {
        Assert.assertEquals("{\"wildcard\":{\"k1\":\"?aa*\"}}",
                toJson(QueryBuilders.wildcardQuery("k1", "?aa*")));
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
