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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.literal.Literal;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

public class FrontendConjunctsUtilsTest {
    @Test
    public void testEqString() {
        EqualTo equalTo = generateEqualTo("c1", "v1");
        Assertions.assertFalse(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(equalTo), "c1", "v1"));
        // Return true only when the columnName matches but the value differs.
        Assertions.assertTrue(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(equalTo), "c1", "v2"));
        Assertions.assertFalse(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(equalTo), "c2", "v2"));
        Assertions.assertFalse(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(equalTo), "c2", "v1"));
    }

    @Test
    public void testEqDigit() {
        EqualTo equalTo = generateEqualTo("c1", 2L);
        Assertions.assertFalse(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(equalTo), "c1", 2));
        Assertions.assertFalse(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(equalTo), "c1", 2L));
        Assertions.assertFalse(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(equalTo), "c1", 2.0));
        Assertions.assertFalse(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(equalTo), "c1", 2.0d));
        Assertions.assertTrue(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(equalTo), "c1", 3));
    }

    @Test
    public void testOr() {
        Or or = new Or(generateEqualTo("c1", "v1"), generateEqualTo("c2", "v2"));
        Assertions.assertFalse(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(or), "c1", "v1"));
        Assertions.assertFalse(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(or), "c2", "v2"));
        Assertions.assertFalse(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(or), "c3", "v3"));
        Assertions.assertFalse(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(or), "c1", "v2"));
        Assertions.assertFalse(
                FrontendConjunctsUtils.isFiltered(Lists.newArrayList(or), ImmutableMap.of("c1", "v1", "c2", "v2")));
        Assertions.assertFalse(
                FrontendConjunctsUtils.isFiltered(Lists.newArrayList(or), ImmutableMap.of("c1", "v1", "c2", "v3")));
        Assertions.assertFalse(
                FrontendConjunctsUtils.isFiltered(Lists.newArrayList(or), ImmutableMap.of("c1", "v3", "c2", "v2")));
        Assertions.assertTrue(
                FrontendConjunctsUtils.isFiltered(Lists.newArrayList(or), ImmutableMap.of("c1", "v3", "c2", "v3")));
    }

    @Test
    public void testMultiConjuncts() {
        EqualTo c1 = generateEqualTo("c1", 1);
        EqualTo c2 = generateEqualTo("c2", 2);
        Assertions.assertTrue(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(c1, c2), "c1", 2));
        Assertions.assertFalse(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(c1, c2), "c1", 1));
        Assertions.assertFalse(
                FrontendConjunctsUtils.isFiltered(Lists.newArrayList(c1, c2), ImmutableMap.of("c1", 1, "c2", 2)));
        Assertions.assertTrue(
                FrontendConjunctsUtils.isFiltered(Lists.newArrayList(c1, c2), ImmutableMap.of("c1", 2, "c2", 2)));
    }

    @Test
    public void testException() {
        EqualTo c1 = generateEqualTo("c1", 1);
        Assertions.assertFalse(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(c1), "c1", "v1"));
    }

    @Test
    public void testIn() {
        InPredicate in = generateIn("c1", Lists.newArrayList("v1", "v2"));
        Assertions.assertFalse(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(in), "c1", "v1"));
        Assertions.assertTrue(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(in), "c1", "v3"));
    }

    @Test
    public void testLike() {
        Like like = generateLike("c1", "%value%");
        Assertions.assertFalse(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(like), "c1", "value1"));
        Assertions.assertFalse(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(like), "c1", "1value"));
        // FoldConstant not support like, so return false
        Assertions.assertFalse(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(like), "c1", "xxx"));
    }

    @Test
    public void testNotIn() {
        Not notIn = generateNotIn("c1", Lists.newArrayList("v1", "v2"));
        Assertions.assertTrue(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(notIn), "c1", "v1"));
        Assertions.assertFalse(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(notIn), "c1", "v3"));
    }

    @Test
    public void testLessThan() {
        LessThan lessThan = generateLessThan("c1", 2L);
        Assertions.assertFalse(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(lessThan), "c1", 1L));
        Assertions.assertTrue(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(lessThan), "c1", 2L));
    }

    @Test
    public void testLessThanEqual() {
        LessThanEqual lessThanEqual = generateLessThanEqual("c1", 2L);
        Assertions.assertFalse(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(lessThanEqual), "c1", 1L));
        Assertions.assertFalse(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(lessThanEqual), "c1", 2L));
        Assertions.assertTrue(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(lessThanEqual), "c1", 3L));
    }

    @Test
    public void testGreaterThan() {
        GreaterThan greaterThan = generateGreaterThan("c1", 2L);
        Assertions.assertFalse(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(greaterThan), "c1", 3L));
        Assertions.assertTrue(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(greaterThan), "c1", 2L));
    }

    @Test
    public void testGreaterThanEqual() {
        GreaterThanEqual greaterThanEqual = generateGreaterThanEqual("c1", 2L);
        Assertions.assertFalse(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(greaterThanEqual), "c1", 3L));
        Assertions.assertFalse(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(greaterThanEqual), "c1", 2L));
        Assertions.assertTrue(FrontendConjunctsUtils.isFiltered(Lists.newArrayList(greaterThanEqual), "c1", 1L));
    }

    private EqualTo generateEqualTo(String columnName, Object value) {
        UnboundSlot c1 = new UnboundSlot(columnName);
        Literal v1 = Literal.of(value);
        return new EqualTo(c1, v1);
    }

    private InPredicate generateIn(String columnName, List<Object> values) {
        UnboundSlot c1 = new UnboundSlot(columnName);
        List<Expression> literals = values.stream().map(Literal::of).collect(Collectors.toList());
        return new InPredicate(c1, literals);
    }

    private Not generateNotIn(String columnName, List<Object> values) {
        UnboundSlot c1 = new UnboundSlot(columnName);
        List<Expression> literals = values.stream().map(Literal::of).collect(Collectors.toList());
        InPredicate inPredicate = new InPredicate(c1, literals);
        return new Not(inPredicate);
    }

    private Like generateLike(String columnName, Object value) {
        UnboundSlot c1 = new UnboundSlot(columnName);
        Literal v1 = Literal.of(value);
        return new Like(c1, v1);
    }

    private LessThan generateLessThan(String columnName, Object value) {
        UnboundSlot c1 = new UnboundSlot(columnName);
        Literal v1 = Literal.of(value);
        return new LessThan(c1, v1);
    }

    private LessThanEqual generateLessThanEqual(String columnName, Object value) {
        UnboundSlot c1 = new UnboundSlot(columnName);
        Literal v1 = Literal.of(value);
        return new LessThanEqual(c1, v1);
    }

    private GreaterThan generateGreaterThan(String columnName, Object value) {
        UnboundSlot c1 = new UnboundSlot(columnName);
        Literal v1 = Literal.of(value);
        return new GreaterThan(c1, v1);
    }

    private GreaterThanEqual generateGreaterThanEqual(String columnName, Object value) {
        UnboundSlot c1 = new UnboundSlot(columnName);
        Literal v1 = Literal.of(value);
        return new GreaterThanEqual(c1, v1);
    }
}
