/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.impl.filter.leaf;

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.impl.filter.FilterUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestFloatFilters extends ATestLeafFilter {

  @Test
    public void testEquals() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .equals("f4", PredicateLeaf.Type.FLOAT, getPredicateValue(PredicateLeaf.Type.FLOAT, lowIdx))
      .build();
    assertFalse(fc.isSelectedInUse());
    FilterUtils.createVectorFilter(sArg, schema).accept(fc);

    validateSelected(PredicateLeaf.Operator.EQUALS, false);
  }

  @Test
    public void testNotEquals() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startNot()
      .equals("f4", PredicateLeaf.Type.FLOAT, getPredicateValue(PredicateLeaf.Type.FLOAT, lowIdx))
      .end()
      .build();
    assertFalse(fc.isSelectedInUse());
    FilterUtils.createVectorFilter(sArg, schema).accept(fc);

    validateSelected(PredicateLeaf.Operator.EQUALS, true);
  }

  @Test
    public void testLessThan() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .lessThan("f4", PredicateLeaf.Type.FLOAT, getPredicateValue(PredicateLeaf.Type.FLOAT, lowIdx))
      .build();
    assertFalse(fc.isSelectedInUse());
    FilterUtils.createVectorFilter(sArg, schema).accept(fc);

    validateSelected(PredicateLeaf.Operator.LESS_THAN, false);
  }

  @Test
    public void testNotLessThan() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startNot()
      .lessThan("f4", PredicateLeaf.Type.FLOAT, getPredicateValue(PredicateLeaf.Type.FLOAT, lowIdx))
      .end()
      .build();
    assertFalse(fc.isSelectedInUse());
    FilterUtils.createVectorFilter(sArg, schema).accept(fc);

    validateSelected(PredicateLeaf.Operator.LESS_THAN, true);
  }

  @Test
    public void testLessThanEquals() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .lessThanEquals("f4", PredicateLeaf.Type.FLOAT, getPredicateValue(PredicateLeaf.Type.FLOAT, lowIdx))
      .build();
    assertFalse(fc.isSelectedInUse());
    FilterUtils.createVectorFilter(sArg, schema).accept(fc);

    validateSelected(PredicateLeaf.Operator.LESS_THAN_EQUALS, false);
  }

  @Test
    public void testNotLessThanEquals() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startNot()
      .lessThanEquals("f4", PredicateLeaf.Type.FLOAT, getPredicateValue(PredicateLeaf.Type.FLOAT, lowIdx))
      .end()
      .build();
    assertFalse(fc.isSelectedInUse());
    FilterUtils.createVectorFilter(sArg, schema).accept(fc);

    validateSelected(PredicateLeaf.Operator.LESS_THAN_EQUALS, true);
  }

  @Test
    public void testBetween() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .between("f4", PredicateLeaf.Type.FLOAT,
          getPredicateValue(PredicateLeaf.Type.FLOAT, lowIdx),
          getPredicateValue(PredicateLeaf.Type.FLOAT, highIdx))
      .build();
    assertFalse(fc.isSelectedInUse());
    FilterUtils.createVectorFilter(sArg, schema).accept(fc);

    validateSelected(PredicateLeaf.Operator.BETWEEN, false);
  }

  @Test
    public void testNotBetween() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startNot()
      .between("f4", PredicateLeaf.Type.FLOAT,
          getPredicateValue(PredicateLeaf.Type.FLOAT, lowIdx),
          getPredicateValue(PredicateLeaf.Type.FLOAT, highIdx))
      .end()
      .build();
    assertFalse(fc.isSelectedInUse());
    FilterUtils.createVectorFilter(sArg, schema).accept(fc);

    validateSelected(PredicateLeaf.Operator.BETWEEN, true);
  }

  @Test
    public void testIn() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .in("f4", PredicateLeaf.Type.FLOAT,
          getPredicateValue(PredicateLeaf.Type.FLOAT, lowIdx),
          getPredicateValue(PredicateLeaf.Type.FLOAT, highIdx))
      .build();
    assertFalse(fc.isSelectedInUse());
    FilterUtils.createVectorFilter(sArg, schema).accept(fc);

    validateSelected(PredicateLeaf.Operator.IN, false);
  }

  @Test
    public void testNotIn() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startNot()
      .in("f4", PredicateLeaf.Type.FLOAT,
          getPredicateValue(PredicateLeaf.Type.FLOAT, lowIdx),
          getPredicateValue(PredicateLeaf.Type.FLOAT, highIdx))
      .end()
      .build();
    assertFalse(fc.isSelectedInUse());
    FilterUtils.createVectorFilter(sArg, schema).accept(fc);

    validateSelected(PredicateLeaf.Operator.IN, true);
  }

  @Test
    public void testIsNull() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .isNull("f4", PredicateLeaf.Type.FLOAT)
      .build();
    assertFalse(fc.isSelectedInUse());
    FilterUtils.createVectorFilter(sArg, schema).accept(fc);

    validateSelected(PredicateLeaf.Operator.IS_NULL, false);
  }

  @Test
    public void testNotIsNull() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startNot()
      .isNull("f4", PredicateLeaf.Type.FLOAT)
      .end()
      .build();
    assertFalse(fc.isSelectedInUse());
    FilterUtils.createVectorFilter(sArg, schema).accept(fc);

    validateSelected(PredicateLeaf.Operator.IS_NULL, true);
  }

}
