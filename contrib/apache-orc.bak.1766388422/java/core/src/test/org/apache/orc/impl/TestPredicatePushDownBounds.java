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
package org.apache.orc.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.IntegerColumnStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.util.BloomFilter;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.apache.orc.impl.TestRecordReaderImpl.createPredicateLeaf;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestPredicatePushDownBounds {

  /**
   * This test case handles the Equals corner case where the predicate is equal
   * to truncated upper and lower bounds.
   *
   * @throws Exception
   */
  @Test
  public void testCornerCases() {

    int stringLength = 1100;
    byte[] utf8F;
    byte[] utf8P;

    final TypeDescription schema = TypeDescription.createString();
    final ColumnStatisticsImpl stat = ColumnStatisticsImpl.create(schema);

    BloomFilter bf = new BloomFilter(100);
    // FFF... to PPP...
    for (int i = 70; i <= 80; i++) {
      final String inputString = StringUtils
          .repeat(Character.toString((char) i), stringLength);
      bf.addString(inputString);
    }

    final String longStringF = StringUtils
        .repeat(Character.toString('F'), stringLength);
    final String longStringP = StringUtils
        .repeat(Character.toString('P'), stringLength);

    /* String that matches the upperbound value after truncation */
    final String upperboundString =
        StringUtils.repeat(Character.toString('P'), 1023) + "Q";
    /* String that matches the lower value after truncation */
    final String lowerboundString = StringUtils
        .repeat(Character.toString('F'), 1024);

    final String shortStringF = StringUtils.repeat(Character.toString('F'), 50);
    final String shortStringP =
        StringUtils.repeat(Character.toString('P'), 50) + "Q";

    /* Test for a case EQUALS where only upperbound is set */
    final PredicateLeaf predicateUpperBoundEquals = TestRecordReaderImpl
        .createPredicateLeaf(PredicateLeaf.Operator.EQUALS,
            PredicateLeaf.Type.STRING, "x", upperboundString, null);

    /* Test for a case LESS_THAN where only upperbound is set */
    final PredicateLeaf predicateUpperBoundLessThan = TestRecordReaderImpl
        .createPredicateLeaf(PredicateLeaf.Operator.LESS_THAN,
            PredicateLeaf.Type.STRING, "x", upperboundString, null);

    /* Test for a case LESS_THAN_EQUALS where only upperbound is set */
    final PredicateLeaf predicateUpperBoundLessThanEquals = TestRecordReaderImpl
        .createPredicateLeaf(PredicateLeaf.Operator.LESS_THAN_EQUALS,
            PredicateLeaf.Type.STRING, "x", upperboundString, null);

    utf8F = shortStringF.getBytes(StandardCharsets.UTF_8);
    stat.increment();
    stat.updateString(utf8F, 0, utf8F.length, 1);

    utf8P = longStringP.getBytes(StandardCharsets.UTF_8);
    stat.increment();
    stat.updateString(utf8P, 0, utf8P.length, 1);

    assertEquals(SearchArgument.TruthValue.NO, RecordReaderImpl
        .evaluatePredicate(stat, predicateUpperBoundEquals, null));

    assertEquals(SearchArgument.TruthValue.YES, RecordReaderImpl
        .evaluatePredicate(stat, predicateUpperBoundLessThan, null));

    assertEquals(SearchArgument.TruthValue.YES, RecordReaderImpl
        .evaluatePredicate(stat, predicateUpperBoundLessThanEquals, null));

    stat.reset();

    utf8F = longStringF.getBytes(StandardCharsets.UTF_8);
    stat.increment();
    stat.updateString(utf8F, 0, utf8F.length, 1);

    utf8P = shortStringP.getBytes(StandardCharsets.UTF_8);
    stat.increment();
    stat.updateString(utf8P, 0, utf8P.length, 1);

    /* Test for a case Equals where only lowerbound is set */
    final PredicateLeaf predicateLowerBoundEquals = createPredicateLeaf(
        PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.STRING, "x",
        lowerboundString, null);

    /* Test for a case LESS_THAN where only lowerbound is set */
    final PredicateLeaf predicateLowerBoundLessThan = createPredicateLeaf(
        PredicateLeaf.Operator.LESS_THAN, PredicateLeaf.Type.STRING, "x",
        lowerboundString, null);

    /* Test for a case LESS_THAN_EQUALS where only lowerbound is set */
    final PredicateLeaf predicateLowerBoundLessThanEquals = createPredicateLeaf(
        PredicateLeaf.Operator.LESS_THAN_EQUALS, PredicateLeaf.Type.STRING, "x",
        lowerboundString, null);

    assertEquals(SearchArgument.TruthValue.NO, RecordReaderImpl
        .evaluatePredicate(stat, predicateLowerBoundEquals, null));

    assertEquals(SearchArgument.TruthValue.NO, RecordReaderImpl
        .evaluatePredicate(stat, predicateLowerBoundLessThan, bf));

    assertEquals(SearchArgument.TruthValue.NO, RecordReaderImpl
        .evaluatePredicate(stat, predicateLowerBoundLessThanEquals, null));

  }

  /**
   * A case where the search values fall within the upperbound and lower bound
   * range.
   *
   * @throws Exception
   */
  @Test
  public void testNormalCase() throws Exception {

    int stringLength = 1100;
    /* length of string in BF */
    int bfStringLength = 50;
    //int stringLength = 11;
    byte[] utf8F;
    byte[] utf8P;

    final TypeDescription schema = TypeDescription.createString();
    final ColumnStatisticsImpl stat = ColumnStatisticsImpl.create(schema);

    BloomFilter bf = new BloomFilter(100);
    // FFF... to PPP...
    for (int i = 70; i <= 80; i++) {
      final String inputString = StringUtils
          .repeat(Character.toString((char) i), bfStringLength);
      bf.addString(inputString);
    }

    final String longStringF = StringUtils
        .repeat(Character.toString('F'), stringLength);
    final String longStringP = StringUtils
        .repeat(Character.toString('P'), stringLength);
    final String predicateString = StringUtils
        .repeat(Character.toString('I'), 50);


    /* Test for a case where only upperbound is set */
    final PredicateLeaf predicateEquals = createPredicateLeaf(
        PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.STRING, "x",
        predicateString, null);

    /* trigger lower bound */
    utf8F = longStringF.getBytes(StandardCharsets.UTF_8);
    stat.increment();
    stat.updateString(utf8F, 0, utf8F.length, 1);

    /* trigger upper bound */
    utf8P = longStringP.getBytes(StandardCharsets.UTF_8);
    stat.increment();
    stat.updateString(utf8P, 0, utf8P.length, 1);

    assertEquals(SearchArgument.TruthValue.YES_NO,
        RecordReaderImpl.evaluatePredicate(stat, predicateEquals, bf));

  }

  /**
   * Test for IN search arg when upper and lower bounds are set.
   *
   * @throws Exception
   */
  @Test
  public void testIN() throws Exception {
    int stringLength = 1100;
    byte[] utf8F;
    byte[] utf8P;

    final TypeDescription schema = TypeDescription.createString();
    final ColumnStatisticsImpl stat = ColumnStatisticsImpl.create(schema);

    final BloomFilter bf = new BloomFilter(100);
    // FFF... to PPP...
    for (int i = 70; i <= 80; i++) {
      final String inputString = StringUtils
          .repeat(Character.toString((char) i), stringLength);
      bf.addString(inputString);
    }

    final String longStringF = StringUtils
        .repeat(Character.toString('F'), stringLength);
    final String longStringP = StringUtils
        .repeat(Character.toString('P'), stringLength);

    /* String that matches the upperbound value after truncation */
    final String upperboundString =
        StringUtils.repeat(Character.toString('P'), 1023) + "Q";
    /* String that matches the lower value after truncation */
    final String lowerboundString = StringUtils
        .repeat(Character.toString('F'), 1024);

    final String shortStringF = StringUtils.repeat(Character.toString('F'), 50);
    final String shortStringP =
        StringUtils.repeat(Character.toString('P'), 50) + "Q";

    final List<Object> args = new ArrayList<Object>();
    args.add(upperboundString);

    /* set upper bound */
    utf8F = shortStringF.getBytes(StandardCharsets.UTF_8);
    stat.increment();
    stat.updateString(utf8F, 0, utf8F.length, 1);

    utf8P = longStringP.getBytes(StandardCharsets.UTF_8);
    stat.increment();
    stat.updateString(utf8P, 0, utf8P.length, 1);

    /* Test for a case IN where only upper bound is set and test literal is equal to upperbound */
    final PredicateLeaf predicateUpperBoundSet = TestRecordReaderImpl
        .createPredicateLeaf(PredicateLeaf.Operator.IN,
            PredicateLeaf.Type.STRING, "x", null, args);

    assertEquals(SearchArgument.TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(stat, predicateUpperBoundSet, null));

    /* Test for lower bound set only */
    args.clear();
    args.add(lowerboundString);

    stat.reset();
    /* set lower bound */
    utf8F = longStringF.getBytes(StandardCharsets.UTF_8);
    stat.increment();
    stat.updateString(utf8F, 0, utf8F.length, 1);

    utf8P = shortStringP.getBytes(StandardCharsets.UTF_8);
    stat.increment();
    stat.updateString(utf8P, 0, utf8P.length, 1);

    /* Test for a case IN where only lower bound is set and the test literal is lowerbound string */
    final PredicateLeaf predicateLowerBoundSet = TestRecordReaderImpl
        .createPredicateLeaf(PredicateLeaf.Operator.IN,
            PredicateLeaf.Type.STRING, "x", null, args);

    assertEquals(SearchArgument.TruthValue.NO,
        RecordReaderImpl.evaluatePredicate(stat, predicateLowerBoundSet, null));

    /* Test the case were both upper and lower bounds are set */
    args.clear();
    args.add(lowerboundString);
    args.add(upperboundString);

    stat.reset();
    /* set upper and lower bound */
    utf8F = longStringF.getBytes(StandardCharsets.UTF_8);
    stat.increment();
    stat.updateString(utf8F, 0, utf8F.length, 1);

    utf8P = longStringP.getBytes(StandardCharsets.UTF_8);
    stat.increment();
    stat.updateString(utf8P, 0, utf8P.length, 1);

    final PredicateLeaf predicateUpperLowerBoundSet = TestRecordReaderImpl
        .createPredicateLeaf(PredicateLeaf.Operator.IN,
            PredicateLeaf.Type.STRING, "x", null, args);

    assertEquals(SearchArgument.TruthValue.NO, RecordReaderImpl
        .evaluatePredicate(stat, predicateUpperLowerBoundSet, null));

    /* test the boundary condition */
    args.clear();
    args.add(longStringF);
    args.add(longStringP);

    stat.reset();
    /* set upper and lower bound */
    utf8F = longStringF.getBytes(StandardCharsets.UTF_8);
    stat.increment();
    stat.updateString(utf8F, 0, utf8F.length, 1);

    utf8P = longStringP.getBytes(StandardCharsets.UTF_8);
    stat.increment();
    stat.updateString(utf8P, 0, utf8P.length, 1);

    final PredicateLeaf predicateUpperLowerBoundSetBoundary = TestRecordReaderImpl
        .createPredicateLeaf(PredicateLeaf.Operator.IN,
            PredicateLeaf.Type.STRING, "x", null, args);

    assertEquals(SearchArgument.TruthValue.YES_NO, RecordReaderImpl
        .evaluatePredicate(stat, predicateUpperLowerBoundSetBoundary, null));

  }

  /**
   * Test for LESS_THAN_EQUALS search arg when upper and lower bounds are the same.
   *
   * @throws Exception
   */
  @Test
  public void testLessThanEquals() {
    final TypeDescription schema = TypeDescription.createInt();
    final ColumnStatisticsImpl stat = ColumnStatisticsImpl.create(schema);

    stat.increment();
    stat.updateInteger(1, 100);
    stat.updateInteger(3, 100);

    IntegerColumnStatistics typed = (IntegerColumnStatistics) stat;
    assertEquals(1, typed.getMinimum());
    assertEquals(3, typed.getMaximum());

    SearchArgument sArg = SearchArgumentFactory.newBuilder()
        .startAnd()
        .lessThanEquals("c", PredicateLeaf.Type.LONG, 3L)
        .end()
        .build();

    assertEquals(SearchArgument.TruthValue.YES, RecordReaderImpl
        .evaluatePredicate(stat, sArg.getLeaves().get(0), null));

    // Corner case where MIN == MAX == 3
    final ColumnStatisticsImpl newStat = ColumnStatisticsImpl.create(schema);

    newStat.increment();
    newStat.updateInteger(3, 100);

    typed = (IntegerColumnStatistics) newStat;
    assertEquals(3, typed.getMinimum());
    assertEquals(3, typed.getMaximum());

    sArg = SearchArgumentFactory.newBuilder()
        .startAnd()
        .lessThanEquals("c", PredicateLeaf.Type.LONG, 3L)
        .end()
        .build();

    assertEquals(SearchArgument.TruthValue.YES, RecordReaderImpl
        .evaluatePredicate(newStat, sArg.getLeaves().get(0), null));
  }
}
