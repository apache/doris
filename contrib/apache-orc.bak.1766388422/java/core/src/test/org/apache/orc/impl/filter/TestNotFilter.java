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

package org.apache.orc.impl.filter;

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.OrcFilterContext;
import org.apache.orc.impl.filter.leaf.TestFilters;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestNotFilter extends ATestFilter {

  @Test
  public void testUnboundedNot() {
    setBatch(new Long[] {1L, 2L, 3L, 4L, 5L, 6L},
             new String[] {"a", "b", "c", "d", "e", "f"});
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startNot()
      .in("f1", PredicateLeaf.Type.LONG, 3L, 5L)
      .end()
      .build();
    Consumer<OrcFilterContext> f = TestFilters.createBatchFilter(sArg, schema);
    f.accept(fc.setBatch(batch));

    validateSelected(0, 1, 3, 5);
  }

  @Test
  public void testEmptyUnbounded() {
    setBatch(new Long[] {1L, 2L, 3L, 4L, 5L, 6L},
             new String[] {"a", "b", "c", "d", "e", "f"});
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startNot()
      .in("f1", PredicateLeaf.Type.LONG, 7L, 8L)
      .end()
      .build();
    Consumer<OrcFilterContext> f = TestFilters.createBatchFilter(sArg, schema);
    f.accept(fc.setBatch(batch));

    assertEquals(6, fc.getSelectedSize());
    assertArrayEquals(new int[] {0, 1, 2, 3, 4, 5},
                             Arrays.copyOf(fc.getSelected(), fc.getSelectedSize()));
  }

  @Test
  public void testBounded() {
    setBatch(new Long[] {1L, 2L, 3L, 4L, 5L, 6L},
             new String[] {"a", "b", "c", "d", "e", "f"});
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startAnd()
      .in("f2", PredicateLeaf.Type.STRING, "b", "c")
      .startNot()
      .in("f1", PredicateLeaf.Type.LONG, 2L, 8L)
      .end()
      .end()
      .build();
    Consumer<OrcFilterContext> f = TestFilters.createBatchFilter(sArg, schema);
    f.accept(fc.setBatch(batch));

    validateSelected(2);
  }

  @Test
  public void testEmptyBounded() {
    setBatch(new Long[] {1L, 2L, 3L, 4L, 5L, 6L},
             new String[] {"a", "b", "c", "d", "e", "f"});
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startAnd()
      .in("f2", PredicateLeaf.Type.STRING, "b", "c")
      .startNot()
      .in("f1", PredicateLeaf.Type.LONG, 7L, 8L)
      .end()
      .end()
      .build();
    Consumer<OrcFilterContext> f = TestFilters.createBatchFilter(sArg, schema);
    f.accept(fc.setBatch(batch));

    validateSelected(1, 2);
  }

  @Test
  public void testNotAndPushDown() {
    setBatch(new Long[] {1L, 2L, 3L, 4L, 5L, 6L},
             new String[] {"a", "b", "c", "d", "e", "f"});
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startNot()
      .startAnd()
      .equals("f1", PredicateLeaf.Type.LONG, 3L)
      .equals("f2", PredicateLeaf.Type.STRING, "c")
      .end()
      .end()
      .build();

    Consumer<OrcFilterContext> f = TestFilters.createBatchFilter(sArg, schema);
    f.accept(fc.setBatch(batch));

    validateSelected(0, 1, 3, 4, 5);
  }

  @Test
  public void testNotOrPushDown() {
    setBatch(new Long[] {1L, 2L, 3L, 4L, 5L, 6L},
             new String[] {"a", "b", "c", "d", "e", "f"});
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startNot()
      .startOr()
      .equals("f1", PredicateLeaf.Type.LONG, 3L)
      .equals("f2", PredicateLeaf.Type.STRING, "d")
      .end()
      .end()
      .build();

    Consumer<OrcFilterContext> f = TestFilters.createBatchFilter(sArg, schema);
    f.accept(fc.setBatch(batch));

    validateSelected(0, 1, 4, 5);
  }
}