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
import org.apache.orc.OrcFile;
import org.apache.orc.OrcFilterContext;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.filter.BatchFilter;
import org.apache.orc.impl.filter.ATestFilter;
import org.apache.orc.impl.filter.AndFilter;
import org.apache.orc.impl.filter.FilterFactory;
import org.apache.orc.impl.filter.FilterUtils;
import org.apache.orc.impl.filter.OrFilter;
import org.apache.orc.impl.filter.VectorFilter;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestFilters extends ATestFilter {

  public static BatchFilter createBatchFilter(SearchArgument sArg,
                                              TypeDescription readSchema) {
    return createBatchFilter(sArg, readSchema, OrcFile.Version.UNSTABLE_PRE_2_0);
  }

  public static BatchFilter createBatchFilter(SearchArgument sArg,
                                              TypeDescription readSchema,
                                              OrcFile.Version version) {
    return createBatchFilter(sArg, readSchema, version, false);
  }

  public static BatchFilter createBatchFilter(SearchArgument sArg,
                                              TypeDescription readSchema,
                                              OrcFile.Version version,
                                              boolean normalize) {
    Reader.Options options = new Reader.Options().allowSARGToFilter(true);
    options.searchArgument(sArg, new String[0]);
    return FilterFactory.createBatchFilter(options, readSchema, false,
                                           version, normalize, null, null);
  }

  @Test
  public void testAndOfOr() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startAnd()
      .startOr()
      .in("f1", PredicateLeaf.Type.LONG, 1L, 6L)
      .in("f1", PredicateLeaf.Type.LONG, 3L, 4L)
      .end()
      .startOr()
      .in("f1", PredicateLeaf.Type.LONG, 1L, 6L)
      .in("f2", PredicateLeaf.Type.STRING, "c", "e")
      .end()
      .end()
      .build();

    setBatch(new Long[] {1L, 2L, 3L, 4L, 5L, 6L},
             new String[] {"a", "b", "c", "d", "e", "f"});

    BatchFilter filter = FilterUtils.createVectorFilter(sArg, schema);
    filter.accept(fc);
    assertArrayEquals(new String[] {"f1", "f2"}, filter.getColumnNames());
    validateSelected(0, 2, 5);
  }

  @Test
  public void testOrOfAnd() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startOr()
      .startAnd()
      .in("f1", PredicateLeaf.Type.LONG, 1L, 6L)
      .in("f2", PredicateLeaf.Type.STRING, "a", "c")
      .end()
      .startAnd()
      .in("f1", PredicateLeaf.Type.LONG, 3L, 4L)
      .in("f2", PredicateLeaf.Type.STRING, "c", "e")
      .end()
      .end()
      .build();

    setBatch(new Long[] {1L, 2L, 3L, 4L, 5L, 6L},
             new String[] {"a", "b", "c", "d", "e", "f"});

    FilterUtils.createVectorFilter(sArg, schema).accept(fc.setBatch(batch));
    validateSelected(0, 2);
  }

  @Test
  public void testOrOfAndNative() {
    VectorFilter f = new OrFilter(
      new VectorFilter[] {
        new AndFilter(new VectorFilter[] {
          new LongFilters.LongIn("f1",
                                 Arrays.asList(1L, 6L), false),
          new StringFilters.StringIn("f2",
                                     Arrays.asList("a", "c"), false)
        }),
        new AndFilter(new VectorFilter[] {
          new LongFilters.LongIn("f1",
                                 Arrays.asList(3L, 4L), false),
          new StringFilters.StringIn("f2",
                                     Arrays.asList("c", "e"), false)
        })
      }
    );

    setBatch(new Long[] {1L, 2L, 3L, 4L, 5L, 6L},
             new String[] {"a", "b", "c", "d", "e", "f"});

    filter(f);
    assertEquals(2, fc.getSelectedSize());
    assertArrayEquals(new int[] {0, 2},
                      Arrays.copyOf(fc.getSelected(), fc.getSelectedSize()));
  }

  @Test
  public void testAndNotNot() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startAnd()
      .startNot()
      .in("f1", PredicateLeaf.Type.LONG, 7L)
      .end()
      .startNot()
      .isNull("f2", PredicateLeaf.Type.STRING)
      .end()
      .end()
      .build();

    setBatch(new Long[] {1L, 2L, 3L, 4L, 5L, 6L},
             new String[] {"a", "b", "c", "d", "e", "f"});

    Consumer<OrcFilterContext> filter = createBatchFilter(sArg, schema);
    filter.accept(fc.setBatch(batch));
    assertEquals(6, fc.getSelectedSize());
    assertArrayEquals(new int[] {0, 1, 2, 3, 4, 5},
                      Arrays.copyOf(fc.getSelected(), fc.getSelectedSize()));
  }

  @Test
  public void testUnSupportedSArg() {
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
      .nullSafeEquals("f1", PredicateLeaf.Type.LONG, 0L)
      .build();

    assertNull(FilterUtils.createVectorFilter(sarg, schema));
  }

  @Test
  public void testRepeatedProtected() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startOr()
      .in("f2", PredicateLeaf.Type.STRING, "a", "d")
      .lessThan("f1", PredicateLeaf.Type.LONG, 6L)
      .end()
      .build();

    setBatch(new Long[] {1L, 1L, 1L, 1L, 1L, 1L},
             new String[] {"a", "b", "c", "d", "e", "f"});
    batch.cols[0].isRepeating = true;
    FilterUtils.createVectorFilter(sArg, schema).accept(fc.setBatch(batch));
    validateAllSelected(6);
  }

  @Test
  public void testNullProtected() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startOr()
      .in("f2", PredicateLeaf.Type.STRING, "a", "d")
      .lessThan("f1", PredicateLeaf.Type.LONG, 4L)
      .end()
      .build();

    setBatch(new Long[] {1L, 2L, null, 4L, 5L, 6L},
             new String[] {"a", "b", "c", "d", "e", "f"});
    FilterUtils.createVectorFilter(sArg, schema).accept(fc.setBatch(batch));
    validateSelected(0, 1, 3);
  }

  @Test
  public void testUnsupportedNotLeaf() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startNot()
      .nullSafeEquals("f1", PredicateLeaf.Type.LONG, 2L)
      .end()
      .build();

    assertNull(FilterUtils.createVectorFilter(sArg, schema));
  }

  @Test
  public void testAndOrAnd() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startAnd()
      .startOr()
      .lessThan("f1", PredicateLeaf.Type.LONG, 3L)
      .startAnd()
      .equals("f2", PredicateLeaf.Type.STRING, "a")
      .equals("f1", PredicateLeaf.Type.LONG, 5L)
      .end()
      .end()
      .in("f2", PredicateLeaf.Type.STRING, "a", "c")
      .end()
      .build();

    setBatch(new Long[] {1L, 2L, null, 4L, 5L, 6L},
             new String[] {"a", "b", "c", "d", "e", "f"});
    FilterUtils.createVectorFilter(sArg, schema).accept(fc.setBatch(batch));
    validateSelected(0);
  }
}
