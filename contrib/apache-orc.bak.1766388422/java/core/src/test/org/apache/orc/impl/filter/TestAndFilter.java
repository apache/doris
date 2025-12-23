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
import org.apache.orc.OrcFile;
import org.apache.orc.OrcFilterContext;
import org.apache.orc.impl.filter.leaf.TestFilters;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestAndFilter extends ATestFilter {

  @Test
  public void testAndSelectsNothing() {
    setBatch(new Long[] {1L, 2L, 3L, 4L, 5L, 6L},
             new String[] {"a", "b", "c", "d", "e", "f"});
    SearchArgument s = SearchArgumentFactory.newBuilder()
      .startAnd()
      .equals("f1", PredicateLeaf.Type.LONG, 3L)
      .equals("f1", PredicateLeaf.Type.LONG, 4L)
      .end()
      .build();
    Consumer<OrcFilterContext> f = TestFilters.createBatchFilter(s,
                                                                 schema,
                                                                 OrcFile.Version.CURRENT);
    assertFalse(fc.isSelectedInUse());
    f.accept(fc);

    validateNoneSelected();
  }

  @Test
  public void testANDConversion() throws FilterFactory.UnSupportedSArgException {
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
      .startAnd()
      .in("f1", PredicateLeaf.Type.LONG, 1L, 2L, 3L)
      .in("f2", PredicateLeaf.Type.STRING, "a", "b", "c")
      .end()
      .build();

    Set<String> colIds = new HashSet<>();
    VectorFilter f = FilterFactory.createSArgFilter(sarg.getCompactExpression(),
                                                    colIds,
                                                    sarg.getLeaves(),
                                                    schema,
                                                    false,
                                                    OrcFile.Version.CURRENT);
    assertNotNull(f);
    assertTrue(f instanceof AndFilter);
    assertEquals(2, ((AndFilter) f).filters.length);
    assertEquals(2, colIds.size());
    assertTrue(colIds.contains("f1"));
    assertTrue(colIds.contains("f2"));

    // Setup the data such that the AND condition should not select any row
    setBatch(
      new Long[] {1L, 0L, 2L, 4L, 3L},
      new String[] {"z", "a", "y", "b", "x"});
    fc.setBatch(batch);

    filter(f);
    assertTrue(fc.isSelectedInUse());
    assertEquals(0, fc.getSelectedSize());
  }

}