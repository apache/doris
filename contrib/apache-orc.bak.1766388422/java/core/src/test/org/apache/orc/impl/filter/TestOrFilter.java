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
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestOrFilter extends ATestFilter {
  @Test
  public void testORConversion() throws FilterFactory.UnSupportedSArgException {
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
      .startOr()
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
    assertTrue(f instanceof OrFilter);
    assertEquals(2, ((OrFilter) f).filters.length);
    assertEquals(2, colIds.size());
    assertTrue(colIds.contains("f1"));
    assertTrue(colIds.contains("f2"));

    // Setup the data such that the OR condition should select every row
    setBatch(
      new Long[] {1L, 0L, 2L, 4L, 3L},
      new String[] {"z", "a", "y", "b", "x"});
    fc.setBatch(batch);
    filter(f);
    validateAllSelected(5);
  }
}
