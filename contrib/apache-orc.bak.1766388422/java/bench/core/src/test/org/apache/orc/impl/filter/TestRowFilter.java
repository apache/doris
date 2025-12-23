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

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.OrcFilterContextImpl;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRowFilter extends ATestFilter {

  private final TypeDescription schema = TypeDescription.createStruct()
    .addField("f1", TypeDescription.createLong())
    .addField("f2", TypeDescription.createString());
  final OrcFilterContextImpl fc = new OrcFilterContextImpl(schema, false);

  private final VectorizedRowBatch batch = schema.createRowBatch();

  @Test
  public void testINLongConversion() throws FilterFactory.UnSupportedSArgException {
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
      .in("f1", PredicateLeaf.Type.LONG, 1L, 2L, 3L)
      .build();

    Set<String> colIds = new HashSet<>();
    RowFilter filter = RowFilterFactory.create(sarg.getExpression(),
                                               colIds,
                                               sarg.getLeaves(),
                                               schema,
                                               OrcFile.Version.CURRENT);
    assertNotNull(filter);
    assertTrue(filter instanceof RowFilter.LeafFilter);
    assertEquals(1, colIds.size());
    assertTrue(colIds.contains("f1"));

    setBatch(new Long[] {1L, 0L, 2L, 4L, 3L},
             new String[] {});
    fc.setBatch(batch);

    for (int i = 0; i < batch.size; i++) {
      if (i % 2 == 0) {
        assertTrue(filter.accept(fc, i));
      } else {
        assertFalse(filter.accept(fc, i));
      }
    }
  }

  @Test
  public void testINStringConversion() throws FilterFactory.UnSupportedSArgException {
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
      .in("f2", PredicateLeaf.Type.STRING, "a", "b")
      .build();

    Set<String> colIds = new HashSet<>();
    RowFilter filter = RowFilterFactory.create(sarg.getExpression(),
                                               colIds,
                                               sarg.getLeaves(),
                                               schema,
                                               OrcFile.Version.CURRENT);
    assertNotNull(filter);
    assertTrue(filter instanceof RowFilter.LeafFilter);
    assertEquals(1, colIds.size());
    assertTrue(colIds.contains("f2"));

    setBatch(new Long[] {1L, 0L, 2L, 4L, 3L},
             new String[] {"a", "z", "b", "y", "a"});
    fc.setBatch(batch);

    for (int i = 0; i < batch.size; i++) {
      if (i % 2 == 0) {
        assertTrue(filter.accept(fc, i));
      } else {
        assertFalse(filter.accept(fc, i));
      }
    }
  }

  @Test
  public void testORConversion() throws FilterFactory.UnSupportedSArgException {
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
      .startOr()
      .in("f1", PredicateLeaf.Type.LONG, 1L, 2L, 3L)
      .in("f2", PredicateLeaf.Type.STRING, "a", "b", "c")
      .end()
      .build();

    Set<String> colIds = new HashSet<>();
    RowFilter filter = RowFilterFactory.create(sarg.getExpression(),
                                               colIds,
                                               sarg.getLeaves(),
                                               schema,
                                               OrcFile.Version.CURRENT);
    assertNotNull(filter);
    assertTrue(filter instanceof RowFilter.OrFilter);
    assertEquals(2, ((RowFilter.OrFilter) filter).filters.length);
    assertEquals(2, colIds.size());
    assertTrue(colIds.contains("f1"));
    assertTrue(colIds.contains("f2"));

    // Setup the data such that the OR condition should select every row
    setBatch(new Long[] {1L, 0L, 2L, 4L, 3L},
             new String[] {"z", "a", "y", "b", "x"});
    fc.setBatch(batch);


    for (int i = 0; i < batch.size; i++) {
      assertTrue(filter.accept(fc, i));
    }
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
    RowFilter filter = RowFilterFactory.create(sarg.getExpression(),
                                               colIds,
                                               sarg.getLeaves(),
                                               schema,
                                               OrcFile.Version.CURRENT);
    assertNotNull(filter);
    assertTrue(filter instanceof RowFilter.AndFilter);
    assertEquals(2, ((RowFilter.AndFilter) filter).filters.length);
    assertEquals(2, colIds.size());
    assertTrue(colIds.contains("f1"));
    assertTrue(colIds.contains("f2"));

    // Setup the data such that the AND condition should not select any row
    setBatch(new Long[] {1L, 0L, 2L, 4L, 3L},
             new String[] {"z", "a", "y", "b", "x"});
    fc.setBatch(batch);

    for (int i = 0; i < batch.size; i++) {
      assertFalse(filter.accept(fc, i));
    }
  }
}