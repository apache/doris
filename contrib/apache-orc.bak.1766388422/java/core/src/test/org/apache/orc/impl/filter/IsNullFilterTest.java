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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;

public class IsNullFilterTest extends ATestFilter {
  @Test
  public void nullFilterTest() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startOr()
      .isNull("f1", PredicateLeaf.Type.LONG)
      .isNull("f2", PredicateLeaf.Type.STRING)
      .end()
      .build();

    setBatch(new Long[] {1L, 2L, null, 4L, 5L, null},
             new String[] {"a", "b", "c", null, "e", "f"});
    assertFalse(fc.isSelectedInUse());
    FilterUtils.createVectorFilter(sArg, schema).accept(fc);

    validateSelected(2, 3, 5);
  }

  @Test
  public void repeatedNullFilterTest() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startOr()
      .equals("f2", PredicateLeaf.Type.STRING, "c")
      .isNull("f1", PredicateLeaf.Type.LONG)
      .end()
      .build();

    setBatch(new Long[] {null, null, null, null, null, null},
             new String[] {"a", "b", "c", "d", "e", "f"});
    batch.cols[0].isRepeating = true;
    batch.cols[0].noNulls = false;
    batch.cols[1].isRepeating = false;
    batch.cols[1].noNulls = false;
    assertFalse(fc.isSelectedInUse());
    FilterUtils.createVectorFilter(sArg, schema).accept(fc);

    validateAllSelected(6);
  }

  @Test
  public void notNullFilterTest() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startNot()
      .startOr()
      .isNull("f1", PredicateLeaf.Type.LONG)
      .isNull("f2", PredicateLeaf.Type.STRING)
      .end()
      .end()
      .build();

    setBatch(new Long[] {1L, 2L, null, 4L, 5L, null},
             new String[] {"a", "b", "c", null, "e", "f"});
    assertFalse(fc.isSelectedInUse());
    FilterUtils.createVectorFilter(sArg, schema).accept(fc);

    validateSelected(0, 1, 4);
  }

  @Test
  public void repeatedNotNullFilterTest() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startOr()
      .equals("f2", PredicateLeaf.Type.STRING, "c")
      .startNot()
      .isNull("f1", PredicateLeaf.Type.LONG)
      .end()
      .end()
      .build();

    setBatch(new Long[] {null, null, null, null, null, null},
             new String[] {"a", "b", "c", "d", "e", "f"});
    batch.cols[0].isRepeating = true;
    batch.cols[0].noNulls = false;
    batch.cols[1].isRepeating = false;
    batch.cols[1].noNulls = true;
    assertFalse(fc.isSelectedInUse());
    FilterUtils.createVectorFilter(sArg, schema).accept(fc);

    validateSelected(2);
  }

  @Test
  public void repeatedNotNullFilterNoNullsTest() {
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startOr()
      .equals("f2", PredicateLeaf.Type.STRING, "c")
      .startNot()
      .isNull("f1", PredicateLeaf.Type.LONG)
      .end()
      .end()
      .build();

    setBatch(new Long[] {1L, 1L, 1L, 1L, 1L, 1L},
             new String[] {"a", "b", "c", "d", "e", "f"});
    batch.cols[0].isRepeating = true;
    batch.cols[0].noNulls = true;
    batch.cols[1].isRepeating = false;
    batch.cols[1].noNulls = true;
    assertFalse(fc.isSelectedInUse());
    FilterUtils.createVectorFilter(sArg, schema).accept(fc);

    validateAllSelected(6);
  }
}