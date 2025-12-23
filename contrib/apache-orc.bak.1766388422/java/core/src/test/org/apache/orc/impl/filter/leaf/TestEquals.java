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

import org.apache.orc.impl.filter.ATestFilter;
import org.apache.orc.impl.filter.VectorFilter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestEquals extends ATestFilter {

  @Test
  public void testFoundMatching() {
    setBatch(new Long[] {1L, 2L, 3L, 4L, 5L, 6L},
             new String[] {"a", "b", "c", "d", "e", "f"});
    VectorFilter f = new LongFilters.LongEquals("f1", 3L, false);
    assertFalse(fc.isSelectedInUse());
    filter(f);

    validateSelected(2);
  }

  @Test
  public void testNothingFound() {
    setBatch(new Long[] {1L, 2L, 3L, 4L, 5L, null},
             new String[] {"a", "b", "c", "d", "e", "f"});
    VectorFilter f = new LongFilters.LongEquals("f1", 8L, false);
    assertFalse(fc.isSelectedInUse());
    filter(f);

    validateNoneSelected();
  }

  @Test
  public void testRepeatingVector() {
    setBatch(new Long[] {1L, null, null, null, null, null},
             new String[] {"a", "b", "c", "d", "e", "f"});
    fc.getCols()[0].isRepeating = true;
    VectorFilter f = new LongFilters.LongEquals("f1", 1L, false);
    filter(f);
    validateAllSelected(6);
  }

  @Test
  public void testRepeatingNull() {
    setBatch(new Long[] {null, null, null, null, null, null},
             new String[] {"a", "b", "c", "d", "e", "f"});
    fc.getCols()[0].isRepeating = true;
    VectorFilter f = new LongFilters.LongEquals("f1", 1L, false);
    filter(f);
    validateNoneSelected();
  }
}