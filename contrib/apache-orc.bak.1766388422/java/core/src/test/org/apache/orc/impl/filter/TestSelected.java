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

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSelected {
  private final Selected src = new Selected();
  private final Selected tgt = new Selected();

  @Test
  public void testUnionBothEmpty() {
    // Both are empty
    src.sel = new int[10];
    tgt.sel = new int[10];
    tgt.unionDisjoint(src);
    assertArrayEquals(new int[10], tgt.sel);
    assertEquals(0, tgt.selSize);
  }

  @Test
  public void testUnionTgtEmpty() {
    // tgt has no selection
    src.sel = new int[] {1, 3, 7, 0, 0};
    src.selSize = 3;
    tgt.sel = new int[5];
    tgt.selSize = 0;
    tgt.unionDisjoint(src);
    assertEquals(src.selSize, tgt.selSize);
    assertArrayEquals(src.sel, tgt.sel);
  }

  @Test
  public void testUnionSrcEmpty() {
    // current size is zero
    src.sel = new int[5];
    src.selSize = 0;
    tgt.sel = new int[] {1, 3, 7, 0, 0};
    tgt.selSize = 3;
    tgt.unionDisjoint(src);
    validate(tgt, 1, 3, 7);
  }

  @Test
  public void testUnionCurrSmallerThanAdd() {
    // current size is zero
    src.sel = new int[] {7, 0, 0, 0, 0};
    src.selSize = 1;
    tgt.sel = new int[] {1, 3, 0, 0, 0};
    tgt.selSize = 2;
    tgt.unionDisjoint(src);
    validate(tgt, 1, 3, 7);
  }

  @Test
  public void testUnionAddSmallerThanCurr() {
    // current size is zero
    src.sel = new int[] {1, 7, 0, 0, 0};
    src.selSize = 2;
    tgt.sel = new int[] {3, 0, 0, 0, 0};
    tgt.selSize = 1;
    tgt.unionDisjoint(src);
    validate(tgt, 1, 3, 7);
  }

  @Test
  public void testUnionNoChange() {
    // current size is zero
    src.sel = new int[] {0, 0, 0, 0, 0};
    src.selSize = 0;
    tgt.sel = new int[] {1, 3, 7, 0, 0};
    tgt.selSize = 3;
    tgt.unionDisjoint(src);
    validate(tgt, 1, 3, 7);
  }

  @Test
  public void testUnionNewEnclosed() {
    // current size is zero
    src.sel = new int[] {1, 7, 0, 0, 0};
    src.selSize = 2;
    tgt.sel = new int[] {3, 4, 0, 0, 0};
    tgt.selSize = 2;
    tgt.unionDisjoint(src);
    validate(tgt, 1, 3, 4, 7);
  }

  @Test
  public void testUnionPrevEnclosed() {
    // current size is zero
    src.sel = new int[] {3, 4, 0, 0, 0};
    src.selSize = 2;
    tgt.sel = new int[] {1, 7, 0, 0, 0};
    tgt.selSize = 2;
    tgt.unionDisjoint(src);
    validate(tgt, 1, 3, 4, 7);
  }

  @Test
  public void testMinus() {
    src.sel = new int[] {3, 4, 0, 0, 0};
    src.selSize = 2;
    tgt.sel = new int[] {1, 7, 0, 0, 0};
    tgt.selSize = 2;
    tgt.minus(src);
    validate(tgt, 1, 7);
  }

  @Test
  public void testMinusAllElements() {
    src.sel = new int[] {1, 7, 0, 0, 0};
    src.selSize = 2;
    tgt.sel = new int[] {1, 7, 0, 0, 0};
    tgt.selSize = 2;
    tgt.minus(src);
    assertEquals(0, tgt.selSize);
  }

  @Test
  public void testMinusInterleavedElements() {
    src.sel = new int[] {1, 5, 9, 0, 0};
    src.selSize = 3;
    tgt.sel = new int[] {1, 3, 5, 7, 9};
    tgt.selSize = 5;
    tgt.minus(src);
    validate(tgt, 3, 7);
  }

  @Test
  public void testMinusEmpty() {
    src.sel = new int[] {1, 5, 9, 0, 0};
    src.selSize = 0;
    tgt.sel = new int[] {1, 3, 5, 7, 9};
    tgt.selSize = 5;
    tgt.minus(src);
    validate(tgt, 1, 3, 5, 7, 9);
  }

  @Test
  public void testMinusSrcLarger() {
    src.sel = new int[] {10, 50, 90, 0, 0};
    src.selSize = 3;
    tgt.sel = new int[] {1, 3, 5, 7, 9};
    tgt.selSize = 5;
    tgt.minus(src);
    validate(tgt, 1, 3, 5, 7, 9);
  }

  @Test
  public void testMinusSrcSmaller() {
    tgt.sel = new int[] {10, 50, 90, 0, 0};
    tgt.selSize = 3;
    src.sel = new int[] {1, 3, 5, 7, 9};
    src.selSize = 5;
    tgt.minus(src);
    validate(tgt, 10, 50, 90);
  }

  private void validate(Selected tgt, int... expected) {
    assertArrayEquals(expected, Arrays.copyOf(tgt.sel, tgt.selSize));
  }
}