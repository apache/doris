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

import org.apache.orc.OrcFilterContext;

/**
 * Wrapper class for the selected vector that centralizes the convenience functions
 */
public class Selected {
  // Sorted array of row indices
  int[] sel;
  int selSize;

  Selected(int[] sel) {
    this.sel = sel;
    this.selSize = 0;
  }

  Selected() {
    this(new int[1024]);
  }

  void clear() {
    this.selSize = 0;
  }

  void selectAll(Selected src) {
    System.arraycopy(src.sel, 0, this.sel, 0, src.selSize);
    this.selSize = src.selSize;
  }

  /**
   * Initialize the selected vector from the supplied filter context
   *
   * @param fc Input filterContext
   */
  void initialize(OrcFilterContext fc) {
    ensureSize(fc.getSelectedSize());
    selSize = fc.getSelectedSize();

    if (fc.isSelectedInUse()) {
      System.arraycopy(fc.getSelected(), 0, sel, 0, selSize);
    } else {
      for (int i = 0; i < selSize; i++) {
        sel[i] = i;
      }
    }
  }

  /**
   * Only adjust the size and don't worry about the state, if required this is handled before
   * this is
   * called.
   *
   * @param size Desired size
   */
  void ensureSize(int size) {
    if (size > sel.length) {
      sel = new int[size];
      selSize = 0;
    }
  }

  void set(Selected inBound) {
    ensureSize(inBound.selSize);
    System.arraycopy(inBound.sel, 0, sel, 0, inBound.selSize);
    selSize = inBound.selSize;
  }

  /**
   * Expects the elements the src to be disjoint with respect to this and is not validated.
   *
   * @param src The disjoint selection indices that should be merged into this.
   */
  void unionDisjoint(Selected src) {
    // merge from the back to avoid the need for an intermediate store
    int writeIdx = src.selSize + this.selSize - 1;
    int srcIdx = src.selSize - 1;
    int thisIdx = this.selSize - 1;

    while (thisIdx >= 0 || srcIdx >= 0) {
      if (srcIdx < 0 || (thisIdx >= 0 && src.sel[srcIdx] < this.sel[thisIdx])) {
        // src is exhausted or this is larger
        this.sel[writeIdx--] = this.sel[thisIdx--];
      } else {
        this.sel[writeIdx--] = src.sel[srcIdx--];
      }
    }
    this.selSize += src.selSize;
  }

  /**
   * Remove the elements of src from this.
   *
   * @param src The selection indices that should be removed from the current selection.
   */
  void minus(Selected src) {
    int writeidx = 0;
    int evalIdx = 0;
    int srcIdx = 0;
    while (srcIdx < src.selSize && evalIdx < this.selSize) {
      if (this.sel[evalIdx] < src.sel[srcIdx]) {
        // Evaluation is smaller so retain this
        this.sel[writeidx] = this.sel[evalIdx];
        evalIdx += 1;
        writeidx += 1;
      } else if (this.sel[evalIdx] > src.sel[srcIdx]) {
        // Evaluation is larger cannot decide, navigate src forward
        srcIdx += 1;
      } else {
        // Equal should be ignored so move both evalIdx and srcIdx forward
        evalIdx += 1;
        srcIdx += 1;
      }
    }
    if (evalIdx < this.selSize) {
      System.arraycopy(this.sel, evalIdx, this.sel, writeidx, this.selSize - evalIdx);
      writeidx += this.selSize - evalIdx;
    }
    this.selSize = writeidx;
  }
}
