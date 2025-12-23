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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.orc.OrcFilterContext;

public abstract class LeafFilter implements VectorFilter {
  public String getColName() {
    return colName;
  }

  private final String colName;
  private final boolean negated;

  protected LeafFilter(String colName, boolean negated) {
    this.colName = colName;
    this.negated = negated;
  }

  @Override
  public void filter(OrcFilterContext fc,
                     Selected bound,
                     Selected selOut) {
    ColumnVector[] branch = fc.findColumnVector(colName);
    ColumnVector v = branch[branch.length - 1];
    boolean noNulls = OrcFilterContext.noNulls(branch);
    int currSize = 0;
    int rowIdx;

    if (v.isRepeating) {
      if (!OrcFilterContext.isNull(branch, 0) && allowWithNegation(v, 0)) {
        // If the repeating value is allowed then allow the current selSize
        for (int i = 0; i < bound.selSize; i++) {
          rowIdx = bound.sel[i];
          selOut.sel[currSize++] = rowIdx;
        }
      }
    } else if (noNulls) {
      for (int i = 0; i < bound.selSize; i++) {
        rowIdx = bound.sel[i];

        // Check the value
        if (allowWithNegation(v, rowIdx)) {
          selOut.sel[currSize++] = rowIdx;
        }
      }
    } else {
      for (int i = 0; i < bound.selSize; i++) {
        rowIdx = bound.sel[i];

        // Check the value only if not null
        if (!OrcFilterContext.isNull(branch, rowIdx) &&
            allowWithNegation(v, rowIdx)) {
          selOut.sel[currSize++] = rowIdx;
        }
      }
    }

    selOut.selSize = currSize;
  }

  private boolean allowWithNegation(ColumnVector v, int rowIdx) {
    return allow(v, rowIdx) != negated;
  }

  protected abstract boolean allow(ColumnVector v, int rowIdx);
}
