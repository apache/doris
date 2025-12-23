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

public class IsNullFilter implements VectorFilter {

  private final String colName;

  public IsNullFilter(String colName) {
    this.colName = colName;
  }

  @Override
  public void filter(OrcFilterContext fc,
                     Selected bound,
                     Selected selOut) {
    ColumnVector[] branch = fc.findColumnVector(colName);
    ColumnVector v = branch[branch.length - 1];
    boolean noNulls = OrcFilterContext.noNulls(branch);

    // If the vector does not have nulls then none of them are selected and nothing to do
    if (!noNulls) {
      if (v.isRepeating && OrcFilterContext.isNull(branch, 0)) {
        // If the repeating vector is null then set all as selected.
        selOut.selectAll(bound);
      } else {
        int currSize = 0;
        int rowIdx;
        for (int i = 0; i < bound.selSize; i++) {
          // Identify the rowIdx from the selected vector
          rowIdx = bound.sel[i];

          if (OrcFilterContext.isNull(branch, rowIdx)) {
            selOut.sel[currSize++] = rowIdx;
          }
        }
        selOut.selSize = currSize;
      }
    }
  }
}
