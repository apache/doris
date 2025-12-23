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

interface RowFilter {
  boolean accept(OrcFilterContext batch, int rowIdx);

  class LeafFilter extends org.apache.orc.impl.filter.LeafFilter implements RowFilter {
    final org.apache.orc.impl.filter.LeafFilter filter;

    LeafFilter(org.apache.orc.impl.filter.LeafFilter filter) {
      super(filter.getColName(), false);
      this.filter = filter;
    }

    @Override
    public boolean accept(OrcFilterContext batch, int rowIdx) {
      ColumnVector[] branch = batch.findColumnVector(filter.getColName());
      ColumnVector v = branch[branch.length - 1];
      boolean noNulls = OrcFilterContext.noNulls(branch);
      int idx = rowIdx;
      if (v.isRepeating) {
        idx = 0;
      }
      if (noNulls || !OrcFilterContext.isNull(branch, idx)) {
        return allow(v, idx);
      } else {
        return false;
      }
    }

    @Override
    protected boolean allow(ColumnVector v, int rowIdx) {
      return filter.allow(v,rowIdx);
    }
  }

  class OrFilter implements RowFilter {
    final RowFilter[] filters;

    OrFilter(RowFilter[] filters) {
      this.filters = filters;
    }

    @Override
    public boolean accept(OrcFilterContext batch, int rowIdx) {
      boolean result = true;
      for (RowFilter filter : filters) {
        result = filter.accept(batch, rowIdx);
        if (result) {
          break;
        }
      }
      return result;
    }
  }

  class AndFilter implements RowFilter {
    final RowFilter[] filters;

    AndFilter(RowFilter[] filters) {
      this.filters = filters;
    }

    @Override
    public boolean accept(OrcFilterContext batch, int rowIdx) {
      boolean result = true;
      for (RowFilter filter : filters) {
        result = filter.accept(batch, rowIdx);
        if (!result) {
          break;
        }
      }
      return result;
    }
  }
}
