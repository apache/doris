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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.orc.impl.filter.LeafFilter;

import java.util.Arrays;
import java.util.List;

class LongFilters {
  private LongFilters() {
  }

  static class LongBetween extends LeafFilter {
    private final long low;
    private final long high;

    LongBetween(String colName, Object low, Object high, boolean negated) {
      super(colName, negated);
      this.low = (long) low;
      this.high = (long) high;
    }

    @Override
    protected boolean allow(ColumnVector v, int rowIdx) {
      return ((LongColumnVector) v).vector[rowIdx] >= low &&
             ((LongColumnVector) v).vector[rowIdx] <= high;
    }
  }

  static class LongEquals extends LeafFilter {
    private final long aValue;

    LongEquals(String colName, Object aValue, boolean negated) {
      super(colName, negated);
      this.aValue = (long) aValue;
    }

    @Override
    protected boolean allow(ColumnVector v, int rowIdx) {
      return ((LongColumnVector) v).vector[rowIdx] == aValue;
    }
  }

  static class LongIn extends LeafFilter {
    private final long[] inValues;

    LongIn(String colName, List<Object> values, boolean negated) {
      super(colName, negated);
      inValues = new long[values.size()];
      for (int i = 0; i < values.size(); i++) {
        inValues[i] = (long) values.get(i);
      }
      Arrays.sort(inValues);
    }

    @Override
    protected boolean allow(ColumnVector v, int rowIdx) {
      return Arrays.binarySearch(inValues, ((LongColumnVector) v).vector[rowIdx]) >= 0;
    }
  }

  static class LongLessThan extends LeafFilter {
    private final long aValue;

    LongLessThan(String colName, Object aValue, boolean negated) {
      super(colName, negated);
      this.aValue = (long) aValue;
    }

    @Override
    protected boolean allow(ColumnVector v, int rowIdx) {
      return ((LongColumnVector) v).vector[rowIdx] < aValue;
    }
  }

  static class LongLessThanEquals extends LeafFilter {
    private final long aValue;

    LongLessThanEquals(String colName, Object aValue, boolean negated) {
      super(colName, negated);
      this.aValue = (long) aValue;
    }

    @Override
    protected boolean allow(ColumnVector v, int rowIdx) {
      return ((LongColumnVector) v).vector[rowIdx] <= aValue;
    }
  }
}
