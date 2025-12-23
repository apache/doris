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
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.impl.filter.LeafFilter;

import java.util.Arrays;
import java.util.List;

class FloatFilters {
  private FloatFilters() {
  }

  static class FloatBetween extends LeafFilter {
    private final double low;
    private final double high;

    FloatBetween(String colName, Object low, Object high, boolean negated) {
      super(colName, negated);
      this.low = (double) low;
      this.high = (double) high;
    }

    @Override
    protected boolean allow(ColumnVector v, int rowIdx) {
      return ((DoubleColumnVector) v).vector[rowIdx] >= low &&
          ((DoubleColumnVector) v).vector[rowIdx] <= high;
    }
  }

  static class FloatEquals extends LeafFilter {
    private final double aValue;

    FloatEquals(String colName, Object aValue, boolean negated) {
      super(colName, negated);
      this.aValue = (double) aValue;
    }

    @Override
    protected boolean allow(ColumnVector v, int rowIdx) {
      return ((DoubleColumnVector) v).vector[rowIdx] == aValue;
    }
  }

  static class FloatIn extends LeafFilter {
    private final double[] inValues;

    FloatIn(String colName, List<Object> values, boolean negated) {
      super(colName, negated);
      inValues = new double[values.size()];
      for (int i = 0; i < values.size(); i++) {
        inValues[i] = (double) values.get(i);
      }
      Arrays.sort(inValues);
    }

    @Override
    protected boolean allow(ColumnVector v, int rowIdx) {
      return Arrays.binarySearch(inValues, ((DoubleColumnVector) v).vector[rowIdx]) >= 0;
    }
  }

  static class FloatLessThan extends LeafFilter {
    private final double aValue;

    FloatLessThan(String colName, Object aValue, boolean negated) {
      super(colName, negated);
      this.aValue = (double) aValue;
    }

    @Override
    protected boolean allow(ColumnVector v, int rowIdx) {
      return ((DoubleColumnVector) v).vector[rowIdx] < aValue;
    }
  }

  static class FloatLessThanEquals extends LeafFilter {
    private final double aValue;

    FloatLessThanEquals(String colName, Object aValue, boolean negated) {
      super(colName, negated);
      this.aValue = (double) aValue;
    }

    @Override
    protected boolean allow(ColumnVector v, int rowIdx) {
      return ((DoubleColumnVector) v).vector[rowIdx] <= aValue;
    }
  }
}
