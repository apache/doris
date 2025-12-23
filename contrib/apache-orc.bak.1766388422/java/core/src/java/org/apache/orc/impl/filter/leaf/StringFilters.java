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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.orc.impl.filter.LeafFilter;
import org.apache.orc.util.CuckooSetBytes;

import java.nio.charset.StandardCharsets;
import java.util.List;

class StringFilters {
  private StringFilters() {
  }

  static class StringBetween extends LeafFilter {
    private final byte[] low;
    private final byte[] high;

    StringBetween(String colName, Object low, Object high, boolean negated) {
      super(colName, negated);
      this.low = ((String) low).getBytes(StandardCharsets.UTF_8);
      this.high = ((String) high).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    protected boolean allow(ColumnVector v, int rowIdx) {
      BytesColumnVector bv = (BytesColumnVector) v;
      return StringExpr.compare(bv.vector[rowIdx], bv.start[rowIdx], bv.length[rowIdx],
                                low, 0, low.length) >= 0 &&
             StringExpr.compare(bv.vector[rowIdx], bv.start[rowIdx], bv.length[rowIdx],
                                high, 0, high.length) <= 0;
    }
  }

  static class StringEquals extends LeafFilter {
    private final byte[] aValue;

    StringEquals(String colName, Object aValue, boolean negated) {
      super(colName, negated);
      this.aValue = ((String) aValue).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    protected boolean allow(ColumnVector v, int rowIdx) {
      BytesColumnVector bv = (BytesColumnVector) v;
      return StringExpr.equal(aValue, 0, aValue.length,
                              bv.vector[rowIdx], bv.start[rowIdx], bv.length[rowIdx]);
    }
  }

  static class StringIn extends LeafFilter {
    // The set object containing the IN list. This is optimized for lookup
    // of the data type of the column.
    private final CuckooSetBytes inSet;

    StringIn(String colName, List<Object> values, boolean negated) {
      super(colName, negated);
      final byte[][] inValues = new byte[values.size()][];
      for (int i = 0; i < values.size(); i++) {
        inValues[i] = ((String) values.get(i)).getBytes(StandardCharsets.UTF_8);
      }
      inSet = new CuckooSetBytes(inValues.length);
      inSet.load(inValues);
    }

    @Override
    protected boolean allow(ColumnVector v, int rowIdx) {
      BytesColumnVector bv = (BytesColumnVector) v;
      return inSet.lookup(bv.vector[rowIdx], bv.start[rowIdx], bv.length[rowIdx]);
    }
  }

  static class StringLessThan extends LeafFilter {
    private final byte[] aValue;

    StringLessThan(String colName, Object aValue, boolean negated) {
      super(colName, negated);
      this.aValue = ((String) aValue).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    protected boolean allow(ColumnVector v, int rowIdx) {
      BytesColumnVector bv = (BytesColumnVector) v;
      return StringExpr.compare(bv.vector[rowIdx], bv.start[rowIdx], bv.length[rowIdx],
                                aValue, 0, aValue.length) < 0;
    }
  }

  static class StringLessThanEquals extends LeafFilter {
    private final byte[] aValue;

    StringLessThanEquals(String colName, Object aValue, boolean negated) {
      super(colName, negated);
      this.aValue = ((String) aValue).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    protected boolean allow(ColumnVector v, int rowIdx) {
      BytesColumnVector bv = (BytesColumnVector) v;
      return StringExpr.compare(bv.vector[rowIdx], bv.start[rowIdx], bv.length[rowIdx],
                                aValue, 0, aValue.length) <= 0;
    }
  }
}
