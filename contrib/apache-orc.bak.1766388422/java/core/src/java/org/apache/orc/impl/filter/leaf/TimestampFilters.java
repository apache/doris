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
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.orc.impl.filter.LeafFilter;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class TimestampFilters {
  private TimestampFilters() {
  }


  static class TimestampBetween extends LeafFilter {
    private final Timestamp low;
    private final Timestamp high;

    TimestampBetween(String colName, Object low, Object high, boolean negated) {
      super(colName, negated);
      this.low = (Timestamp) low;
      this.high = (Timestamp) high;
    }

    @Override
    protected boolean allow(ColumnVector v, int rowIdx) {
      return ((TimestampColumnVector) v).compareTo(rowIdx, low) >= 0 &&
             ((TimestampColumnVector) v).compareTo(rowIdx, high) <= 0;
    }
  }

  static class TimestampEquals extends LeafFilter {
    private final Timestamp aValue;

    TimestampEquals(String colName, Object aValue, boolean negated) {
      super(colName, negated);
      this.aValue = (Timestamp) aValue;
    }

    @Override
    protected boolean allow(ColumnVector v, int rowIdx) {
      return ((TimestampColumnVector) v).compareTo(rowIdx, aValue) == 0;
    }
  }

  static class TimestampIn extends LeafFilter {
    private final Set<Timestamp> inValues;

    TimestampIn(String colName, List<Object> values, boolean negated) {
      super(colName, negated);
      inValues = new HashSet<>(values.size());
      for (Object value : values) {
        inValues.add((Timestamp) value);
      }
    }

    @Override
    protected boolean allow(ColumnVector v, int rowIdx) {
      return inValues.contains(((TimestampColumnVector) v).asScratchTimestamp(rowIdx));
    }
  }

  static class TimestampLessThan extends LeafFilter {
    private final Timestamp aValue;

    TimestampLessThan(String colName, Object aValue, boolean negated) {
      super(colName, negated);
      this.aValue = (Timestamp) aValue;
    }

    @Override
    protected boolean allow(ColumnVector v, int rowIdx) {
      return ((TimestampColumnVector) v).compareTo(rowIdx, aValue) < 0;
    }
  }

  static class TimestampLessThanEquals extends LeafFilter {
    private final Timestamp aValue;

    TimestampLessThanEquals(String colName, Object aValue, boolean negated) {
      super(colName, negated);
      this.aValue = (Timestamp) aValue;
    }

    @Override
    protected boolean allow(ColumnVector v, int rowIdx) {
      return ((TimestampColumnVector) v).compareTo(rowIdx, aValue) <= 0;
    }
  }
}
