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
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.impl.filter.ATestFilter;
import org.junit.jupiter.api.BeforeEach;

import java.sql.Timestamp;
import java.util.stream.IntStream;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ATestLeafFilter extends ATestFilter {
  static final int lowIdx = 2;
  static final int highIdx = 4;
  static final int size = 6;

  @BeforeEach
  public void setup() {
    HiveDecimalWritable[] decValues = new HiveDecimalWritable[] {
      new HiveDecimalWritable(Long.MIN_VALUE + "100.01"),
      new HiveDecimalWritable(0),
      new HiveDecimalWritable(Long.MAX_VALUE + "100.01"),
      new HiveDecimalWritable(Long.MAX_VALUE + "101.00"),
      new HiveDecimalWritable(Long.MAX_VALUE + "101.01"),
      null};
    Timestamp[] tsValues = new Timestamp[] {
      createTimestamp(-100000, 55),
      createTimestamp(0, 0),
      createTimestamp(0, 1),
      createTimestamp(0, 2),
      createTimestamp(123456, 1),
      null
    };
    setBatch(new Long[] {1L, 2L, 3L, 4L, 5L, null},
             new String[] {"a", "b", "c", "d", "e", null},
             decValues,
             new Double[] {1.01, 2.0, 2.1, 3.55, 4.0, null},
             tsValues);
  }

  private Timestamp createTimestamp(long time, int nano) {
    Timestamp result = new Timestamp(0);
    result.setTime(time);
    result.setNanos(nano);
    return result;
  }

  protected Object getPredicateValue(PredicateLeaf.Type type, int idx) {
    switch (type) {
      case LONG:
        return ((LongColumnVector) batch.cols[0]).vector[idx];
      case STRING:
        BytesColumnVector bv = (BytesColumnVector) batch.cols[1];
        return new String(bv.vector[idx], bv.start[idx], bv.length[idx], UTF_8);
      case DECIMAL:
        return ((DecimalColumnVector) batch.cols[2]).vector[idx];
      case FLOAT:
        return ((DoubleColumnVector) batch.cols[3]).vector[idx];
      case TIMESTAMP:
        TimestampColumnVector tv = (TimestampColumnVector) batch.cols[4];
        Timestamp value = new Timestamp(0);
        value.setTime(tv.time[idx]);
        value.setNanos(tv.nanos[idx]);
        return value;
      default:
        throw new IllegalArgumentException(String.format("Type: %s is unsupported", type));
    }
  }

  protected void validateSelected(PredicateLeaf.Operator op, boolean not) {
    // Except for IS_NULL restrict the range to size - 1 as the last element is a null
    switch (op) {
      case EQUALS:
        validateSelected(IntStream.range(0, size - 1)
                           .filter(i -> not ^ (i == lowIdx))
                           .toArray());
        break;
      case LESS_THAN:
        validateSelected(IntStream.range(0, size - 1)
                           .filter(i -> not ^ (i < lowIdx))
                           .toArray());
        break;
      case LESS_THAN_EQUALS:
        validateSelected(IntStream.range(0, size - 1)
                           .filter(i -> not ^ (i <= lowIdx))
                           .toArray());
        break;
      case IN:
        validateSelected(IntStream.range(0, size - 1)
                           .filter(i -> not ^ (i == lowIdx || i == highIdx))
                           .toArray());
        break;
      case BETWEEN:
        validateSelected(IntStream.range(0, size - 1)
                           .filter(i -> not ^ (i >= lowIdx && i <= highIdx))
                           .toArray());
        break;
      case IS_NULL:
        validateSelected(IntStream.range(0, size)
                           .filter(i -> not ^ (i == 5))
                           .toArray());
        break;
      default:
        throw new IllegalArgumentException();
    }
  }
}
