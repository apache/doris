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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.OrcFilterContext;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.OrcFilterContextImpl;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ATestFilter {
  protected final TypeDescription schema = TypeDescription.createStruct()
    .addField("f1", TypeDescription.createLong())
    .addField("f2", TypeDescription.createString())
    .addField("f3", TypeDescription.createDecimal().withPrecision(38).withScale(2))
    .addField("f4", TypeDescription.createDouble())
    .addField("f5", TypeDescription.createTimestamp());
  protected final OrcFilterContextImpl fc = new OrcFilterContextImpl(schema, false);

  protected final VectorizedRowBatch batch = schema.createRowBatch();

  protected void setBatch(Long[] f1Values, String[] f2Values) {
    setBatch(f1Values, f2Values, null, null, null);
  }

  protected void setBatch(Long[] f1Values,
                          String[] f2Values,
                          HiveDecimalWritable[] f3Values,
                          Double[] f4Values,
                          Timestamp[] f5Values) {

    batch.reset();
    for (int i = 0; i < f1Values.length; i++) {
      setLong(f1Values[i], (LongColumnVector) batch.cols[0], i);
      setString(f2Values[i], (BytesColumnVector) batch.cols[1], i);
      if (f3Values != null) {
        setDecimal(f3Values[i], (DecimalColumnVector) batch.cols[2], i);
      }
      if (f4Values != null) {
        setDouble(f4Values[i], (DoubleColumnVector) batch.cols[3], i);
      }
      if (f5Values != null) {
        setTimestamp(f5Values[i], (TimestampColumnVector) batch.cols[4], i);
      }
    }

    batch.size = f1Values.length;
    fc.setBatch(batch);
  }

  private void setTimestamp(Timestamp value, TimestampColumnVector v, int idx) {
    if (value == null) {
      v.noNulls = false;
      v.isNull[idx] = true;
    } else {
      v.isNull[idx] = false;
      v.getScratchTimestamp().setTime(value.getTime());
      v.getScratchTimestamp().setNanos(value.getNanos());
      v.setFromScratchTimestamp(idx);
    }
  }

  private void setDouble(Double value, DoubleColumnVector v, int idx) {
    if (value == null) {
      v.noNulls = false;
      v.isNull[idx] = true;
    } else {
      v.isNull[idx] = false;
      v.vector[idx] = value;
    }
  }

  private void setDecimal(HiveDecimalWritable value, DecimalColumnVector v, int idx) {
    if (value == null) {
      v.noNulls = false;
      v.isNull[idx] = true;
    } else {
      v.isNull[idx] = false;
      v.vector[idx] = value;
    }
  }

  private void setString(String value, BytesColumnVector v, int idx) {
    if (value == null) {
      v.noNulls = false;
      v.isNull[idx] = true;
    } else {
      v.isNull[idx] = false;
      byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
      v.vector[idx] = bytes;
      v.start[idx] = 0;
      v.length[idx] = bytes.length;
    }
  }

  private void setLong(Long value, LongColumnVector v, int idx) {
    if (value == null) {
      v.noNulls = false;
      v.isNull[idx] = true;
    } else {
      v.isNull[idx] = false;
      v.vector[idx] = value;
    }
  }

  protected void validateSelected(int... v) {
    validateSelected(fc, v);
  }

  static void validateSelected(OrcFilterContext fc, int... v) {
    assertTrue(fc.isSelectedInUse());
    assertEquals(v.length, fc.getSelectedSize());
    assertArrayEquals(v, Arrays.copyOf(fc.getSelected(), v.length));
  }

  protected void validateAllSelected(int size) {
    validateAllSelected(fc, size);
  }

  static void validateAllSelected(OrcFilterContext fc, int size) {
    assertFalse(fc.isSelectedInUse());
    assertEquals(size, fc.getSelectedSize());
  }

  protected void validateNoneSelected() {
    validateNoneSelected(fc);
  }

  static void validateNoneSelected(OrcFilterContext fc) {
    assertTrue(fc.isSelectedInUse());
    assertEquals(0, fc.getSelectedSize());
  }

  protected void filter(VectorFilter filter) {
    BatchFilterFactory.create(filter, null).accept(fc);
  }
}
