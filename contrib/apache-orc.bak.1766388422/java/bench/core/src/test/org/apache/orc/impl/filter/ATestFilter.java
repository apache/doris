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
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.DateUtils;
import org.apache.orc.impl.OrcFilterContextImpl;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ATestFilter {
  protected final TypeDescription schema = TypeDescription.createStruct()
    .addField("f1", TypeDescription.createLong())
    .addField("f2", TypeDescription.createString())
    .addField("f3p", TypeDescription.createDate())
    .addField("f3h", TypeDescription.createDate());
  protected final OrcFilterContextImpl fc = new OrcFilterContextImpl(schema, false);

  protected final VectorizedRowBatch batch = schema.createRowBatch();

  protected void setBatch(Long[] f1Values, String[] f2Values) {
    setBatch(f1Values, f2Values, new String[0]);
  }

  protected void setBatch(Long[] f1Values, String[] f2Values, String[] f3Values) {
    final LongColumnVector f1Vector = (LongColumnVector) batch.cols[0];
    final BytesColumnVector f2Vector = (BytesColumnVector) batch.cols[1];
    final DateColumnVector f3p = (DateColumnVector) batch.cols[2];
    final DateColumnVector f3h = (DateColumnVector) batch.cols[3];

    batch.reset();
    f1Vector.noNulls = true;
    for (int i =0; i < f1Values.length; i++) {
      if (f1Values[i] == null) {
        f1Vector.noNulls = false;
        f1Vector.isNull[i] = true;
      } else {
        f1Vector.isNull[i] = false;
        f1Vector.vector[i] = f1Values[i];
      }
    }

    for (int i = 0; i < f2Values.length; i++) {
      if (f2Values[i] == null) {
        f2Vector.noNulls = false;
        f2Vector.isNull[i] = true;
      } else {
        f2Vector.isNull[i] = false;
        byte[] bytes = f2Values[i].getBytes(StandardCharsets.UTF_8);
        f2Vector.vector[i] = bytes;
        f2Vector.start[i] = 0;
        f2Vector.length[i] = bytes.length;
      }
    }

    for (int i = 0; i < f3Values.length; i++) {
      if (f3Values[i] == null) {
        f3p.noNulls = false;
        f3p.isNull[i] = true;
        f3h.noNulls = false;
        f3h.isNull[i] = true;
      } else {
        f3p.isNull[i] = false;
        f3p.vector[i] = DateUtils.parseDate(f3Values[i], true);
        f3h.isNull[i] = false;
        f3h.vector[i] = DateUtils.parseDate(f3Values[i], false);
      }
    }
    batch.size = f1Values.length;
    fc.setBatch(batch);
  }

  protected void validateSelected(int... v) {
    assertTrue(fc.isSelectedInUse());
    assertEquals(v.length, fc.getSelectedSize());
    assertArrayEquals(v, Arrays.copyOf(fc.getSelected(), v.length));
  }

  protected void validateNoneSelected() {
    assertTrue(fc.isSelectedInUse());
    assertEquals(0, fc.getSelectedSize());
  }
}
