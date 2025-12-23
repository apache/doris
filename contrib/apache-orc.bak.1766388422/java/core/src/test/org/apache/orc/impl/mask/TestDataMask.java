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
package org.apache.orc.impl.mask;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.DataMask;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestDataMask {

  @Test
  public void testNullFactory() throws Exception {
    TypeDescription schema = TypeDescription.fromString("struct<x:int>");
    // take the first column's type
    DataMask mask = DataMask.Factory.build(DataMask.Standard.NULLIFY.getDescription(),
        schema.findSubtype(1), (type) -> null);;
    assertEquals(NullifyMask.class.toString(), mask.getClass().toString());
    LongColumnVector cv = (LongColumnVector) schema.createRowBatch().cols[0];
    LongColumnVector masked = (LongColumnVector) schema.createRowBatch().cols[0];
    cv.vector[0] = 10;
    cv.vector[1] = 20;
    mask.maskData(cv, masked, 0, 2);
    assertTrue(masked.isRepeating);
    assertFalse(masked.noNulls);
    assertTrue(masked.isNull[0]);
  }

  @Test
  public void testRedactFactory() throws Exception {
    TypeDescription schema =
        TypeDescription.fromString("struct<s:struct<x:int,y:string>>");
    DataMask mask = DataMask.Factory.build(DataMask.Standard.REDACT.getDescription(),
        schema.findSubtype(1), (type) -> null);
    assertEquals(StructIdentity.class.toString(), mask.getClass().toString());
    StructColumnVector cv = (StructColumnVector)schema.createRowBatch().cols[0];
    StructColumnVector masked = (StructColumnVector)schema.createRowBatch().cols[0];
    LongColumnVector x = (LongColumnVector) cv.fields[0];
    BytesColumnVector y = (BytesColumnVector) cv.fields[1];
    x.vector[0] = 123;
    y.setVal(0, "Owen".getBytes(StandardCharsets.UTF_8));
    x.vector[1] = 456789;
    y.setVal(1, "ORC".getBytes(StandardCharsets.UTF_8));
    mask.maskData(cv, masked, 0, 2);
    x = (LongColumnVector) masked.fields[0];
    y = (BytesColumnVector) masked.fields[1];
    assertEquals(999, x.vector[0]);
    assertEquals(999999, x.vector[1]);
    assertEquals("Xxxx", y.toString(0));
    assertEquals("XXX", y.toString(1));
  }

  @Test
  public void testIdentityRedact() throws Exception {
    TypeDescription schema =
        TypeDescription.fromString("struct<s:struct<a:decimal(18,6),b:double," +
            "c:array<int>,d:map<timestamp,date>,e:uniontype<int,binary>,f:string>>");
    DataMask nullify =
        DataMask.Factory.build(DataMask.Standard.NULLIFY.getDescription(),
            schema.findSubtype(1), (type) -> null);
    // create a redact mask that passes everything though
    DataMask identity =
        DataMask.Factory.build(DataMask.Standard.REDACT
                                   .getDescription("__________", "_ _ _ _ _ _"),
            schema.findSubtype(1), (type) -> null);

    // allow easier access to fields
    StructColumnVector cv = (StructColumnVector)schema.createRowBatch().cols[0];
    StructColumnVector masked = (StructColumnVector)schema.createRowBatch().cols[0];
    DecimalColumnVector a = (DecimalColumnVector) cv.fields[0];
    DoubleColumnVector b = (DoubleColumnVector) cv.fields[1];
    ListColumnVector c = (ListColumnVector) cv.fields[2];
    LongColumnVector ce = (LongColumnVector) c.child;
    MapColumnVector d = (MapColumnVector) cv.fields[3];
    TimestampColumnVector dk = (TimestampColumnVector) d.keys;
    LongColumnVector dv = (LongColumnVector) d.values;
    UnionColumnVector e = (UnionColumnVector) cv.fields[4];
    LongColumnVector e1 = (LongColumnVector) e.fields[0];
    BytesColumnVector e2 = (BytesColumnVector) e.fields[1];
    BytesColumnVector f = (BytesColumnVector) cv.fields[5];

    // set up the input data
    for(int i=0; i < 3; ++i) {
      a.set(i, new HiveDecimalWritable((i + 1) + "." + (i + 1)));
      b.vector[i] = 1.25 * (i + 1);
      // layout c normally
      c.offsets[i] = i == 0 ? 0 : c.offsets[i-1] + c.lengths[i-1];
      c.lengths[i] = 2 * i;
      // layout d backward
      d.offsets[i] = 2 * (2 - i);
      d.lengths[i] = 2;
      e.tags[i] = i % 2;
      e1.vector[i] = i * 10;
      f.setVal(i, Integer.toHexString(0x123 * i).getBytes(StandardCharsets.UTF_8));
    }
    e2.setVal(1, "Foobar".getBytes(StandardCharsets.UTF_8));
    for(int i=0; i < 6; ++i) {
      ce.vector[i] = i;
      dk.time[i] = 1111 * i;
      dk.nanos[i] = 0;
      dv.vector[i] = i * 11;
    }

    // send it through the nullify mask
    nullify.maskData(cv, masked, 0, 3);
    assertFalse(masked.noNulls);
    assertTrue(masked.isRepeating);
    assertTrue(masked.isNull[0]);

    // send it through our identity mask
    identity.maskData(cv, masked, 0 , 3);
    assertTrue(masked.noNulls);
    assertFalse(masked.isRepeating);

    // point accessors to masked values
    a = (DecimalColumnVector) masked.fields[0];
    b = (DoubleColumnVector) masked.fields[1];
    c = (ListColumnVector) masked.fields[2];
    ce = (LongColumnVector) c.child;
    d = (MapColumnVector) masked.fields[3];
    dk = (TimestampColumnVector) d.keys;
    dv = (LongColumnVector) d.values;
    e = (UnionColumnVector) masked.fields[4];
    e1 = (LongColumnVector) e.fields[0];
    e2 = (BytesColumnVector) e.fields[1];
    f = (BytesColumnVector) masked.fields[5];

    // check the outputs
    for(int i=0; i < 3; ++i) {
      String msg = "iter " + i;
      assertEquals((i + 1) + "." + (i + 1), a.vector[i].toString(), msg);
      assertEquals(1.25 * (i + 1), b.vector[i], 0.0001, msg);
      assertEquals(i == 0 ? 0 : c.offsets[i-1] + c.lengths[i-1], c.offsets[i], msg);
      assertEquals(2 * i, c.lengths[i], msg);
      assertEquals(i == 0 ? 4 : d.offsets[i-1] - d.lengths[i], d.offsets[i], msg);
      assertEquals(2, d.lengths[i], msg);
      assertEquals(i % 2, e.tags[i], msg);
      assertEquals(Integer.toHexString(0x123 * i), f.toString(i), msg);
    }
    // check the subvalues for the list and map
    for(int i=0; i < 6; ++i) {
      String msg = "iter " + i;
      assertEquals(i, ce.vector[i], msg);
      assertEquals(i * 1111, dk.time[i], msg);
      assertEquals(i * 11, dv.vector[i], msg);
    }
    assertEquals(0, e1.vector[0]);
    assertEquals(20, e1.vector[2]);
    // the redact mask always replaces binary with null
    assertFalse(e2.noNulls);
    assertTrue(e2.isRepeating);
    assertTrue(e2.isNull[0]);
  }

}
