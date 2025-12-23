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

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.OrcFilterContextImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class TestConvFilter {
  private final int scale = 4;
  private final TypeDescription schema = TypeDescription.createStruct()
    .addField("f1", TypeDescription.createBoolean())
    .addField("f2", TypeDescription.createDate())
    .addField("f3", TypeDescription.createDecimal().withPrecision(18).withScale(scale));

  private final OrcFilterContextImpl fc = new OrcFilterContextImpl(schema, false);
  private final VectorizedRowBatch batch = schema.createRowBatchV2();

  @BeforeEach
  public void setup() throws ParseException {
    setBatch();
  }

  @Test
  public void testBooleanEquals() {
    // Equals
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .equals("f1", PredicateLeaf.Type.BOOLEAN, true)
      .build();

    FilterUtils.createVectorFilter(sArg, schema).accept(fc);
    ATestFilter.validateSelected(fc, 0, 3, 4);
  }

  @Test
  public void testBooleanIn() {
    // Equals
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .equals("f1", PredicateLeaf.Type.BOOLEAN, false)
      .build();

    FilterUtils.createVectorFilter(sArg, schema).accept(fc);
    ATestFilter.validateSelected(fc, 1, 2);
  }

  @Test
  public void testBooleanBetween() {
    // Equals
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startOr()
      .between("f1", PredicateLeaf.Type.BOOLEAN, false, true)
      .end()
      .build();

    FilterUtils.createVectorFilter(sArg, schema).accept(fc);
    ATestFilter.validateSelected(fc, 0, 1, 2, 3, 4);
  }

  @Test
  public void testDateEquals() throws ParseException {
    // Equals
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .equals("f2", PredicateLeaf.Type.DATE, date("2000-01-01"))
      .build();

    FilterUtils.createVectorFilter(sArg, schema).accept(fc);
    ATestFilter.validateSelected(fc, 1);
  }

  @Test
  public void testDateIn() throws ParseException {
    // Equals
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .in("f2", PredicateLeaf.Type.DATE, date("2000-01-01"), date("2100-06-07"))
      .build();

    FilterUtils.createVectorFilter(sArg, schema).accept(fc);
    ATestFilter.validateSelected(fc, 1, 4);
  }

  @Test
  public void testDateBetween() throws ParseException {
    // Equals
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startOr()
      .between("f2", PredicateLeaf.Type.DATE, date("2000-01-01"), date("2100-06-07"))
      .end()
      .build();

    FilterUtils.createVectorFilter(sArg, schema).accept(fc);
    ATestFilter.validateSelected(fc, 1, 2, 3, 4);
  }

  @Test
  public void testDecimalEquals() {
    // Equals
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .equals("f3", PredicateLeaf.Type.DECIMAL, decimal(12345678))
      .build();

    FilterUtils.createVectorFilter(sArg, schema).accept(fc);
    ATestFilter.validateSelected(fc, 2);
  }

  @Test
  public void testDecimalIn() {
    // Equals
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .in("f3", PredicateLeaf.Type.DECIMAL, decimal(0), decimal(Long.MAX_VALUE / 18))
      .build();

    FilterUtils.createVectorFilter(sArg, schema).accept(fc);
    ATestFilter.validateSelected(fc, 1, 3);
  }

  @Test
  public void testDecimalBetween() {
    // Equals
    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .startOr()
      .between("f3", PredicateLeaf.Type.DECIMAL, decimal(0), decimal(Long.MAX_VALUE / 18))
      .end()
      .build();

    FilterUtils.createVectorFilter(sArg, schema).accept(fc);
    ATestFilter.validateSelected(fc, 1, 2, 3);
  }

  protected void setBatch(Boolean[] f1Values, Date[] f2Values, HiveDecimalWritable[] f3Values) {
    batch.reset();
    for (int i = 0; i < f1Values.length; i++) {
      setBoolean(f1Values[i], (LongColumnVector) batch.cols[0], i);
      setDate(f2Values[i], (LongColumnVector) batch.cols[1], i);
      setDecimal(f3Values[i], (LongColumnVector) batch.cols[2], i);
    }

    batch.size = f1Values.length;
    fc.setBatch(batch);
  }

  private void setDecimal(HiveDecimalWritable value, LongColumnVector v, int idx) {
    if (value == null) {
      v.noNulls = false;
      v.isNull[idx] = true;
    } else {
      assert (HiveDecimalWritable.isPrecisionDecimal64(value.precision())
              && value.scale() <= scale);
      v.isNull[idx] = false;
      v.vector[idx] = value.serialize64(scale);
    }
  }

  private void setBoolean(Boolean value, LongColumnVector v, int idx) {
    if (value == null) {
      v.noNulls = false;
      v.isNull[idx] = true;
    } else {
      v.isNull[idx] = false;
      v.vector[idx] = value ? 1 : 0;
    }
  }

  private void setDate(Date value, LongColumnVector v, int idx) {
    if (value == null) {
      v.noNulls = false;
      v.isNull[idx] = true;
    } else {
      v.isNull[idx] = false;
      v.vector[idx] = value.toLocalDate().toEpochDay();
    }
  }

  private final SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");

  private Date date(String value) throws ParseException {
    return new Date(fmt.parse(value).getTime());
  }

  private HiveDecimalWritable decimal(long lValue) {
    return new HiveDecimalWritable(HiveDecimal.create(lValue, scale));
  }

  private void setBatch() throws ParseException {
    setBatch(new Boolean[] {true, false, false, true, true, null},
             new Date[] {
               date("1900-01-01"),
               date("2000-01-01"),
               date("2000-01-02"),
               date("2019-12-31"),
               date("2100-06-07"),
               null
             },
             new HiveDecimalWritable[] {
               new HiveDecimalWritable(HiveDecimal.create(Long.MIN_VALUE / 9, scale)),
               new HiveDecimalWritable(HiveDecimal.create(0, scale)),
               new HiveDecimalWritable(HiveDecimal.create(12345678, scale)),
               new HiveDecimalWritable(HiveDecimal.create(Long.MAX_VALUE / 18, scale)),
               new HiveDecimalWritable(HiveDecimal.create(Long.MAX_VALUE / 9, scale)),
               null
             });

  }
}
