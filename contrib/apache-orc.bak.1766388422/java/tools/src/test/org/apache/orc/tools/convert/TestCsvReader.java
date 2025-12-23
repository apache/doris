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

package org.apache.orc.tools.convert;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.util.Locale;

import static org.apache.orc.tools.convert.ConvertTool.DEFAULT_TIMESTAMP_FORMAT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCsvReader {

  Locale defaultLocale;

  @BeforeEach
  public void storeDefaultLocale() {
    defaultLocale = Locale.getDefault();
    Locale.setDefault(Locale.US);
  }

  @AfterEach
  public void restoreDefaultLocale() {
    Locale.setDefault(defaultLocale);
  }

  @Test
  public void testSimple() throws Exception {
    // yyyy[[-][/]]MM[[-][/]]dd[['T'][ ]]HH:mm:ss[ ][XXX][X]
    StringReader input = new StringReader(
        "1,1.25,1.01,'a',f,'2000-01-01T00:00:00+00:00'\n" +
        "2,2.5,2.02,'14',t,'2000/01/01T00:00:00+00'\n" +
        "3,3.75,3.03,'1e',false,'2000-01-01T00:00:00Z'\n" +
        "4,5,4.04,'28',true,'2000-01-01 00:00:00+00'\n" +
        "5,6.25,5.05,'32',0,'2000-01-01 00:00:00-00'\n" +
        "6,7.5,6.06,'3c',1,'2000-01-01T04:00:00+04'\n" +
        "7,8.75,7.07,'46',2,'1999-12-31T20:00:00-04:00'\n" +
        "8,10,8.08,'50',t,'2000-01-01T00:00:00+00'\n"
    );
    TypeDescription schema = TypeDescription.fromString(
        "struct<a:int,b:double,c:decimal(10,2),d:string,e:boolean,e:timestamp>");
    RecordReader reader = new CsvReader(input, null, 1, schema, ',', '\'',
        '\\', 0, "", DEFAULT_TIMESTAMP_FORMAT);
    VectorizedRowBatch batch = schema.createRowBatch(5);
    assertTrue(reader.nextBatch(batch));
    assertEquals(5, batch.size);
    long bool = 0;
    for(int r = 0; r < batch.size; ++r) {
      assertEquals(r+1, ((LongColumnVector) batch.cols[0]).vector[r]);
      assertEquals(1.25 * (r + 1), ((DoubleColumnVector) batch.cols[1]).vector[r], 0.001);
      assertEquals((r + 1) + ".0" + (r + 1), ((DecimalColumnVector) batch.cols[2]).vector[r].toFormatString(2));
      assertEquals(Integer.toHexString((r + 1) * 10), ((BytesColumnVector) batch.cols[3]).toString(r));
      assertEquals(bool, ((LongColumnVector) batch.cols[4]).vector[r]);
      bool = 1 - bool;
      assertEquals(946684800000L, ((TimestampColumnVector) batch.cols[5]).getTime(r));
    }
    assertTrue(reader.nextBatch(batch));
    assertEquals(3, batch.size);
    for(int r = 0; r < batch.size; ++r) {
      assertEquals(r + 6, ((LongColumnVector) batch.cols[0]).vector[r]);
      assertEquals(1.25 * (r + 6), ((DoubleColumnVector) batch.cols[1]).vector[r], 0.001);
      assertEquals((r + 6) + ".0" + (r + 6), ((DecimalColumnVector) batch.cols[2]).vector[r].toFormatString(2));
      assertEquals(Integer.toHexString((r + 6) * 10), ((BytesColumnVector) batch.cols[3]).toString(r));
      assertEquals(bool, ((LongColumnVector) batch.cols[4]).vector[r]);
      bool = 1 - bool;
      assertEquals(946684800000L, ((TimestampColumnVector) batch.cols[5]).getTime(r));
    }
    assertFalse(reader.nextBatch(batch));
  }

  @Test
  public void testNulls() throws Exception {
    StringReader input = new StringReader(
        "1,1,1,'a'\n" +
        "'null','null','null','null'\n" +
        "3,3,3,'row 3'\n"
    );
    TypeDescription schema = TypeDescription.fromString(
        "struct<a:int,b:double,c:decimal(10,2),d:string>");
    RecordReader reader = new CsvReader(input, null, 1, schema, ',', '\'',
        '\\', 0, "null", DEFAULT_TIMESTAMP_FORMAT);
    VectorizedRowBatch batch = schema.createRowBatch();
    assertTrue(reader.nextBatch(batch));
    assertEquals(3, batch.size);
    for(int c=0; c < 4; ++c) {
      assertFalse(batch.cols[c].noNulls, "column " + c);
    }

    // check row 0
    assertEquals(1, ((LongColumnVector) batch.cols[0]).vector[0]);
    assertEquals(1, ((DoubleColumnVector) batch.cols[1]).vector[0], 0.001);
    assertEquals("1", ((DecimalColumnVector) batch.cols[2]).vector[0].toString());
    assertEquals("a", ((BytesColumnVector) batch.cols[3]).toString(0));
    for(int c=0; c < 4; ++c) {
      assertFalse(batch.cols[c].isNull[0], "column " + c);
    }

    // row 1
    for(int c=0; c < 4; ++c) {
      assertTrue(batch.cols[c].isNull[1], "column " + c);
    }

    // check row 2
    assertEquals(3, ((LongColumnVector) batch.cols[0]).vector[2]);
    assertEquals(3, ((DoubleColumnVector) batch.cols[1]).vector[2], 0.001);
    assertEquals("3", ((DecimalColumnVector) batch.cols[2]).vector[2].toString());
    assertEquals("row 3", ((BytesColumnVector) batch.cols[3]).toString(2));
    for(int c=0; c < 4; ++c) {
      assertFalse(batch.cols[c].isNull[2], "column " + c);
    }
  }

  @Test
  public void testStructs() throws Exception {
    StringReader input = new StringReader(
        "1,2,3,4\n" +
        "5,6,7,8\n"
    );
    TypeDescription schema = TypeDescription.fromString(
        "struct<a:int,b:struct<c:int,d:int>,e:int>");
    RecordReader reader = new CsvReader(input, null, 1, schema, ',', '\'',
        '\\', 0, "null", DEFAULT_TIMESTAMP_FORMAT);
    VectorizedRowBatch batch = schema.createRowBatch();
    assertTrue(reader.nextBatch(batch));
    assertEquals(2, batch.size);
    int nextVal = 1;
    for(int r=0; r < 2; ++r) {
      assertEquals(nextVal++, ((LongColumnVector) batch.cols[0]).vector[r], "row " + r);
      StructColumnVector b = (StructColumnVector) batch.cols[1];
      assertEquals(nextVal++, ((LongColumnVector) b.fields[0]).vector[r], "row " + r);
      assertEquals(nextVal++, ((LongColumnVector) b.fields[1]).vector[r], "row " + r);
      assertEquals(nextVal++, ((LongColumnVector) batch.cols[2]).vector[r], "row " + r);
    }
    assertFalse(reader.nextBatch(batch));
  }

  @Test
  public void testLargeNumbers() throws Exception {
    StringReader input = new StringReader(
            "2147483646,-2147483647,9223372036854775806,-9223372036854775807\n"
    );
    TypeDescription schema = TypeDescription.fromString(
            "struct<a:int,b:int,d:bigint,e:bigint>");
    RecordReader reader = new CsvReader(input, null, 1, schema, ',', '\'',
            '\\', 0, "null", DEFAULT_TIMESTAMP_FORMAT);
    VectorizedRowBatch batch = schema.createRowBatch();
    assertTrue(reader.nextBatch(batch));
    assertEquals(1, batch.size);
    assertEquals(2147483646, ((LongColumnVector) batch.cols[0]).vector[0]);
    assertEquals(-2147483647, ((LongColumnVector) batch.cols[1]).vector[0]);
    assertEquals(9223372036854775806L, ((LongColumnVector) batch.cols[2]).vector[0]);
    assertEquals(-9223372036854775807L, ((LongColumnVector) batch.cols[3]).vector[0]);
    assertFalse(reader.nextBatch(batch));
  }

  @Test
  public void testCustomTimestampFormat() throws Exception {
    String tsFormat = "d[d] MMM yyyy HH:mm:ss.SSSSSS";

    StringReader input = new StringReader(
            "'21 Mar 2018 12:23:34.123456'\n" +
                    "'3 Feb 2018 18:04:51.456789'\n"
    );
    TypeDescription schema = TypeDescription.fromString(
            "struct<a:timestamp>");
    RecordReader reader = new CsvReader(input, null, 1, schema, ',', '\'',
            '\\', 0, "", tsFormat);
    VectorizedRowBatch batch = schema.createRowBatch(2);
    assertTrue(reader.nextBatch(batch));
    assertEquals(2, batch.size);
    TimestampColumnVector cv = (TimestampColumnVector) batch.cols[0];
    assertEquals("2018-03-21 12:23:34.123456", cv.asScratchTimestamp(0).toString());
    assertEquals("2018-02-03 18:04:51.456789", cv.asScratchTimestamp(1).toString());
  }
}
