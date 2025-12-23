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
package org.apache.orc;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.impl.RecordReaderImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;

import static org.apache.orc.TestVectorOrcFile.assertEmptyStats;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestOrcNullOptimization {

  TypeDescription createMyStruct() {
    return TypeDescription.createStruct()
        .addField("a", TypeDescription.createInt())
        .addField("b", TypeDescription.createString())
        .addField("c", TypeDescription.createBoolean())
        .addField("d", TypeDescription.createList(
            TypeDescription.createStruct()
                .addField("z", TypeDescription.createInt())));
  }

  void addRow(Writer writer, VectorizedRowBatch batch,
              Integer a, String b, Boolean c,
              Integer... d) throws IOException {
    if (batch.size == batch.getMaxSize()) {
      writer.addRowBatch(batch);
      batch.reset();
    }
    int row = batch.size++;
    LongColumnVector aColumn = (LongColumnVector) batch.cols[0];
    BytesColumnVector bColumn = (BytesColumnVector) batch.cols[1];
    LongColumnVector cColumn = (LongColumnVector) batch.cols[2];
    ListColumnVector dColumn = (ListColumnVector) batch.cols[3];
    StructColumnVector dStruct = (StructColumnVector) dColumn.child;
    LongColumnVector dInt = (LongColumnVector) dStruct.fields[0];
    if (a == null) {
      aColumn.noNulls = false;
      aColumn.isNull[row] = true;
    } else {
      aColumn.vector[row] = a;
    }
    if (b == null) {
      bColumn.noNulls = false;
      bColumn.isNull[row] = true;
    } else {
      bColumn.setVal(row, b.getBytes(StandardCharsets.UTF_8));
    }
    if (c == null) {
      cColumn.noNulls = false;
      cColumn.isNull[row] = true;
    } else {
      cColumn.vector[row] = c ? 1 : 0;
    }
    if (d == null) {
      dColumn.noNulls = false;
      dColumn.isNull[row] = true;
    } else {
      dColumn.offsets[row] = dColumn.childCount;
      dColumn.lengths[row] = d.length;
      dColumn.childCount += d.length;
      for(int e=0; e < d.length; ++e) {
        dInt.vector[(int) dColumn.offsets[row] + e] = d[e];
      }
    }
  }

  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  Configuration conf;
  FileSystem fs;
  Path testFilePath;

  @BeforeEach
  public void openFileSystem(TestInfo testInfo) throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestOrcNullOptimization." +
        testInfo.getTestMethod().get().getName() + ".orc");
    fs.delete(testFilePath, false);
  }

  @Test
  public void testMultiStripeWithNull() throws Exception {
    TypeDescription schema = createMyStruct();
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .setSchema(schema)
                                         .stripeSize(100000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(10000));
    Random rand = new Random(100);
    VectorizedRowBatch batch = schema.createRowBatch();
    addRow(writer, batch, null, null, true, 100);
    for (int i = 2; i < 20000; i++) {
      addRow(writer, batch, rand.nextInt(1), "a", true, 100);
    }
    addRow(writer, batch, null, null, true, 100);
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    // check the stats
    ColumnStatistics[] stats = reader.getStatistics();
    assertEquals(20000, reader.getNumberOfRows());
    assertEquals(20000, stats[0].getNumberOfValues());

    assertEquals(0, ((IntegerColumnStatistics) stats[1]).getMaximum());
    assertEquals(0, ((IntegerColumnStatistics) stats[1]).getMinimum());
    assertTrue(((IntegerColumnStatistics) stats[1]).isSumDefined());
    assertEquals(0, ((IntegerColumnStatistics) stats[1]).getSum());
    assertEquals("count: 19998 hasNull: true bytesOnDisk: 184 min: 0 max: 0 sum: 0",
        stats[1].toString());

    assertEquals("a", ((StringColumnStatistics) stats[2]).getMaximum());
    assertEquals("a", ((StringColumnStatistics) stats[2]).getMinimum());
    assertEquals(19998, stats[2].getNumberOfValues());
    assertEquals("count: 19998 hasNull: true bytesOnDisk: 200 min: a max: a sum: 19998",
        stats[2].toString());

    // check the inspectors
    assertEquals("struct<a:int,b:string,c:boolean,d:array<struct<z:int>>>",
        reader.getSchema().toString());

    RecordReader rows = reader.rows();

    List<Boolean> expected = Lists.newArrayList();
    for (StripeInformation sinfo : reader.getStripes()) {
      expected.add(false);
    }
    // only the first and last stripe will have PRESENT stream
    expected.set(0, true);
    expected.set(expected.size() - 1, true);

    List<Boolean> got = Lists.newArrayList();
    // check if the strip footer contains PRESENT stream
    for (StripeInformation sinfo : reader.getStripes()) {
      OrcProto.StripeFooter sf =
        ((RecordReaderImpl) rows).readStripeFooter(sinfo);
      got.add(sf.toString().indexOf(OrcProto.Stream.Kind.PRESENT.toString())
              != -1);
    }
    assertEquals(expected, got);

    batch = reader.getSchema().createRowBatch();
    LongColumnVector aColumn = (LongColumnVector) batch.cols[0];
    BytesColumnVector bColumn = (BytesColumnVector) batch.cols[1];
    LongColumnVector cColumn = (LongColumnVector) batch.cols[2];
    ListColumnVector dColumn = (ListColumnVector) batch.cols[3];
    LongColumnVector dElements =
        (LongColumnVector)(((StructColumnVector) dColumn.child).fields[0]);
    assertTrue(rows.nextBatch(batch));
    assertEquals(1024, batch.size);

    // row 1
    assertTrue(aColumn.isNull[0]);
    assertTrue(bColumn.isNull[0]);
    assertEquals(1, cColumn.vector[0]);
    assertEquals(0, dColumn.offsets[0]);
    assertEquals(1, dColumn.lengths[1]);
    assertEquals(100, dElements.vector[0]);

    rows.seekToRow(19998);
    rows.nextBatch(batch);
    assertEquals(2, batch.size);

    // last-1 row
    assertEquals(0, aColumn.vector[0]);
    assertEquals("a", bColumn.toString(0));
    assertEquals(1, cColumn.vector[0]);
    assertEquals(0, dColumn.offsets[0]);
    assertEquals(1, dColumn.lengths[0]);
    assertEquals(100, dElements.vector[0]);

    // last row
    assertTrue(aColumn.isNull[1]);
    assertTrue(bColumn.isNull[1]);
    assertEquals(1, cColumn.vector[1]);
    assertEquals(1, dColumn.offsets[1]);
    assertEquals(1, dColumn.lengths[1]);
    assertEquals(100, dElements.vector[1]);

    assertFalse(rows.nextBatch(batch));
    rows.close();
  }

  @Test
  public void testMultiStripeWithoutNull() throws Exception {
    TypeDescription schema = createMyStruct();
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .setSchema(schema)
                                         .stripeSize(100000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(10000));
    Random rand = new Random(100);
    int batchSize = 5000;
    VectorizedRowBatch batch = schema.createRowBatch(batchSize);
    ColumnStatistics[] writerStats = writer.getStatistics();
    assertEmptyStats(writerStats);
    int count = 0;
    for (int i = 1; i < 20000; i++) {
      addRow(writer, batch, rand.nextInt(1), "a", true, 100);
      count++;
      if (count % batchSize == 1) {
        writerStats = writer.getStatistics();
      } else {
        assertArrayEquals(writerStats, writer.getStatistics());
      }
    }
    addRow(writer, batch, 0, "b", true, 100);
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    // check the stats
    ColumnStatistics[] stats = reader.getStatistics();
    assertArrayEquals(stats, writer.getStatistics());
    assertEquals(20000, reader.getNumberOfRows());
    assertEquals(20000, stats[0].getNumberOfValues());

    assertEquals(0, ((IntegerColumnStatistics) stats[1]).getMaximum());
    assertEquals(0, ((IntegerColumnStatistics) stats[1]).getMinimum());
    assertTrue(((IntegerColumnStatistics) stats[1]).isSumDefined());
    assertEquals(0, ((IntegerColumnStatistics) stats[1]).getSum());
    assertEquals("count: 20000 hasNull: false bytesOnDisk: 160 min: 0 max: 0 sum: 0",
        stats[1].toString());

    assertEquals("b", ((StringColumnStatistics) stats[2]).getMaximum());
    assertEquals("a", ((StringColumnStatistics) stats[2]).getMinimum());
    assertEquals(20000, stats[2].getNumberOfValues());
    assertEquals("count: 20000 hasNull: false bytesOnDisk: 180 min: a max: b sum: 20000",
        stats[2].toString());

    // check the inspectors
    assertEquals("struct<a:int,b:string,c:boolean,d:array<struct<z:int>>>",
        reader.getSchema().toString());

    RecordReader rows = reader.rows();

    // none of the stripes will have PRESENT stream
    List<Boolean> expected = Lists.newArrayList();
    for (StripeInformation sinfo : reader.getStripes()) {
      expected.add(false);
    }

    List<Boolean> got = Lists.newArrayList();
    // check if the strip footer contains PRESENT stream
    for (StripeInformation sinfo : reader.getStripes()) {
      OrcProto.StripeFooter sf =
        ((RecordReaderImpl) rows).readStripeFooter(sinfo);
      got.add(sf.toString().indexOf(OrcProto.Stream.Kind.PRESENT.toString())
              != -1);
    }
    assertEquals(expected, got);

    rows.seekToRow(19998);

    batch = reader.getSchema().createRowBatch();
    LongColumnVector aColumn = (LongColumnVector) batch.cols[0];
    BytesColumnVector bColumn = (BytesColumnVector) batch.cols[1];
    LongColumnVector cColumn = (LongColumnVector) batch.cols[2];
    ListColumnVector dColumn = (ListColumnVector) batch.cols[3];
    LongColumnVector dElements =
        (LongColumnVector)(((StructColumnVector) dColumn.child).fields[0]);

    assertTrue(rows.nextBatch(batch));
    assertEquals(2, batch.size);

    // last-1 row
    assertEquals(0, aColumn.vector[0]);
    assertEquals("a", bColumn.toString(0));
    assertEquals(1, cColumn.vector[0]);
    assertEquals(0, dColumn.offsets[0]);
    assertEquals(1, dColumn.lengths[0]);
    assertEquals(100, dElements.vector[0]);

    // last row
    assertEquals(0, aColumn.vector[1]);
    assertEquals("b", bColumn.toString(1));
    assertEquals(1, cColumn.vector[1]);
    assertEquals(1, dColumn.offsets[1]);
    assertEquals(1, dColumn.lengths[1]);
    assertEquals(100, dElements.vector[1]);
    rows.close();
  }

  @Test
  public void testColumnsWithNullAndCompression() throws Exception {
    TypeDescription schema = createMyStruct();
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .setSchema(schema)
                                         .stripeSize(100000)
                                         .bufferSize(10000));
    VectorizedRowBatch batch = schema.createRowBatch();
    addRow(writer, batch, 3, "a", true, 100);
    addRow(writer, batch, null, "b", true, 100);
    addRow(writer, batch, 3, null, false, 100);
    addRow(writer, batch, 3, "d", true, 100);
    addRow(writer, batch, 2, "e", true, 100);
    addRow(writer, batch, 2, "f", true, 100);
    addRow(writer, batch, 2, "g", true, 100);
    addRow(writer, batch, 2, "h", true, 100);
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    // check the stats
    ColumnStatistics[] stats = reader.getStatistics();
    assertArrayEquals(stats, writer.getStatistics());
    assertEquals(8, reader.getNumberOfRows());
    assertEquals(8, stats[0].getNumberOfValues());

    assertEquals(3, ((IntegerColumnStatistics) stats[1]).getMaximum());
    assertEquals(2, ((IntegerColumnStatistics) stats[1]).getMinimum());
    assertTrue(((IntegerColumnStatistics) stats[1]).isSumDefined());
    assertEquals(17, ((IntegerColumnStatistics) stats[1]).getSum());
    assertEquals("count: 7 hasNull: true bytesOnDisk: 12 min: 2 max: 3 sum: 17",
        stats[1].toString());

    assertEquals("h", ((StringColumnStatistics) stats[2]).getMaximum());
    assertEquals("a", ((StringColumnStatistics) stats[2]).getMinimum());
    assertEquals(7, stats[2].getNumberOfValues());
    assertEquals("count: 7 hasNull: true bytesOnDisk: 20 min: a max: h sum: 7",
        stats[2].toString());

    // check the inspectors
    batch = reader.getSchema().createRowBatch();
    LongColumnVector aColumn = (LongColumnVector) batch.cols[0];
    BytesColumnVector bColumn = (BytesColumnVector) batch.cols[1];
    LongColumnVector cColumn = (LongColumnVector) batch.cols[2];
    ListColumnVector dColumn = (ListColumnVector) batch.cols[3];
    LongColumnVector dElements =
        (LongColumnVector)(((StructColumnVector) dColumn.child).fields[0]);
    assertEquals("struct<a:int,b:string,c:boolean,d:array<struct<z:int>>>",
        reader.getSchema().toString());

    RecordReader rows = reader.rows();
    // only the last strip will have PRESENT stream
    List<Boolean> expected = Lists.newArrayList();
    for (StripeInformation sinfo : reader.getStripes()) {
      expected.add(false);
    }
    expected.set(expected.size() - 1, true);

    List<Boolean> got = Lists.newArrayList();
    // check if the strip footer contains PRESENT stream
    for (StripeInformation sinfo : reader.getStripes()) {
      OrcProto.StripeFooter sf =
        ((RecordReaderImpl) rows).readStripeFooter(sinfo);
      got.add(sf.toString().indexOf(OrcProto.Stream.Kind.PRESENT.toString())
              != -1);
    }
    assertEquals(expected, got);

    assertTrue(rows.nextBatch(batch));
    assertEquals(8, batch.size);

    // row 1
    assertEquals(3, aColumn.vector[0]);
    assertEquals("a", bColumn.toString(0));
    assertEquals(1, cColumn.vector[0]);
    assertEquals(0, dColumn.offsets[0]);
    assertEquals(1, dColumn.lengths[0]);
    assertEquals(100, dElements.vector[0]);

    // row 2
    assertTrue(aColumn.isNull[1]);
    assertEquals("b", bColumn.toString(1));
    assertEquals(1, cColumn.vector[1]);
    assertEquals(1, dColumn.offsets[1]);
    assertEquals(1, dColumn.lengths[1]);
    assertEquals(100, dElements.vector[1]);

    // row 3
    assertEquals(3, aColumn.vector[2]);
    assertTrue(bColumn.isNull[2]);
    assertEquals(0, cColumn.vector[2]);
    assertEquals(2, dColumn.offsets[2]);
    assertEquals(1, dColumn.lengths[2]);
    assertEquals(100, dElements.vector[2]);

    rows.close();
  }
}
