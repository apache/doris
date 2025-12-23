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
import com.google.common.primitives.Longs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.sql.Timestamp;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestNewIntegerEncoding {

  private static Stream<Arguments> data() {
    return Stream.of(
        Arguments.of(OrcFile.EncodingStrategy.COMPRESSION),
        Arguments.of(OrcFile.EncodingStrategy.SPEED));
  }

  public static TypeDescription getRowSchema() {
    return TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("long1", TypeDescription.createLong());
  }

  public static void appendRow(VectorizedRowBatch batch,
                               int int1, long long1) {
    int row = batch.size++;
    ((LongColumnVector) batch.cols[0]).vector[row] = int1;
    ((LongColumnVector) batch.cols[1]).vector[row] = long1;
  }

  public static void appendLong(VectorizedRowBatch batch,
                                long long1) {
    int row = batch.size++;
    ((LongColumnVector) batch.cols[0]).vector[row] = long1;
  }

  Path workDir = new Path(System.getProperty("test.tmp.dir", "target"
      + File.separator + "test" + File.separator + "tmp"));

  Configuration conf;
  FileSystem fs;
  Path testFilePath;

  @BeforeEach
  public void openFileSystem(TestInfo testInfo) throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestOrcFile."
        + testInfo.getTestMethod().get().getName() + ".orc");
    fs.delete(testFilePath, false);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testBasicRow(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema= getRowSchema();
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .setSchema(schema)
                                         .stripeSize(100000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(10000)
                                         .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch();
    appendRow(batch, 111, 1111L);
    appendRow(batch, 111, 1111L);
    appendRow(batch, 111, 1111L);
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(111, ((LongColumnVector) batch.cols[0]).vector[r]);
        assertEquals(1111, ((LongColumnVector) batch.cols[1]).vector[r]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testBasicOld(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createLong();
    long[] inp = new long[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 3, 4, 5, 6,
        7, 8, 9, 10, 1, 1, 1, 1, 1, 1, 10, 9, 7, 6, 5, 4, 3, 2, 1, 1, 1, 1, 1,
        2, 5, 1, 3, 7, 1, 9, 2, 6, 3, 7, 1, 9, 2, 6, 3, 7, 1, 9, 2, 6, 3, 7, 1,
        9, 2, 6, 3, 7, 1, 9, 2, 6, 2000, 2, 1, 1, 1, 1, 1, 3, 7, 1, 9, 2, 6, 1,
        1, 1, 1, 1 };
    List<Long> input = Lists.newArrayList(Longs.asList(inp));
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .setSchema(schema)
                                         .compress(CompressionKind.NONE)
                                         .version(OrcFile.Version.V_0_11)
                                         .bufferSize(10000)
                                         .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch();
    for(Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    batch = reader.getSchema().createRowBatch();
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testBasicNew(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    long[] inp = new long[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 3, 4, 5, 6,
        7, 8, 9, 10, 1, 1, 1, 1, 1, 1, 10, 9, 7, 6, 5, 4, 3, 2, 1, 1, 1, 1, 1,
        2, 5, 1, 3, 7, 1, 9, 2, 6, 3, 7, 1, 9, 2, 6, 3, 7, 1, 9, 2, 6, 3, 7, 1,
        9, 2, 6, 3, 7, 1, 9, 2, 6, 2000, 2, 1, 1, 1, 1, 1, 3, 7, 1, 9, 2, 6, 1,
        1, 1, 1, 1 };
    List<Long> input = Lists.newArrayList(Longs.asList(inp));

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .compress(CompressionKind.NONE)
            .bufferSize(10000)
            .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch();
    for(Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    batch = reader.getSchema().createRowBatch();
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testBasicDelta1(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    long[] inp = new long[] { -500, -400, -350, -325, -310 };
    List<Long> input = Lists.newArrayList(Longs.asList(inp));

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .compress(CompressionKind.NONE)
            .bufferSize(10000)
            .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch();
    for(Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testBasicDelta2(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    long[] inp = new long[] { -500, -600, -650, -675, -710 };
    List<Long> input = Lists.newArrayList(Longs.asList(inp));

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .compress(CompressionKind.NONE)
            .bufferSize(10000)
            .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch();
    for(Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testBasicDelta3(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    long[] inp = new long[] { 500, 400, 350, 325, 310 };
    List<Long> input = Lists.newArrayList(Longs.asList(inp));

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .compress(CompressionKind.NONE)
            .bufferSize(10000)
            .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch();
    for(Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testBasicDelta4(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    long[] inp = new long[] { 500, 600, 650, 675, 710 };
    List<Long> input = Lists.newArrayList(Longs.asList(inp));

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .compress(CompressionKind.NONE)
            .bufferSize(10000)
            .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch();
    for(Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @Test
  public void testDeltaOverflow() throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    long[] inp = new long[]{4513343538618202719l, 4513343538618202711l,
        2911390882471569739l,
        -9181829309989854913l};
    List<Long> input = Lists.newArrayList(Longs.asList(inp));

    Writer writer = OrcFile.createWriter(
        testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
            .compress(CompressionKind.NONE).bufferSize(10000));
    VectorizedRowBatch batch = schema.createRowBatch();
    for (Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile
        .createReader(testFilePath, OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @Test
  public void testDeltaOverflow2() throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    long[] inp = new long[]{Long.MAX_VALUE, 4513343538618202711l,
        2911390882471569739l,
        Long.MIN_VALUE};
    List<Long> input = Lists.newArrayList(Longs.asList(inp));

    Writer writer = OrcFile.createWriter(
        testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
            .compress(CompressionKind.NONE).bufferSize(10000));
    VectorizedRowBatch batch = schema.createRowBatch();
    for (Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile
        .createReader(testFilePath, OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @Test
  public void testDeltaOverflow3() throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    long[] inp = new long[]{-4513343538618202711l, -2911390882471569739l, -2,
        Long.MAX_VALUE};
    List<Long> input = Lists.newArrayList(Longs.asList(inp));

    Writer writer = OrcFile.createWriter(
        testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
            .compress(CompressionKind.NONE).bufferSize(10000));
    VectorizedRowBatch batch = schema.createRowBatch();
    for (Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile
        .createReader(testFilePath, OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testIntegerMin(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    List<Long> input = Lists.newArrayList();
    input.add((long) Integer.MIN_VALUE);

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .bufferSize(10000)
            .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch();
    for(Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testIntegerMax(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    List<Long> input = Lists.newArrayList();
    input.add((long) Integer.MAX_VALUE);

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .compress(CompressionKind.NONE)
            .bufferSize(10000)
            .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch();
    for(Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testLongMin(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    List<Long> input = Lists.newArrayList();
    input.add(Long.MIN_VALUE);

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .compress(CompressionKind.NONE)
            .bufferSize(10000)
            .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch();
    for(Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testLongMax(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    List<Long> input = Lists.newArrayList();
    input.add(Long.MAX_VALUE);

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .compress(CompressionKind.NONE)
            .bufferSize(10000)
            .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch();
    for(Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testRandomInt(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    List<Long> input = Lists.newArrayList();
    Random rand = new Random();
    for(int i = 0; i < 100000; i++) {
      input.add((long) rand.nextInt());
    }

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .compress(CompressionKind.NONE)
            .bufferSize(10000)
            .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch(100000);
    for(Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testRandomLong(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    List<Long> input = Lists.newArrayList();
    Random rand = new Random();
    for(int i = 0; i < 100000; i++) {
      input.add(rand.nextLong());
    }

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .compress(CompressionKind.NONE)
            .bufferSize(10000)
            .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch(100000);
    for(Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testPatchedBaseNegativeMin(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    long[] inp = new long[] { 20, 2, 3, 2, 1, 3, 17, 71, 35, 2, 1, 139, 2, 2,
        3, 1783, 475, 2, 1, 1, 3, 1, 3, 2, 32, 1, 2, 3, 1, 8, 30, 1, 3, 414, 1,
        1, 135, 3, 3, 1, 414, 2, 1, 2, 2, 594, 2, 5, 6, 4, 11, 1, 2, 2, 1, 1,
        52, 4, 1, 2, 7, 1, 17, 334, 1, 2, 1, 2, 2, 6, 1, 266, 1, 2, 217, 2, 6,
        2, 13, 2, 2, 1, 2, 3, 5, 1, 2, 1, 7244, 11813, 1, 33, 2, -13, 1, 2, 3,
        13, 1, 92, 3, 13, 5, 14, 9, 141, 12, 6, 15, 25, 1, 1, 1, 46, 2, 1, 1,
        141, 3, 1, 1, 1, 1, 2, 1, 4, 34, 5, 78, 8, 1, 2, 2, 1, 9, 10, 2, 1, 4,
        13, 1, 5, 4, 4, 19, 5, 1, 1, 1, 68, 33, 399, 1, 1885, 25, 5, 2, 4, 1,
        1, 2, 16, 1, 2966, 3, 1, 1, 25501, 1, 1, 1, 66, 1, 3, 8, 131, 14, 5, 1,
        2, 2, 1, 1, 8, 1, 1, 2, 1, 5, 9, 2, 3, 112, 13, 2, 2, 1, 5, 10, 3, 1,
        1, 13, 2, 3, 4, 1, 3, 1, 1, 2, 1, 1, 2, 4, 2, 207, 1, 1, 2, 4, 3, 3, 2,
        2, 16 };
    List<Long> input = Lists.newArrayList(Longs.asList(inp));

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .compress(CompressionKind.NONE)
            .bufferSize(10000)
            .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch();
    for(Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testPatchedBaseNegativeMin2(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    long[] inp = new long[] { 20, 2, 3, 2, 1, 3, 17, 71, 35, 2, 1, 139, 2, 2,
        3, 1783, 475, 2, 1, 1, 3, 1, 3, 2, 32, 1, 2, 3, 1, 8, 30, 1, 3, 414, 1,
        1, 135, 3, 3, 1, 414, 2, 1, 2, 2, 594, 2, 5, 6, 4, 11, 1, 2, 2, 1, 1,
        52, 4, 1, 2, 7, 1, 17, 334, 1, 2, 1, 2, 2, 6, 1, 266, 1, 2, 217, 2, 6,
        2, 13, 2, 2, 1, 2, 3, 5, 1, 2, 1, 7244, 11813, 1, 33, 2, -1, 1, 2, 3,
        13, 1, 92, 3, 13, 5, 14, 9, 141, 12, 6, 15, 25, 1, 1, 1, 46, 2, 1, 1,
        141, 3, 1, 1, 1, 1, 2, 1, 4, 34, 5, 78, 8, 1, 2, 2, 1, 9, 10, 2, 1, 4,
        13, 1, 5, 4, 4, 19, 5, 1, 1, 1, 68, 33, 399, 1, 1885, 25, 5, 2, 4, 1,
        1, 2, 16, 1, 2966, 3, 1, 1, 25501, 1, 1, 1, 66, 1, 3, 8, 131, 14, 5, 1,
        2, 2, 1, 1, 8, 1, 1, 2, 1, 5, 9, 2, 3, 112, 13, 2, 2, 1, 5, 10, 3, 1,
        1, 13, 2, 3, 4, 1, 3, 1, 1, 2, 1, 1, 2, 4, 2, 207, 1, 1, 2, 4, 3, 3, 2,
        2, 16 };
    List<Long> input = Lists.newArrayList(Longs.asList(inp));

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .compress(CompressionKind.NONE)
            .bufferSize(10000)
            .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch();
    for(Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testPatchedBaseNegativeMin3(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    long[] inp = new long[] { 20, 2, 3, 2, 1, 3, 17, 71, 35, 2, 1, 139, 2, 2,
        3, 1783, 475, 2, 1, 1, 3, 1, 3, 2, 32, 1, 2, 3, 1, 8, 30, 1, 3, 414, 1,
        1, 135, 3, 3, 1, 414, 2, 1, 2, 2, 594, 2, 5, 6, 4, 11, 1, 2, 2, 1, 1,
        52, 4, 1, 2, 7, 1, 17, 334, 1, 2, 1, 2, 2, 6, 1, 266, 1, 2, 217, 2, 6,
        2, 13, 2, 2, 1, 2, 3, 5, 1, 2, 1, 7244, 11813, 1, 33, 2, 0, 1, 2, 3,
        13, 1, 92, 3, 13, 5, 14, 9, 141, 12, 6, 15, 25, 1, 1, 1, 46, 2, 1, 1,
        141, 3, 1, 1, 1, 1, 2, 1, 4, 34, 5, 78, 8, 1, 2, 2, 1, 9, 10, 2, 1, 4,
        13, 1, 5, 4, 4, 19, 5, 1, 1, 1, 68, 33, 399, 1, 1885, 25, 5, 2, 4, 1,
        1, 2, 16, 1, 2966, 3, 1, 1, 25501, 1, 1, 1, 66, 1, 3, 8, 131, 14, 5, 1,
        2, 2, 1, 1, 8, 1, 1, 2, 1, 5, 9, 2, 3, 112, 13, 2, 2, 1, 5, 10, 3, 1,
        1, 13, 2, 3, 4, 1, 3, 1, 1, 2, 1, 1, 2, 4, 2, 207, 1, 1, 2, 4, 3, 3, 2,
        2, 16 };
    List<Long> input = Lists.newArrayList(Longs.asList(inp));

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .compress(CompressionKind.NONE)
            .bufferSize(10000)
            .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch();
    for(Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testPatchedBaseNegativeMin4(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    long[] inp = new long[] { 13, 13, 11, 8, 13, 10, 10, 11, 11, 14, 11, 7, 13,
        12, 12, 11, 15, 12, 12, 9, 8, 10, 13, 11, 8, 6, 5, 6, 11, 7, 15, 10, 7,
        6, 8, 7, 9, 9, 11, 33, 11, 3, 7, 4, 6, 10, 14, 12, 5, 14, 7, 6 };
    List<Long> input = Lists.newArrayList(Longs.asList(inp));

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .compress(CompressionKind.NONE)
            .bufferSize(10000)
            .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch();
    for(Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testPatchedBaseAt0(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    List<Long> input = Lists.newArrayList();
    Random rand = new Random();
    for(int i = 0; i < 5120; i++) {
      input.add((long) rand.nextInt(100));
    }
    input.set(0, 20000L);

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .compress(CompressionKind.NONE)
            .bufferSize(10000)
            .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch(5120);
    for(Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testPatchedBaseAt1(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    List<Long> input = Lists.newArrayList();
    Random rand = new Random();
    for(int i = 0; i < 5120; i++) {
      input.add((long) rand.nextInt(100));
    }
    input.set(1, 20000L);

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .compress(CompressionKind.NONE)
            .bufferSize(10000)
            .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch(5120);
    for(Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testPatchedBaseAt255(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    List<Long> input = Lists.newArrayList();
    Random rand = new Random();
    for(int i = 0; i < 5120; i++) {
      input.add((long) rand.nextInt(100));
    }
    input.set(255, 20000L);

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .bufferSize(10000)
            .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch(5120);
    for(Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testPatchedBaseAt256(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    List<Long> input = Lists.newArrayList();
    Random rand = new Random();
    for(int i = 0; i < 5120; i++) {
      input.add((long) rand.nextInt(100));
    }
    input.set(256, 20000L);

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .bufferSize(10000)
            .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch(5120);
    for(Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testPatchedBase510(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    List<Long> input = Lists.newArrayList();
    Random rand = new Random();
    for(int i = 0; i < 5120; i++) {
      input.add((long) rand.nextInt(100));
    }
    input.set(510, 20000L);

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .bufferSize(10000)
            .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch(5120);
    for(Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testPatchedBase511(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    List<Long> input = Lists.newArrayList();
    Random rand = new Random();
    for(int i = 0; i < 5120; i++) {
      input.add((long) rand.nextInt(100));
    }
    input.set(511, 20000L);

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .bufferSize(10000)
            .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch(5120);
    for(Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testPatchedBaseMax1(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    List<Long> input = Lists.newArrayList();
    Random rand = new Random();
    for (int i = 0; i < 5120; i++) {
      input.add((long) rand.nextInt(60));
    }
    input.set(511, Long.MAX_VALUE);

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .bufferSize(10000)
            .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch(5120);
    for (Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testPatchedBaseMax2(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    List<Long> input = Lists.newArrayList();
    Random rand = new Random();
    for (int i = 0; i < 5120; i++) {
      input.add((long) rand.nextInt(60));
    }
    input.set(128, Long.MAX_VALUE);
    input.set(256, Long.MAX_VALUE);
    input.set(511, Long.MAX_VALUE);

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .bufferSize(10000)
            .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch(5120);
    for (Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testPatchedBaseMax3(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    List<Long> input = Lists.newArrayList();
    input.add(371946367L);
    input.add(11963367L);
    input.add(68639400007L);
    input.add(100233367L);
    input.add(6367L);
    input.add(10026367L);
    input.add(3670000L);
    input.add(3602367L);
    input.add(4719226367L);
    input.add(7196367L);
    input.add(444442L);
    input.add(210267L);
    input.add(21033L);
    input.add(160267L);
    input.add(400267L);
    input.add(23634347L);
    input.add(16027L);
    input.add(46026367L);
    input.add(Long.MAX_VALUE);
    input.add(33333L);

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .bufferSize(10000)
            .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch();
    for (Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testPatchedBaseMax4(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    List<Long> input = Lists.newArrayList();
    for (int i = 0; i < 25; i++) {
      input.add(371292224226367L);
      input.add(119622332222267L);
      input.add(686329400222007L);
      input.add(100233333222367L);
      input.add(636272333322222L);
      input.add(10202633223267L);
      input.add(36700222022230L);
      input.add(36023226224227L);
      input.add(47192226364427L);
      input.add(71963622222447L);
      input.add(22244444222222L);
      input.add(21220263327442L);
      input.add(21032233332232L);
      input.add(16026322232227L);
      input.add(40022262272212L);
      input.add(23634342227222L);
      input.add(16022222222227L);
      input.add(46026362222227L);
      input.add(46026362222227L);
      input.add(33322222222323L);
    }
    input.add(Long.MAX_VALUE);

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .bufferSize(10000)
            .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch();
    for (Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testPatchedBaseTimestamp(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createStruct()
        .addField("ts", TypeDescription.createTimestamp());

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .bufferSize(10000)
            .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch();

    List<Timestamp> tslist = Lists.newArrayList();
    tslist.add(Timestamp.valueOf("2099-01-01 00:00:00"));
    tslist.add(Timestamp.valueOf("2003-01-01 00:00:00"));
    tslist.add(Timestamp.valueOf("1999-01-01 00:00:00"));
    tslist.add(Timestamp.valueOf("1995-01-01 00:00:00"));
    tslist.add(Timestamp.valueOf("2002-01-01 00:00:00"));
    tslist.add(Timestamp.valueOf("2010-03-02 00:00:00"));
    tslist.add(Timestamp.valueOf("2005-01-01 00:00:00"));
    tslist.add(Timestamp.valueOf("2006-01-01 00:00:00"));
    tslist.add(Timestamp.valueOf("2003-01-01 00:00:00"));
    tslist.add(Timestamp.valueOf("1996-08-02 00:00:00"));
    tslist.add(Timestamp.valueOf("1998-11-02 00:00:00"));
    tslist.add(Timestamp.valueOf("2008-10-02 00:00:00"));
    tslist.add(Timestamp.valueOf("1993-08-02 00:00:00"));
    tslist.add(Timestamp.valueOf("2008-01-02 00:00:00"));
    tslist.add(Timestamp.valueOf("2007-01-01 00:00:00"));
    tslist.add(Timestamp.valueOf("2004-01-01 00:00:00"));
    tslist.add(Timestamp.valueOf("2008-10-02 00:00:00"));
    tslist.add(Timestamp.valueOf("2003-01-01 00:00:00"));
    tslist.add(Timestamp.valueOf("2004-01-01 00:00:00"));
    tslist.add(Timestamp.valueOf("2008-01-01 00:00:00"));
    tslist.add(Timestamp.valueOf("2005-01-01 00:00:00"));
    tslist.add(Timestamp.valueOf("1994-01-01 00:00:00"));
    tslist.add(Timestamp.valueOf("2006-01-01 00:00:00"));
    tslist.add(Timestamp.valueOf("2004-01-01 00:00:00"));
    tslist.add(Timestamp.valueOf("2001-01-01 00:00:00"));
    tslist.add(Timestamp.valueOf("2000-01-01 00:00:00"));
    tslist.add(Timestamp.valueOf("2000-01-01 00:00:00"));
    tslist.add(Timestamp.valueOf("2002-01-01 00:00:00"));
    tslist.add(Timestamp.valueOf("2006-01-01 00:00:00"));
    tslist.add(Timestamp.valueOf("2011-01-01 00:00:00"));
    tslist.add(Timestamp.valueOf("2002-01-01 00:00:00"));
    tslist.add(Timestamp.valueOf("2005-01-01 00:00:00"));
    tslist.add(Timestamp.valueOf("1974-01-01 00:00:00"));
    int idx = 0;
    for (Timestamp ts : tslist) {
      ((TimestampColumnVector) batch.cols[0]).set(idx++, ts);
    }
    batch.size = tslist.size();
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    idx = 0;
    while (rows.nextBatch(batch)) {
      assertEquals(tslist.size(), batch.size);
      for(int r=0; r < batch.size; ++r) {
        assertEquals(tslist.get(idx++),
            ((TimestampColumnVector) batch.cols[0]).asScratchTimestamp(r));
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testDirectLargeNegatives(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .bufferSize(10000)
            .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch();

    appendLong(batch, -7486502418706614742L);
    appendLong(batch, 0L);
    appendLong(batch, 1L);
    appendLong(batch, 1L);
    appendLong(batch, -5535739865598783616L);
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    assertTrue(rows.nextBatch(batch));
    assertEquals(5, batch.size);
    assertEquals(-7486502418706614742L,
        ((LongColumnVector) batch.cols[0]).vector[0]);
    assertEquals(0L,
        ((LongColumnVector) batch.cols[0]).vector[1]);
    assertEquals(1L,
        ((LongColumnVector) batch.cols[0]).vector[2]);
    assertEquals(1L,
        ((LongColumnVector) batch.cols[0]).vector[3]);
    assertEquals(-5535739865598783616L,
        ((LongColumnVector) batch.cols[0]).vector[4]);
    assertFalse(rows.nextBatch(batch));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testSeek(OrcFile.EncodingStrategy encodingStrategy) throws Exception {
    TypeDescription schema = TypeDescription.createLong();

    List<Long> input = Lists.newArrayList();
    Random rand = new Random();
    for(int i = 0; i < 100000; i++) {
      input.add((long) rand.nextInt());
    }
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .compress(CompressionKind.NONE)
            .stripeSize(100000)
            .bufferSize(10000)
            .version(OrcFile.Version.V_0_11)
            .encodingStrategy(encodingStrategy));
    VectorizedRowBatch batch = schema.createRowBatch(100000);
    for(Long l : input) {
      appendLong(batch, l);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    int idx = 55555;
    rows.seekToRow(idx);
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(input.get(idx++).longValue(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
      }
    }
  }
}
