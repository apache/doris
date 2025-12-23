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
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.orc.OrcFile.Version;
import org.apache.orc.OrcFile.WriterOptions;
import org.apache.orc.impl.DataReaderProperties;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.KeyProvider;
import org.apache.orc.impl.MemoryManagerImpl;
import org.apache.orc.impl.OrcCodecPool;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.ReaderImpl;
import org.apache.orc.impl.RecordReaderImpl;
import org.apache.orc.impl.RecordReaderUtils;
import org.apache.orc.impl.WriterImpl;
import org.apache.orc.impl.reader.ReaderEncryption;
import org.apache.orc.impl.reader.StripePlanner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;
import java.util.function.IntFunction;
import java.util.stream.Stream;

import static org.apache.orc.impl.ReaderImpl.DEFAULT_COMPRESSION_BLOCK_SIZE;
import static org.apache.orc.impl.mask.SHA256MaskFactory.printHexBinary;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests for the vectorized reader and writer for ORC files.
 */
public class TestVectorOrcFile {

  private static Stream<Arguments> data() {
    return Stream.of(
        Arguments.of(Version.V_0_11),
        Arguments.of(Version.V_0_12),
        Arguments.of(Version.UNSTABLE_PRE_2_0)
    );
  }

  public static String getFileFromClasspath(String name) {
    URL url = ClassLoader.getSystemResource(name);
    if (url == null) {
      throw new IllegalArgumentException("Could not find " + name);
    }
    return url.getPath();
  }

  public static class InnerStruct {
    int int1;
    Text string1 = new Text();
    InnerStruct(int int1, Text string1) {
      this.int1 = int1;
      this.string1.set(string1);
    }
    InnerStruct(int int1, String string1) {
      this.int1 = int1;
      this.string1.set(string1);
    }

    public String toString() {
      return "{" + int1 + ", " + string1 + "}";
    }
  }

  public static class MiddleStruct {
    List<InnerStruct> list = new ArrayList<InnerStruct>();

    MiddleStruct(InnerStruct... items) {
      list.clear();
      list.addAll(Arrays.asList(items));
    }
  }

  private static InnerStruct inner(int i, String s) {
    return new InnerStruct(i, s);
  }

  private static Map<String, InnerStruct> map(InnerStruct... items)  {
    Map<String, InnerStruct> result = new HashMap<String, InnerStruct>();
    for(InnerStruct i: items) {
      result.put(i.string1.toString(), i);
    }
    return result;
  }

  private static List<InnerStruct> list(InnerStruct... items) {
    List<InnerStruct> result = new ArrayList<InnerStruct>();
    result.addAll(Arrays.asList(items));
    return result;
  }

  protected static BytesWritable bytes(int... items) {
    BytesWritable result = new BytesWritable();
    result.setSize(items.length);
    for(int i=0; i < items.length; ++i) {
      result.getBytes()[i] = (byte) items[i];
    }
    return result;
  }

  protected static byte[] bytesArray(int... items) {
    byte[] result = new byte[items.length];
    for(int i=0; i < items.length; ++i) {
      result[i] = (byte) items[i];
    }
    return result;
  }

  private static ByteBuffer byteBuf(int... items) {
    ByteBuffer result = ByteBuffer.allocate(items.length);
    for(int item: items) {
      result.put((byte) item);
    }
    result.flip();
    return result;
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
    testFilePath = new Path(workDir, "TestVectorOrcFile." +
        testInfo.getTestMethod().get().getName().replaceFirst("\\[[0-9]+\\]", "")
        + "." + UUID.randomUUID() + ".orc");
    fs.delete(testFilePath, false);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testReadFormat_0_11(Version fileFormat) throws Exception {
    assumeTrue(fileFormat == Version.V_0_11);
    Path oldFilePath =
        new Path(getFileFromClasspath("orc-file-11-format.orc"));
    Reader reader = OrcFile.createReader(oldFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

    int stripeCount = 0;
    int rowCount = 0;
    long currentOffset = -1;
    for(StripeInformation stripe : reader.getStripes()) {
      stripeCount += 1;
      rowCount += stripe.getNumberOfRows();
      if (currentOffset < 0) {
        currentOffset = stripe.getOffset() + stripe.getIndexLength()
            + stripe.getDataLength() + stripe.getFooterLength();
      } else {
        assertEquals(currentOffset, stripe.getOffset());
        currentOffset += stripe.getIndexLength() + stripe.getDataLength()
            + stripe.getFooterLength();
      }
    }
    assertEquals(reader.getNumberOfRows(), rowCount);
    assertEquals(2, stripeCount);

    // check the stats
    ColumnStatistics[] stats = reader.getStatistics();
    assertEquals(7500, stats[1].getNumberOfValues());
    assertEquals(3750, ((BooleanColumnStatistics) stats[1]).getFalseCount());
    assertEquals(3750, ((BooleanColumnStatistics) stats[1]).getTrueCount());
    assertEquals("count: 7500 hasNull: true true: 3750", stats[1].toString());

    assertEquals(2048, ((IntegerColumnStatistics) stats[3]).getMaximum());
    assertEquals(1024, ((IntegerColumnStatistics) stats[3]).getMinimum());
    assertTrue(((IntegerColumnStatistics) stats[3]).isSumDefined());
    assertEquals(11520000, ((IntegerColumnStatistics) stats[3]).getSum());
    assertEquals("count: 7500 hasNull: true min: 1024 max: 2048 sum: 11520000",
        stats[3].toString());

    assertEquals(Long.MAX_VALUE,
        ((IntegerColumnStatistics) stats[5]).getMaximum());
    assertEquals(Long.MAX_VALUE,
        ((IntegerColumnStatistics) stats[5]).getMinimum());
    assertFalse(((IntegerColumnStatistics) stats[5]).isSumDefined());
    assertEquals(
        "count: 7500 hasNull: true min: 9223372036854775807 max: 9223372036854775807",
        stats[5].toString());

    assertEquals(-15.0, ((DoubleColumnStatistics) stats[7]).getMinimum(), 0.0001);
    assertEquals(-5.0, ((DoubleColumnStatistics) stats[7]).getMaximum(), 0.0001);
    assertEquals(-75000.0, ((DoubleColumnStatistics) stats[7]).getSum(),
        0.00001);
    assertEquals("count: 7500 hasNull: true min: -15.0 max: -5.0 sum: -75000.0",
        stats[7].toString());

    assertEquals("count: 7500 hasNull: true min: bye max: hi sum: 0", stats[9].toString());

    // check the inspectors
    TypeDescription schema = reader.getSchema();
    assertEquals(TypeDescription.Category.STRUCT, schema.getCategory());
    assertEquals("struct<boolean1:boolean,byte1:tinyint,short1:smallint,"
        + "int1:int,long1:bigint,float1:float,double1:double,bytes1:"
        + "binary,string1:string,middle:struct<list:array<struct<int1:int,"
        + "string1:string>>>,list:array<struct<int1:int,string1:string>>,"
        + "map:map<string,struct<int1:int,string1:string>>,ts:timestamp,"
        + "decimal1:decimal(38,10)>", schema.toString());
    VectorizedRowBatch batch = schema.createRowBatch();

    RecordReader rows = reader.rows();
    assertTrue(rows.nextBatch(batch));
    assertEquals(1024, batch.size);

    // check the contents of the first row
    assertFalse(getBoolean(batch, 0));
    assertEquals(1, getByte(batch, 0));
    assertEquals(1024, getShort(batch, 0));
    assertEquals(65536, getInt(batch, 0));
    assertEquals(Long.MAX_VALUE, getLong(batch, 0));
    assertEquals(1.0, getFloat(batch, 0), 0.00001);
    assertEquals(-15.0, getDouble(batch, 0), 0.00001);
    assertEquals(bytes(0, 1, 2, 3, 4), getBinary(batch, 0));
    assertEquals("hi", getText(batch, 0).toString());

    StructColumnVector middle = (StructColumnVector) batch.cols[9];
    ListColumnVector midList = (ListColumnVector) middle.fields[0];
    StructColumnVector midListStruct = (StructColumnVector) midList.child;
    LongColumnVector midListInt = (LongColumnVector) midListStruct.fields[0];
    BytesColumnVector midListStr = (BytesColumnVector) midListStruct.fields[1];
    ListColumnVector list = (ListColumnVector) batch.cols[10];
    StructColumnVector listStruct = (StructColumnVector) list.child;
    LongColumnVector listInts = (LongColumnVector) listStruct.fields[0];
    BytesColumnVector listStrs = (BytesColumnVector) listStruct.fields[1];
    MapColumnVector map = (MapColumnVector) batch.cols[11];
    BytesColumnVector mapKey = (BytesColumnVector) map.keys;
    StructColumnVector mapValue = (StructColumnVector) map.values;
    LongColumnVector mapValueInts = (LongColumnVector) mapValue.fields[0];
    BytesColumnVector mapValueStrs = (BytesColumnVector) mapValue.fields[1];
    TimestampColumnVector timestamp = (TimestampColumnVector) batch.cols[12];
    DecimalColumnVector decs = (DecimalColumnVector) batch.cols[13];

    assertFalse(middle.isNull[0]);
    assertEquals(2, midList.lengths[0]);
    int start = (int) midList.offsets[0];
    assertEquals(1, midListInt.vector[start]);
    assertEquals("bye", midListStr.toString(start));
    assertEquals(2, midListInt.vector[start + 1]);
    assertEquals("sigh", midListStr.toString(start + 1));

    assertEquals(2, list.lengths[0]);
    start = (int) list.offsets[0];
    assertEquals(3, listInts.vector[start]);
    assertEquals("good", listStrs.toString(start));
    assertEquals(4, listInts.vector[start + 1]);
    assertEquals("bad", listStrs.toString(start + 1));
    assertEquals(0, map.lengths[0]);
    assertEquals(Timestamp.valueOf("2000-03-12 15:00:00"),
        timestamp.asScratchTimestamp(0));
    assertEquals(new HiveDecimalWritable(HiveDecimal.create("12345678.6547456")),
        decs.vector[0]);

    // check the contents of row 7499
    rows.seekToRow(7499);
    assertTrue(rows.nextBatch(batch));
    assertTrue(getBoolean(batch, 0));
    assertEquals(100, getByte(batch, 0));
    assertEquals(2048, getShort(batch, 0));
    assertEquals(65536, getInt(batch, 0));
    assertEquals(Long.MAX_VALUE, getLong(batch, 0));
    assertEquals(2.0, getFloat(batch, 0), 0.00001);
    assertEquals(-5.0, getDouble(batch, 0), 0.00001);
    assertEquals(bytes(), getBinary(batch, 0));
    assertEquals("bye", getText(batch, 0).toString());
    assertFalse(middle.isNull[0]);
    assertEquals(2, midList.lengths[0]);
    start = (int) midList.offsets[0];
    assertEquals(1, midListInt.vector[start]);
    assertEquals("bye", midListStr.toString(start));
    assertEquals(2, midListInt.vector[start + 1]);
    assertEquals("sigh", midListStr.toString(start + 1));
    assertEquals(3, list.lengths[0]);
    start = (int) list.offsets[0];
    assertEquals(100000000, listInts.vector[start]);
    assertEquals("cat", listStrs.toString(start));
    assertEquals(-100000, listInts.vector[start + 1]);
    assertEquals("in", listStrs.toString(start + 1));
    assertEquals(1234, listInts.vector[start + 2]);
    assertEquals("hat", listStrs.toString(start + 2));
    assertEquals(2, map.lengths[0]);
    start = (int) map.offsets[0];
    assertEquals("chani", mapKey.toString(start));
    assertEquals(5, mapValueInts.vector[start]);
    assertEquals("chani", mapValueStrs.toString(start));
    assertEquals("mauddib", mapKey.toString(start + 1));
    assertEquals(1, mapValueInts.vector[start + 1]);
    assertEquals("mauddib", mapValueStrs.toString(start + 1));
    assertEquals(Timestamp.valueOf("2000-03-12 15:00:01"),
        timestamp.asScratchTimestamp(0));
    assertEquals(new HiveDecimalWritable(HiveDecimal.create("12345678.6547457")),
        decs.vector[0]);

    // handle the close up
    assertFalse(rows.nextBatch(batch));
    rows.close();
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testTimestampBug(Version fileFormat) throws IOException {
    TypeDescription schema = TypeDescription.createTimestamp();
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
            .bufferSize(10000).version(fileFormat));
    int batchCount = 5;
    VectorizedRowBatch batch = schema.createRowBatch(batchCount * 2);;
    TimestampColumnVector vec = (TimestampColumnVector) batch.cols[0];
    int[] seconds = new int[]{ -2, -1, 0, 1, 2 };
    // write 1st batch with nanosecond <= 999999
    int nanos = 999_999;
    for (int i = 0; i < batchCount; i++) {
      Timestamp curr = Timestamp.from(Instant.ofEpochSecond(seconds[i]));
      curr.setNanos(nanos);
      vec.set(i, curr);
    }

    batch.size = batchCount;
    writer.addRowBatch(batch);

    nanos = 1_000_000;
    // write 2nd batch with nanosecond > 999999
    for (int i = 0; i < batchCount; i++) {
      Timestamp curr = Timestamp.from(Instant.ofEpochSecond(seconds[i]));
      curr.setNanos(nanos);
      vec.set(i, curr);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch(batchCount);
    TimestampColumnVector timestamps = (TimestampColumnVector) batch.cols[0];
    rows.nextBatch(batch);
    // read 1st batch with nanosecond <= 999999
    for (int r=0; r < batchCount; ++r) {
      assertEquals(seconds[r], timestamps.getTimestampAsLong(r));
      assertEquals(999_999, timestamps.nanos[r]);
    }
    rows.nextBatch(batch);
    // read 2nd batch with nanosecond > 999999
    for (int r=0; r < batchCount; ++r) {
      if (seconds[r] == -1) {
        // reproduce the JDK bug of java.sql.Timestamp see ORC-763
        // Wrong extra second: 1969-12-31 23.59.59.001 -> 1970-01-01 00.00.00.001
        assertEquals(0, timestamps.getTimestampAsLong(r));
      } else {
        assertEquals(seconds[r], timestamps.getTimestampAsLong(r));
      }
      assertEquals(1_000_000, timestamps.nanos[r]);
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testTimestamp(Version fileFormat) throws Exception {
    TypeDescription schema = TypeDescription.createTimestamp();
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
            .bufferSize(10000).version(fileFormat));
    List<Timestamp> tslist = Lists.newArrayList();
    tslist.add(Timestamp.valueOf("2037-01-01 00:00:00.000999"));
    tslist.add(Timestamp.valueOf("2003-01-01 00:00:00.000000222"));
    tslist.add(Timestamp.valueOf("1999-01-01 00:00:00.999999999"));
    tslist.add(Timestamp.valueOf("1995-01-01 00:00:00.688888888"));
    tslist.add(Timestamp.valueOf("2002-01-01 00:00:00.1"));
    tslist.add(Timestamp.valueOf("2010-03-02 00:00:00.000009001"));
    tslist.add(Timestamp.valueOf("2005-01-01 00:00:00.000002229"));
    tslist.add(Timestamp.valueOf("2006-01-01 00:00:00.900203003"));
    tslist.add(Timestamp.valueOf("2003-01-01 00:00:00.800000007"));
    tslist.add(Timestamp.valueOf("1996-08-02 00:00:00.723100809"));
    tslist.add(Timestamp.valueOf("1998-11-02 00:00:00.857340643"));
    tslist.add(Timestamp.valueOf("2008-10-02 00:00:00"));

    VectorizedRowBatch batch = new VectorizedRowBatch(1, 1024);
    TimestampColumnVector vec = new TimestampColumnVector(1024);
    batch.cols[0] = vec;
    batch.reset();
    batch.size = tslist.size();
    for (int i=0; i < tslist.size(); ++i) {
      Timestamp ts = tslist.get(i);
      vec.set(i, ts);
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    TimestampColumnVector timestamps = (TimestampColumnVector) batch.cols[0];
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(tslist.get(idx++).getNanos(),
            timestamps.asScratchTimestamp(r).getNanos());
      }
    }
    assertEquals(tslist.size(), rows.getRowNumber());
    assertEquals(0, writer.getSchema().getMaximumId());
    boolean[] expected = new boolean[] {false};
    boolean[] included = OrcUtils.includeColumns("", writer.getSchema());
    assertTrue(Arrays.equals(expected, included));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testStringAndBinaryStatistics(Version fileFormat) throws Exception {

    TypeDescription schema = TypeDescription.createStruct()
        .addField("bytes1", TypeDescription.createBinary())
        .addField("string1", TypeDescription.createString());
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .setSchema(schema)
                                         .stripeSize(100000)
                                         .bufferSize(10000)
                                         .version(fileFormat));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 4;
    BytesColumnVector field1 = (BytesColumnVector) batch.cols[0];
    BytesColumnVector field2 = (BytesColumnVector) batch.cols[1];
    field1.setVal(0, bytesArray(0, 1, 2, 3, 4));
    field1.setVal(1, bytesArray(0, 1, 2, 3));
    field1.setVal(2, bytesArray(0, 1, 2, 3, 4, 5));
    field1.noNulls = false;
    field1.isNull[3] = true;
    field2.setVal(0, "foo".getBytes(StandardCharsets.UTF_8));
    field2.setVal(1, "bar".getBytes(StandardCharsets.UTF_8));
    field2.noNulls = false;
    field2.isNull[2] = true;
    field2.setVal(3, "hi".getBytes(StandardCharsets.UTF_8));
    writer.addRowBatch(batch);
    writer.close();
    schema = writer.getSchema();
    assertEquals(2, schema.getMaximumId());

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

    boolean[] expected = new boolean[] {false, false, true};
    boolean[] included = OrcUtils.includeColumns("string1", schema);
    assertTrue(Arrays.equals(expected, included));

    expected = new boolean[] {false, false, false};
    included = OrcUtils.includeColumns("", schema);
    assertTrue(Arrays.equals(expected, included));

    expected = new boolean[] {false, false, false};
    included = OrcUtils.includeColumns(null, schema);
    assertTrue(Arrays.equals(expected, included));

    // check the stats
    ColumnStatistics[] stats = reader.getStatistics();
    assertArrayEquals(stats, writer.getStatistics());
    assertEquals(4, stats[0].getNumberOfValues());
    assertEquals("count: 4 hasNull: false", stats[0].toString());

    assertEquals(3, stats[1].getNumberOfValues());
    assertEquals(15, ((BinaryColumnStatistics) stats[1]).getSum());
    assertEquals("count: 3 hasNull: true bytesOnDisk: 28 sum: 15", stats[1].toString());

    assertEquals(3, stats[2].getNumberOfValues());
    assertEquals("bar", ((StringColumnStatistics) stats[2]).getMinimum());
    assertEquals("hi", ((StringColumnStatistics) stats[2]).getMaximum());
    assertEquals(8, ((StringColumnStatistics) stats[2]).getSum());
    assertEquals("count: 3 hasNull: true bytesOnDisk: " +
            (fileFormat == OrcFile.Version.V_0_11 ? "30" : "22") +
            " min: bar max: hi sum: 8",
        stats[2].toString());

    // check the inspectors
    batch = reader.getSchema().createRowBatch();
    BytesColumnVector bytes = (BytesColumnVector) batch.cols[0];
    BytesColumnVector strs = (BytesColumnVector) batch.cols[1];
    RecordReader rows = reader.rows();
    assertTrue(rows.nextBatch(batch));
    assertEquals(4, batch.size);

    // check the contents of the first row
    assertEquals(bytes(0,1,2,3,4), getBinary(bytes, 0));
    assertEquals("foo", strs.toString(0));

    // check the contents of second row
    assertEquals(bytes(0,1,2,3), getBinary(bytes, 1));
    assertEquals("bar", strs.toString(1));

    // check the contents of third row
    assertEquals(bytes(0,1,2,3,4,5), getBinary(bytes, 2));
    assertNull(strs.toString(2));

    // check the contents of fourth row
    assertNull(getBinary(bytes, 3));
    assertEquals("hi", strs.toString(3));

    // handle the close up
    assertFalse(rows.nextBatch(batch));
    rows.close();
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testHiveDecimalStatsAllNulls(Version fileFormat) throws Exception {
    TypeDescription schema = TypeDescription.createStruct()
      .addField("dec1", TypeDescription.createDecimal());

    Writer writer = OrcFile.createWriter(testFilePath,
      OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
          .bufferSize(10000).version(fileFormat));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 4;
    DecimalColumnVector field1 = (DecimalColumnVector) batch.cols[0];
    field1.noNulls = false;
    field1.isNull[0] = true;
    field1.isNull[1] = true;
    field1.isNull[2] = true;
    field1.isNull[3] = true;
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
      OrcFile.readerOptions(conf).filesystem(fs));
    // check the stats
    ColumnStatistics[] stats = reader.getStatistics();
    assertEquals(4, stats[0].getNumberOfValues());
    assertEquals(0, stats[1].getNumberOfValues());
    assertTrue(stats[1].hasNull());
    assertNull(((DecimalColumnStatistics)stats[1]).getMinimum());
    assertNull(((DecimalColumnStatistics)stats[1]).getMaximum());
    assertEquals(new HiveDecimalWritable(0).getHiveDecimal(), ((DecimalColumnStatistics)stats[1]).getSum());
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testStripeLevelStats(Version fileFormat) throws Exception {
    TypeDescription schema =
        TypeDescription.fromString("struct<int1:int,string1:string>");
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .bufferSize(10000)
            .enforceBufferSize()
            .version(fileFormat));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 1000;
    LongColumnVector field1 = (LongColumnVector) batch.cols[0];
    BytesColumnVector field2 = (BytesColumnVector) batch.cols[1];
    field1.isRepeating = true;
    field2.isRepeating = true;
    for (int b = 0; b < 11; b++) {
      if (b >= 5) {
        if (b >= 10) {
          field1.vector[0] = 3;
          field2.setVal(0, "three".getBytes(StandardCharsets.UTF_8));
        } else {
          field1.vector[0] = 2;
          field2.setVal(0, "two".getBytes(StandardCharsets.UTF_8));
        }
      } else {
        field1.vector[0] = 1;
        field2.setVal(0, "one".getBytes(StandardCharsets.UTF_8));
      }
      writer.addRowBatch(batch);
    }

    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

    schema = writer.getSchema();
    assertEquals(2, schema.getMaximumId());
    boolean[] expected = new boolean[] {false, true, false};
    boolean[] included = OrcUtils.includeColumns("int1", schema);
    assertTrue(Arrays.equals(expected, included));

    List<StripeStatistics> stats = reader.getStripeStatistics();
    int numStripes = stats.size();
    assertEquals(3, numStripes);
    StripeStatistics ss1 = stats.get(0);
    StripeStatistics ss2 = stats.get(1);
    StripeStatistics ss3 = stats.get(2);

    assertEquals(5000, ss1.getColumnStatistics()[0].getNumberOfValues());
    assertEquals(5000, ss2.getColumnStatistics()[0].getNumberOfValues());
    assertEquals(1000, ss3.getColumnStatistics()[0].getNumberOfValues());

    assertEquals(5000, (ss1.getColumnStatistics()[1]).getNumberOfValues());
    assertEquals(5000, (ss2.getColumnStatistics()[1]).getNumberOfValues());
    assertEquals(1000, (ss3.getColumnStatistics()[1]).getNumberOfValues());
    assertEquals(1, ((IntegerColumnStatistics)ss1.getColumnStatistics()[1]).getMinimum());
    assertEquals(2, ((IntegerColumnStatistics)ss2.getColumnStatistics()[1]).getMinimum());
    assertEquals(3, ((IntegerColumnStatistics)ss3.getColumnStatistics()[1]).getMinimum());
    assertEquals(1, ((IntegerColumnStatistics)ss1.getColumnStatistics()[1]).getMaximum());
    assertEquals(2, ((IntegerColumnStatistics)ss2.getColumnStatistics()[1]).getMaximum());
    assertEquals(3, ((IntegerColumnStatistics)ss3.getColumnStatistics()[1]).getMaximum());
    assertEquals(5000, ((IntegerColumnStatistics)ss1.getColumnStatistics()[1]).getSum());
    assertEquals(10000, ((IntegerColumnStatistics)ss2.getColumnStatistics()[1]).getSum());
    assertEquals(3000, ((IntegerColumnStatistics)ss3.getColumnStatistics()[1]).getSum());

    assertEquals(5000, (ss1.getColumnStatistics()[2]).getNumberOfValues());
    assertEquals(5000, (ss2.getColumnStatistics()[2]).getNumberOfValues());
    assertEquals(1000, (ss3.getColumnStatistics()[2]).getNumberOfValues());
    assertEquals("one", ((StringColumnStatistics)ss1.getColumnStatistics()[2]).getMinimum());
    assertEquals("two", ((StringColumnStatistics)ss2.getColumnStatistics()[2]).getMinimum());
    assertEquals("three", ((StringColumnStatistics)ss3.getColumnStatistics()[2]).getMinimum());
    assertEquals("one", ((StringColumnStatistics)ss1.getColumnStatistics()[2]).getMaximum());
    assertEquals("two", ((StringColumnStatistics) ss2.getColumnStatistics()[2]).getMaximum());
    assertEquals("three", ((StringColumnStatistics)ss3.getColumnStatistics()[2]).getMaximum());
    assertEquals(15000, ((StringColumnStatistics)ss1.getColumnStatistics()[2]).getSum());
    assertEquals(15000, ((StringColumnStatistics)ss2.getColumnStatistics()[2]).getSum());
    assertEquals(5000, ((StringColumnStatistics)ss3.getColumnStatistics()[2]).getSum());

    RecordReaderImpl recordReader = (RecordReaderImpl) reader.rows();
    OrcProto.RowIndex[] index = recordReader.readRowIndex(0, null, null).getRowGroupIndex();
    assertEquals(3, index.length);
    List<OrcProto.RowIndexEntry> items = index[1].getEntryList();
    assertEquals(1, items.size());
    assertEquals(3, items.get(0).getPositionsCount());
    assertEquals(0, items.get(0).getPositions(0));
    assertEquals(0, items.get(0).getPositions(1));
    assertEquals(0, items.get(0).getPositions(2));
    assertEquals(1,
                 items.get(0).getStatistics().getIntStatistics().getMinimum());
    index = recordReader.readRowIndex(1, null, null).getRowGroupIndex();
    assertEquals(3, index.length);
    items = index[1].getEntryList();
    assertEquals(2,
        items.get(0).getStatistics().getIntStatistics().getMaximum());
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testStripeLevelStatsNoForce(Version fileFormat) throws Exception {
    TypeDescription schema =
        TypeDescription.fromString("struct<int1:int,string1:string>");
    OrcConf.DICTIONARY_IMPL.setString(conf, "hash");
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .bufferSize(10000)
            .version(fileFormat));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 1000;
    LongColumnVector field1 = (LongColumnVector) batch.cols[0];
    BytesColumnVector field2 = (BytesColumnVector) batch.cols[1];
    field1.isRepeating = true;
    field2.isRepeating = true;
    for (int b = 0; b < 11; b++) {
      if (b >= 5) {
        if (b >= 10) {
          field1.vector[0] = 3;
          field2.setVal(0, "three".getBytes(StandardCharsets.UTF_8));
        } else {
          field1.vector[0] = 2;
          field2.setVal(0, "two".getBytes(StandardCharsets.UTF_8));
        }
      } else {
        field1.vector[0] = 1;
        field2.setVal(0, "one".getBytes(StandardCharsets.UTF_8));
      }
      writer.addRowBatch(batch);
    }

    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

    schema = writer.getSchema();
    assertEquals(2, schema.getMaximumId());
    boolean[] expected = new boolean[] {false, true, false};
    boolean[] included = OrcUtils.includeColumns("int1", schema);
    assertTrue(Arrays.equals(expected, included));

    List<StripeStatistics> stats = reader.getStripeStatistics();
    int numStripes = stats.size();
    assertEquals(2, numStripes);
    StripeStatistics ss1 = stats.get(0);
    StripeStatistics ss2 = stats.get(1);

    assertEquals(10000, ss1.getColumnStatistics()[0].getNumberOfValues());
    assertEquals(1000, ss2.getColumnStatistics()[0].getNumberOfValues());

    assertEquals(10000, (ss1.getColumnStatistics()[1]).getNumberOfValues());
    assertEquals(1000, (ss2.getColumnStatistics()[1]).getNumberOfValues());
    assertEquals(1, ((IntegerColumnStatistics)ss1.getColumnStatistics()[1]).getMinimum());
    assertEquals(3, ((IntegerColumnStatistics)ss2.getColumnStatistics()[1]).getMinimum());
    assertEquals(2, ((IntegerColumnStatistics)ss1.getColumnStatistics()[1]).getMaximum());
    assertEquals(3, ((IntegerColumnStatistics)ss2.getColumnStatistics()[1]).getMaximum());
    assertEquals(15000, ((IntegerColumnStatistics)ss1.getColumnStatistics()[1]).getSum());
    assertEquals(3000, ((IntegerColumnStatistics)ss2.getColumnStatistics()[1]).getSum());

    assertEquals(10000, (ss1.getColumnStatistics()[2]).getNumberOfValues());
    assertEquals(1000, (ss2.getColumnStatistics()[2]).getNumberOfValues());
    assertEquals("one", ((StringColumnStatistics)ss1.getColumnStatistics()[2]).getMinimum());
    assertEquals("three", ((StringColumnStatistics)ss2.getColumnStatistics()[2]).getMinimum());
    assertEquals("two", ((StringColumnStatistics)ss1.getColumnStatistics()[2]).getMaximum());
    assertEquals("three", ((StringColumnStatistics) ss2.getColumnStatistics()[2]).getMaximum());
    assertEquals(30000, ((StringColumnStatistics)ss1.getColumnStatistics()[2]).getSum());
    assertEquals(5000, ((StringColumnStatistics)ss2.getColumnStatistics()[2]).getSum());

    RecordReaderImpl recordReader = (RecordReaderImpl) reader.rows();
    OrcProto.RowIndex[] index = recordReader.readRowIndex(0, null, null).getRowGroupIndex();
    assertEquals(3, index.length);
    List<OrcProto.RowIndexEntry> items = index[1].getEntryList();
    assertEquals(1, items.size());
    assertEquals(3, items.get(0).getPositionsCount());
    assertEquals(0, items.get(0).getPositions(0));
    assertEquals(0, items.get(0).getPositions(1));
    assertEquals(0, items.get(0).getPositions(2));
    assertEquals(1,
        items.get(0).getStatistics().getIntStatistics().getMinimum());
    index = recordReader.readRowIndex(1, null, null).getRowGroupIndex();
    assertEquals(3, index.length);
    items = index[1].getEntryList();
    assertEquals(3, items.get(0).getStatistics().getIntStatistics().getMaximum());
  }

  private static void setInner(StructColumnVector inner, int rowId,
                               int i, String value) {
    ((LongColumnVector) inner.fields[0]).vector[rowId] = i;
    if (value != null) {
      ((BytesColumnVector) inner.fields[1]).setVal(rowId, value.getBytes(StandardCharsets.UTF_8));
    } else {
      inner.fields[1].isNull[rowId] = true;
      inner.fields[1].noNulls = false;
    }
  }

  private static void checkInner(StructColumnVector inner, int rowId,
                                 int rowInBatch, int i, String value) {
    assertEquals(i,
        ((LongColumnVector) inner.fields[0]).vector[rowInBatch],
        "row " + rowId);
    if (value != null) {
      assertEquals(value,
          ((BytesColumnVector) inner.fields[1]).toString(rowInBatch),
          "row " + rowId);
    } else {
      assertTrue(inner.fields[1].isNull[rowInBatch], "row " + rowId);
      assertFalse(inner.fields[1].noNulls, "row " + rowId);
    }
  }

  private static void setInnerList(ListColumnVector list, int rowId,
                                   List<InnerStruct> value) {
    if (value != null) {
      if (list.childCount + value.size() > list.child.isNull.length) {
        list.child.ensureSize(list.childCount * 2, true);
      }
      list.lengths[rowId] = value.size();
      list.offsets[rowId] = list.childCount;
      for (int i = 0; i < list.lengths[rowId]; ++i) {
        InnerStruct inner = value.get(i);
        setInner((StructColumnVector) list.child, i + list.childCount,
            inner.int1, inner.string1.toString());
      }
      list.childCount += value.size();
    } else {
      list.isNull[rowId] = true;
      list.noNulls = false;
    }
  }

  private static void checkInnerList(ListColumnVector list, int rowId,
                                     int rowInBatch, List<InnerStruct> value) {
    if (value != null) {
      assertEquals(value.size(), list.lengths[rowInBatch], "row " + rowId);
      int start = (int) list.offsets[rowInBatch];
      for (int i = 0; i < list.lengths[rowInBatch]; ++i) {
        InnerStruct inner = value.get(i);
        checkInner((StructColumnVector) list.child, rowId, i + start,
            inner.int1, inner.string1.toString());
      }
      list.childCount += value.size();
    } else {
      assertTrue(list.isNull[rowInBatch], "row " + rowId);
      assertFalse(list.noNulls, "row " + rowId);
    }
  }

  private static void setInnerMap(MapColumnVector map, int rowId,
                                  Map<String, InnerStruct> value) {
    if (value != null) {
      if (map.childCount >= map.keys.isNull.length) {
        map.keys.ensureSize(map.childCount * 2, true);
        map.values.ensureSize(map.childCount * 2, true);
      }
      map.lengths[rowId] = value.size();
      int offset = map.childCount;
      map.offsets[rowId] = offset;

      for (Map.Entry<String, InnerStruct> entry : value.entrySet()) {
        ((BytesColumnVector) map.keys).setVal(offset, entry.getKey().getBytes(StandardCharsets.UTF_8));
        InnerStruct inner = entry.getValue();
        setInner((StructColumnVector) map.values, offset, inner.int1,
            inner.string1.toString());
        offset += 1;
      }
      map.childCount = offset;
    } else {
      map.isNull[rowId] = true;
      map.noNulls = false;
    }
  }

  private static void checkInnerMap(MapColumnVector map, int rowId,
                                    int rowInBatch,
                                    Map<String, InnerStruct> value) {
    if (value != null) {
      assertEquals(value.size(), map.lengths[rowInBatch], "row " + rowId);
      int offset = (int) map.offsets[rowInBatch];
      for(int i=0; i < value.size(); ++i) {
        String key = ((BytesColumnVector) map.keys).toString(offset + i);
        InnerStruct expected = value.get(key);
        checkInner((StructColumnVector) map.values, rowId, offset + i,
            expected.int1, expected.string1.toString());
      }
    } else {
      assertTrue(map.isNull[rowId], "row " + rowId);
      assertFalse(map.noNulls, "row " + rowId);
    }
  }

  private static void setMiddleStruct(StructColumnVector middle, int rowId,
                                      MiddleStruct value) {
    if (value != null) {
      setInnerList((ListColumnVector) middle.fields[0], rowId, value.list);
    } else {
      middle.isNull[rowId] = true;
      middle.noNulls = false;
    }
  }

  private static void checkMiddleStruct(StructColumnVector middle, int rowId,
                                        int rowInBatch, MiddleStruct value) {
    if (value != null) {
      checkInnerList((ListColumnVector) middle.fields[0], rowId, rowInBatch,
          value.list);
    } else {
      assertTrue(middle.isNull[rowInBatch], "row " + rowId);
      assertFalse(middle.noNulls, "row " + rowId);
    }
  }

  private static void setBigRow(VectorizedRowBatch batch, int rowId,
                                Boolean b1, Byte b2, Short s1,
                                Integer i1, Long l1, Float f1,
                                Double d1, BytesWritable b3, String s2,
                                MiddleStruct m1, List<InnerStruct> l2,
                                Map<String, InnerStruct> m2) {
    ((LongColumnVector) batch.cols[0]).vector[rowId] = b1 ? 1 : 0;
    ((LongColumnVector) batch.cols[1]).vector[rowId] = b2;
    ((LongColumnVector) batch.cols[2]).vector[rowId] = s1;
    ((LongColumnVector) batch.cols[3]).vector[rowId] = i1;
    ((LongColumnVector) batch.cols[4]).vector[rowId] = l1;
    ((DoubleColumnVector) batch.cols[5]).vector[rowId] = f1;
    ((DoubleColumnVector) batch.cols[6]).vector[rowId] = d1;
    if (b3 != null) {
      ((BytesColumnVector) batch.cols[7]).setVal(rowId, b3.getBytes(), 0,
          b3.getLength());
    } else {
      batch.cols[7].isNull[rowId] = true;
      batch.cols[7].noNulls = false;
    }
    if (s2 != null) {
      ((BytesColumnVector) batch.cols[8]).setVal(rowId, s2.getBytes(StandardCharsets.UTF_8));
    } else {
      batch.cols[8].isNull[rowId] = true;
      batch.cols[8].noNulls = false;
    }
    setMiddleStruct((StructColumnVector) batch.cols[9], rowId, m1);
    setInnerList((ListColumnVector) batch.cols[10], rowId, l2);
    setInnerMap((MapColumnVector) batch.cols[11], rowId, m2);
  }

  private static void checkBigRow(VectorizedRowBatch batch,
                                  int rowInBatch,
                                  int rowId,
                                  boolean b1, byte b2, short s1,
                                  int i1, long l1, float f1,
                                  double d1, BytesWritable b3, String s2,
                                  MiddleStruct m1, List<InnerStruct> l2,
                                  Map<String, InnerStruct> m2) {
    String msg = "row " + rowId;
    assertEquals(b1, getBoolean(batch, rowInBatch), msg);
    assertEquals(b2, getByte(batch, rowInBatch), msg);
    assertEquals(s1, getShort(batch, rowInBatch), msg);
    assertEquals(i1, getInt(batch, rowInBatch), msg);
    assertEquals(l1, getLong(batch, rowInBatch), msg);
    assertEquals(f1, getFloat(batch, rowInBatch), 0.0001, msg);
    assertEquals(d1, getDouble(batch, rowInBatch), 0.0001, msg);
    if (b3 != null) {
      BytesColumnVector bytes = (BytesColumnVector) batch.cols[7];
      assertEquals(b3.getLength(), bytes.length[rowInBatch], msg);
      for(int i=0; i < b3.getLength(); ++i) {
        assertEquals(b3.getBytes()[i],
            bytes.vector[rowInBatch][bytes.start[rowInBatch] + i],
            "row " + rowId + " byte " + i);
      }
    } else {
      assertTrue(batch.cols[7].isNull[rowInBatch], msg);
      assertFalse(batch.cols[7].noNulls, msg);
    }
    if (s2 != null) {
      assertEquals(s2, getText(batch, rowInBatch).toString(), "row " + rowId);
    } else {
      assertTrue(batch.cols[8].isNull[rowInBatch], msg);
      assertFalse(batch.cols[8].noNulls, msg);
    }
    checkMiddleStruct((StructColumnVector) batch.cols[9], rowId, rowInBatch,
        m1);
    checkInnerList((ListColumnVector) batch.cols[10], rowId, rowInBatch, l2);
    checkInnerMap((MapColumnVector) batch.cols[11], rowId, rowInBatch, m2);
  }

  private static boolean getBoolean(VectorizedRowBatch batch, int rowId) {
    return ((LongColumnVector) batch.cols[0]).vector[rowId] != 0;
  }

  private static byte getByte(VectorizedRowBatch batch, int rowId) {
    return (byte) ((LongColumnVector) batch.cols[1]).vector[rowId];
  }

  private static short getShort(VectorizedRowBatch batch, int rowId) {
    return (short) ((LongColumnVector) batch.cols[2]).vector[rowId];
  }

  private static int getInt(VectorizedRowBatch batch, int rowId) {
    return (int) ((LongColumnVector) batch.cols[3]).vector[rowId];
  }

  private static long getLong(VectorizedRowBatch batch, int rowId) {
    return ((LongColumnVector) batch.cols[4]).vector[rowId];
  }

  private static float getFloat(VectorizedRowBatch batch, int rowId) {
    return (float) ((DoubleColumnVector) batch.cols[5]).vector[rowId];
  }

  private static double getDouble(VectorizedRowBatch batch, int rowId) {
    return ((DoubleColumnVector) batch.cols[6]).vector[rowId];
  }

  protected static BytesWritable getBinary(BytesColumnVector column, int rowId) {
    if (column.isRepeating) {
      rowId = 0;
    }
    if (column.noNulls || !column.isNull[rowId]) {
      return new BytesWritable(Arrays.copyOfRange(column.vector[rowId],
          column.start[rowId], column.start[rowId] + column.length[rowId]));
    } else {
      return null;
    }
  }

  private static BytesWritable getBinary(VectorizedRowBatch batch, int rowId) {
    return getBinary((BytesColumnVector) batch.cols[7], rowId);
  }

  private static Text getText(BytesColumnVector vector, int rowId) {
    if (vector.isRepeating) {
      rowId = 0;
    }
    if (vector.noNulls || !vector.isNull[rowId]) {
      return new Text(Arrays.copyOfRange(vector.vector[rowId],
          vector.start[rowId], vector.start[rowId] + vector.length[rowId]));
    } else {
      return null;
    }
  }

  private static Text getText(VectorizedRowBatch batch, int rowId) {
    return getText((BytesColumnVector) batch.cols[8], rowId);
  }

  private static InnerStruct getInner(StructColumnVector vector,
                                      int rowId) {
    return new InnerStruct(
        (int) ((LongColumnVector) vector.fields[0]).vector[rowId],
        getText((BytesColumnVector) vector.fields[1], rowId));
  }

  private static List<InnerStruct> getList(ListColumnVector cv,
                                           int rowId) {
    if (cv.isRepeating) {
      rowId = 0;
    }
    if (cv.noNulls || !cv.isNull[rowId]) {
      List<InnerStruct> result =
          new ArrayList<InnerStruct>((int) cv.lengths[rowId]);
      for(long i=cv.offsets[rowId];
          i < cv.offsets[rowId] + cv.lengths[rowId]; ++i) {
        result.add(getInner((StructColumnVector) cv.child, (int) i));
      }
      return result;
    } else {
      return null;
    }
  }

  private static List<InnerStruct> getMidList(VectorizedRowBatch batch,
                                              int rowId) {
    return getList((ListColumnVector) ((StructColumnVector) batch.cols[9])
        .fields[0], rowId);
  }

  private static List<InnerStruct> getList(VectorizedRowBatch batch,
                                           int rowId) {
    return getList((ListColumnVector) batch.cols[10], rowId);
  }

  private static Map<Text, InnerStruct> getMap(VectorizedRowBatch batch,
                                               int rowId) {
    MapColumnVector cv = (MapColumnVector) batch.cols[11];
    if (cv.isRepeating) {
      rowId = 0;
    }
    if (cv.noNulls || !cv.isNull[rowId]) {
      Map<Text, InnerStruct> result =
          new HashMap<Text, InnerStruct>((int) cv.lengths[rowId]);
      for(long i=cv.offsets[rowId];
          i < cv.offsets[rowId] + cv.lengths[rowId]; ++i) {
        result.put(getText((BytesColumnVector) cv.keys, (int) i),
            getInner((StructColumnVector) cv.values, (int) i));
      }
      return result;
    } else {
      return null;
    }
  }

  private static TypeDescription createInnerSchema() {
    return TypeDescription.fromString("struct<int1:int,string1:string>");
  }

  private static TypeDescription createComplexInnerSchema()
  {
    return TypeDescription.fromString("struct<int1:int,"
        + "complex:struct<int2:int,String1:string>>");
  }

  private static TypeDescription createQuotedSchema() {
    return TypeDescription.createStruct()
        .addField("`int1`", TypeDescription.createInt())
        .addField("`string1`", TypeDescription.createString());
  }

  private static TypeDescription createQuotedSchemaFromString() {
    return TypeDescription.fromString("struct<```int1```:int,```string1```:string>");
  }

  private static TypeDescription createBigRowSchema() {
    return TypeDescription.createStruct()
        .addField("boolean1", TypeDescription.createBoolean())
        .addField("byte1", TypeDescription.createByte())
        .addField("short1", TypeDescription.createShort())
        .addField("int1", TypeDescription.createInt())
        .addField("long1", TypeDescription.createLong())
        .addField("float1", TypeDescription.createFloat())
        .addField("double1", TypeDescription.createDouble())
        .addField("bytes1", TypeDescription.createBinary())
        .addField("string1", TypeDescription.createString())
        .addField("middle", TypeDescription.createStruct()
            .addField("list", TypeDescription.createList(createInnerSchema())))
        .addField("list", TypeDescription.createList(createInnerSchema()))
        .addField("map", TypeDescription.createMap(
            TypeDescription.createString(),
            createInnerSchema()));
  }

  static void assertArrayBooleanEquals(boolean[] expected, boolean[] actual) {
    assertEquals(expected.length, actual.length);
    boolean diff = false;
    for(int i=0; i < expected.length; ++i) {
      if (expected[i] != actual[i]) {
        System.out.println("Difference at " + i + " expected: " + expected[i] +
          " actual: " + actual[i]);
        diff = true;
      }
    }
    assertFalse(diff);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void test1(Version fileFormat) throws Exception {
    TypeDescription schema = createBigRowSchema();
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .bufferSize(10000)
            .version(fileFormat));
    assertEmptyStats(writer.getStatistics());
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 2;
    setBigRow(batch, 0, false, (byte) 1, (short) 1024, 65536,
        Long.MAX_VALUE, (float) 1.0, -15.0, bytes(0, 1, 2, 3, 4), "hi",
        new MiddleStruct(inner(1, "bye"), inner(2, "sigh")),
        list(inner(3, "good"), inner(4, "bad")),
        map());
    setBigRow(batch, 1, true, (byte) 100, (short) 2048, 65536,
        Long.MAX_VALUE, (float) 2.0, -5.0, bytes(), "bye",
        new MiddleStruct(inner(1, "bye"), inner(2, "sigh")),
        list(inner(100000000, "cat"), inner(-100000, "in"), inner(1234, "hat")),
        map(inner(5, "chani"), inner(1, "mauddib")));
    writer.addRowBatch(batch);
    assertEmptyStats(writer.getStatistics());
    writer.close();
    ColumnStatistics[] closeStatistics = writer.getStatistics();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

    schema = writer.getSchema();
    assertEquals(23, schema.getMaximumId());
    boolean[] expected = new boolean[] {false, false, false, false, false,
        false, false, false, false, false,
        false, false, false, false, false,
        false, false, false, false, false,
        false, false, false, false};
    boolean[] included = OrcUtils.includeColumns("", schema);
    assertTrue(Arrays.equals(expected, included));

    expected = new boolean[] {false, true, false, false, false,
        false, false, false, false, true,
        true, true, true, true, true,
        false, false, false, false, true,
        true, true, true, true};
    included = OrcUtils.includeColumns("boolean1,string1,middle,map", schema);

    assertArrayBooleanEquals(expected, included);

    expected = new boolean[] {false, true, false, false, false,
        false, false, false, false, true,
        true, true, true, true, true,
        false, false, false, false, true,
        true, true, true, true};
    included = OrcUtils.includeColumns("boolean1,string1,middle,map", schema);
    assertArrayBooleanEquals(expected, included);

    expected = new boolean[] {false, true, true, true, true,
        true, true, true, true, true,
        true, true, true, true, true,
        true, true, true, true, true,
        true, true, true, true};
    included = OrcUtils.includeColumns(
        "boolean1,byte1,short1,int1,long1,float1,double1,bytes1,string1,middle,list,map",
        schema);
    assertTrue(Arrays.equals(expected, included));

    // check the stats
    ColumnStatistics[] stats = reader.getStatistics();
    assertArrayEquals(stats, closeStatistics);
    assertEquals(2, stats[1].getNumberOfValues());
    assertEquals(1, ((BooleanColumnStatistics) stats[1]).getFalseCount());
    assertEquals(1, ((BooleanColumnStatistics) stats[1]).getTrueCount());
    assertEquals("count: 2 hasNull: false bytesOnDisk: 5 true: 1", stats[1].toString());

    assertEquals(2048, ((IntegerColumnStatistics) stats[3]).getMaximum());
    assertEquals(1024, ((IntegerColumnStatistics) stats[3]).getMinimum());
    assertTrue(((IntegerColumnStatistics) stats[3]).isSumDefined());
    assertEquals(3072, ((IntegerColumnStatistics) stats[3]).getSum());
    assertEquals("count: 2 hasNull: false bytesOnDisk: " +
        (fileFormat == OrcFile.Version.V_0_11 ? "8" : "9") +
            " min: 1024 max: 2048 sum: 3072", stats[3].toString());

    StripeStatistics ss = reader.getStripeStatistics().get(0);
    assertEquals(2, ss.getColumnStatistics()[0].getNumberOfValues());
    assertEquals(1, ((BooleanColumnStatistics) ss.getColumnStatistics()[1]).getTrueCount());
    assertEquals(1024, ((IntegerColumnStatistics) ss.getColumnStatistics()[3]).getMinimum());
    assertEquals(2048, ((IntegerColumnStatistics) ss.getColumnStatistics()[3]).getMaximum());
    assertEquals(3072, ((IntegerColumnStatistics) ss.getColumnStatistics()[3]).getSum());
    assertEquals(-15.0, ((DoubleColumnStatistics) stats[7]).getMinimum(), 0.0001);
    assertEquals(-5.0, ((DoubleColumnStatistics) stats[7]).getMaximum(), 0.0001);
    assertEquals(-20.0, ((DoubleColumnStatistics) stats[7]).getSum(), 0.00001);
    assertEquals("count: 2 hasNull: false bytesOnDisk: 15 min: -15.0 max: -5.0 sum: -20.0",
        stats[7].toString());

    assertEquals("count: 2 hasNull: false bytesOnDisk: " +
        (fileFormat == OrcFile.Version.V_0_11 ? "20" : "14") +
        " min: bye max: hi sum: 5", stats[9].toString());

    // check the schema
    TypeDescription readerSchema = reader.getSchema();
    assertEquals(TypeDescription.Category.STRUCT, readerSchema.getCategory());
    assertEquals("struct<boolean1:boolean,byte1:tinyint,short1:smallint,"
        + "int1:int,long1:bigint,float1:float,double1:double,bytes1:"
        + "binary,string1:string,middle:struct<list:array<struct<int1:int,"
        + "string1:string>>>,list:array<struct<int1:int,string1:string>>,"
        + "map:map<string,struct<int1:int,string1:string>>>",
        readerSchema.toString());
    List<String> fieldNames = readerSchema.getFieldNames();
    List<TypeDescription> fieldTypes = readerSchema.getChildren();
    assertEquals("boolean1", fieldNames.get(0));
    assertEquals(TypeDescription.Category.BOOLEAN, fieldTypes.get(0).getCategory());
    assertEquals("byte1", fieldNames.get(1));
    assertEquals(TypeDescription.Category.BYTE, fieldTypes.get(1).getCategory());
    assertEquals("short1", fieldNames.get(2));
    assertEquals(TypeDescription.Category.SHORT, fieldTypes.get(2).getCategory());
    assertEquals("int1", fieldNames.get(3));
    assertEquals(TypeDescription.Category.INT, fieldTypes.get(3).getCategory());
    assertEquals("long1", fieldNames.get(4));
    assertEquals(TypeDescription.Category.LONG, fieldTypes.get(4).getCategory());
    assertEquals("float1", fieldNames.get(5));
    assertEquals(TypeDescription.Category.FLOAT, fieldTypes.get(5).getCategory());
    assertEquals("double1", fieldNames.get(6));
    assertEquals(TypeDescription.Category.DOUBLE, fieldTypes.get(6).getCategory());
    assertEquals("bytes1", fieldNames.get(7));
    assertEquals(TypeDescription.Category.BINARY, fieldTypes.get(7).getCategory());
    assertEquals("string1", fieldNames.get(8));
    assertEquals(TypeDescription.Category.STRING, fieldTypes.get(8).getCategory());
    assertEquals("middle", fieldNames.get(9));
    TypeDescription middle = fieldTypes.get(9);
    assertEquals(TypeDescription.Category.STRUCT, middle.getCategory());
    TypeDescription midList = middle.getChildren().get(0);
    assertEquals(TypeDescription.Category.LIST, midList.getCategory());
    TypeDescription inner = midList.getChildren().get(0);
    assertEquals(TypeDescription.Category.STRUCT, inner.getCategory());
    assertEquals("int1", inner.getFieldNames().get(0));
    assertEquals("string1", inner.getFieldNames().get(1));

    RecordReader rows = reader.rows();
    // create a new batch
    batch = readerSchema.createRowBatch();
    assertTrue(rows.nextBatch(batch));
    assertEquals(2, batch.size);
    assertFalse(rows.nextBatch(batch));

    // check the contents of the first row
    assertFalse(getBoolean(batch, 0));
    assertEquals(1, getByte(batch, 0));
    assertEquals(1024, getShort(batch, 0));
    assertEquals(65536, getInt(batch, 0));
    assertEquals(Long.MAX_VALUE, getLong(batch, 0));
    assertEquals(1.0, getFloat(batch, 0), 0.00001);
    assertEquals(-15.0, getDouble(batch, 0), 0.00001);
    assertEquals(bytes(0,1,2,3,4), getBinary(batch, 0));
    assertEquals("hi", getText(batch, 0).toString());
    List<InnerStruct> midRow = getMidList(batch, 0);
    assertNotNull(midRow);
    assertEquals(2, midRow.size());
    assertEquals(1, midRow.get(0).int1);
    assertEquals("bye", midRow.get(0).string1.toString());
    assertEquals(2, midRow.get(1).int1);
    assertEquals("sigh", midRow.get(1).string1.toString());
    List<InnerStruct> list = getList(batch, 0);
    assertEquals(2, list.size());
    assertEquals(3, list.get(0).int1);
    assertEquals("good", list.get(0).string1.toString());
    assertEquals(4, list.get(1).int1);
    assertEquals("bad", list.get(1).string1.toString());
    Map<Text, InnerStruct> map = getMap(batch, 0);
    assertEquals(0, map.size());

    // check the contents of second row
    assertTrue(getBoolean(batch, 1));
    assertEquals(100, getByte(batch, 1));
    assertEquals(2048, getShort(batch, 1));
    assertEquals(65536, getInt(batch, 1));
    assertEquals(Long.MAX_VALUE, getLong(batch, 1));
    assertEquals(2.0, getFloat(batch, 1), 0.00001);
    assertEquals(-5.0, getDouble(batch, 1), 0.00001);
    assertEquals(bytes(), getBinary(batch, 1));
    assertEquals("bye", getText(batch, 1).toString());
    midRow = getMidList(batch, 1);
    assertNotNull(midRow);
    assertEquals(2, midRow.size());
    assertEquals(1, midRow.get(0).int1);
    assertEquals("bye", midRow.get(0).string1.toString());
    assertEquals(2, midRow.get(1).int1);
    assertEquals("sigh", midRow.get(1).string1.toString());
    list = getList(batch, 1);
    assertEquals(3, list.size());
    assertEquals(100000000, list.get(0).int1);
    assertEquals("cat", list.get(0).string1.toString());
    assertEquals(-100000, list.get(1).int1);
    assertEquals("in", list.get(1).string1.toString());
    assertEquals(1234, list.get(2).int1);
    assertEquals("hat", list.get(2).string1.toString());
    map = getMap(batch, 1);
    assertEquals(2, map.size());
    InnerStruct value = map.get(new Text("chani"));
    assertEquals(5, value.int1);
    assertEquals("chani", value.string1.toString());
    value = map.get(new Text("mauddib"));
    assertEquals(1, value.int1);
    assertEquals("mauddib", value.string1.toString());

    // handle the close up
    assertFalse(rows.nextBatch(batch));
    rows.close();
  }

  static void assertEmptyStats(ColumnStatistics[] writerStatistics) {
    for (ColumnStatistics columnStatistics : writerStatistics){
      assertEquals(0, columnStatistics.getNumberOfValues());
      assertFalse(columnStatistics.hasNull());
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testColumnProjection(Version fileFormat) throws Exception {
    TypeDescription schema = createInnerSchema();
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .setSchema(schema)
                                         .stripeSize(1000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(100)
                                         .rowIndexStride(1000)
                                         .version(fileFormat));
    VectorizedRowBatch batch = schema.createRowBatch();
    Random r1 = new Random(1);
    Random r2 = new Random(2);
    int x;
    int minInt=0, maxInt=0;
    String y;
    String minStr = null, maxStr = null;
    batch.size = 1000;
    boolean first = true;
    for(int b=0; b < 21; ++b) {
      for(int r=0; r < 1000; ++r) {
        x = r1.nextInt();
        y = Long.toHexString(r2.nextLong());
        if (first || x < minInt) {
          minInt = x;
        }
        if (first || x > maxInt) {
          maxInt = x;
        }
        if (first || y.compareTo(minStr) < 0) {
          minStr = y;
        }
        if (first || y.compareTo(maxStr) > 0) {
          maxStr = y;
        }
        first = false;
        ((LongColumnVector) batch.cols[0]).vector[r] = x;
        ((BytesColumnVector) batch.cols[1]).setVal(r, y.getBytes(StandardCharsets.UTF_8));
      }
      writer.addRowBatch(batch);
    }
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

    // check out the statistics
    ColumnStatistics[] stats = reader.getStatistics();
    assertEquals(3, stats.length);
    for(ColumnStatistics s: stats) {
      assertEquals(21000, s.getNumberOfValues());
      if (s instanceof IntegerColumnStatistics) {
        assertEquals(minInt, ((IntegerColumnStatistics) s).getMinimum());
        assertEquals(maxInt, ((IntegerColumnStatistics) s).getMaximum());
      } else if (s instanceof  StringColumnStatistics) {
        assertEquals(maxStr, ((StringColumnStatistics) s).getMaximum());
        assertEquals(minStr, ((StringColumnStatistics) s).getMinimum());
      }
    }

    // check out the types
    TypeDescription type = reader.getSchema();
    assertEquals(TypeDescription.Category.STRUCT, type.getCategory());
    assertEquals(2, type.getChildren().size());
    TypeDescription type1 = type.getChildren().get(0);
    TypeDescription type2 = type.getChildren().get(1);
    assertEquals(TypeDescription.Category.INT, type1.getCategory());
    assertEquals(TypeDescription.Category.STRING, type2.getCategory());
    assertEquals("struct<int1:int,string1:string>", type.toString());

    // read the contents and make sure they match
    RecordReader rows1 = reader.rows(
        reader.options().include(new boolean[]{true, true, false}));
    RecordReader rows2 = reader.rows(
        reader.options().include(new boolean[]{true, false, true}));
    r1 = new Random(1);
    r2 = new Random(2);
    VectorizedRowBatch batch1 = reader.getSchema().createRowBatch(1000);
    VectorizedRowBatch batch2 = reader.getSchema().createRowBatch(1000);
    for(int i = 0; i < 21000; i += 1000) {
      assertTrue(rows1.nextBatch(batch1));
      assertTrue(rows2.nextBatch(batch2));
      assertEquals(1000, batch1.size);
      assertEquals(1000, batch2.size);
      for(int j=0; j < 1000; ++j) {
        assertEquals(r1.nextInt(),
            ((LongColumnVector) batch1.cols[0]).vector[j]);
        assertEquals(Long.toHexString(r2.nextLong()),
            ((BytesColumnVector) batch2.cols[1]).toString(j));
      }
    }
    assertFalse(rows1.nextBatch(batch1));
    assertFalse(rows2.nextBatch(batch2));
    rows1.close();
    rows2.close();
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testEmptyFile(Version fileFormat) throws Exception {
    TypeDescription schema = createBigRowSchema();
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .setSchema(schema)
                                         .stripeSize(1000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(100)
                                         .version(fileFormat));
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();
    assertFalse(reader.rows().nextBatch(batch));
    assertEquals(CompressionKind.NONE, reader.getCompressionKind());
    assertEquals(0, reader.getNumberOfRows());
    assertEquals(DEFAULT_COMPRESSION_BLOCK_SIZE, reader.getCompressionSize());
    assertFalse(reader.getMetadataKeys().iterator().hasNext());
    assertEquals(3, reader.getContentLength());
    assertFalse(reader.getStripes().iterator().hasNext());
  }

  @ParameterizedTest
  @MethodSource("data")
  public void metaData(Version fileFormat) throws Exception {
    TypeDescription schema = createBigRowSchema();
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(1000)
            .compress(CompressionKind.NONE)
            .bufferSize(100)
            .version(fileFormat));
    writer.addUserMetadata("my.meta", byteBuf(1, 2, 3, 4, 5, 6, 7, -1, -2, 127,
                                              -128));
    writer.addUserMetadata("clobber", byteBuf(1, 2, 3));
    writer.addUserMetadata("clobber", byteBuf(4, 3, 2, 1));
    ByteBuffer bigBuf = ByteBuffer.allocate(40000);
    Random random = new Random(0);
    random.nextBytes(bigBuf.array());
    writer.addUserMetadata("big", bigBuf);
    bigBuf.position(0);
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 1;
    setBigRow(batch, 0, true, (byte) 127, (short) 1024, 42,
        42L * 1024 * 1024 * 1024, (float) 3.1415, -2.713, null,
        null, null, null, null);
    writer.addRowBatch(batch);
    writer.addUserMetadata("clobber", byteBuf(5,7,11,13,17,19));
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(byteBuf(5, 7, 11, 13, 17, 19), reader.getMetadataValue("clobber"));
    assertEquals(byteBuf(1, 2, 3, 4, 5, 6, 7, -1, -2, 127, -128),
        reader.getMetadataValue("my.meta"));
    assertEquals(bigBuf, reader.getMetadataValue("big"));
    try {
      reader.getMetadataValue("unknown");
      fail();
    } catch (IllegalArgumentException iae) {
      // PASS
    }
    int i = 0;
    for(String key: reader.getMetadataKeys()) {
      if ("my.meta".equals(key) ||
          "clobber".equals(key) ||
          "big".equals(key)) {
        i += 1;
      } else {
        throw new IllegalArgumentException("unknown key " + key);
      }
    }
    assertEquals(3, i);
    int numStripes = reader.getStripeStatistics().size();
    assertEquals(1, numStripes);
  }

  /**
   * Generate an ORC file with a range of dates and times.
   */
  public void createOrcDateFile(Path file, int minYear, int maxYear, Version fileFormat
                                ) throws IOException {
    TypeDescription schema = TypeDescription.createStruct()
        .addField("time", TypeDescription.createTimestamp())
        .addField("date", TypeDescription.createDate());
    Writer writer = OrcFile.createWriter(file,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(100000)
            .bufferSize(10000)
            .blockPadding(false)
            .version(fileFormat));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 1000;
    TimestampColumnVector timestampColVector = (TimestampColumnVector) batch.cols[0];
    for (int year = minYear; year < maxYear; ++year) {
      for (int row = 0; row < 1000; ++row) {
        String timeStr = String.format("%04d-05-05 12:34:56.%04d", year, 2*row);
        timestampColVector.set(row, Timestamp.valueOf(timeStr));
      }
      ((LongColumnVector) batch.cols[1]).vector[0] =
          new DateWritable(new Date(year - 1900, 11, 25)).getDays();
      batch.cols[1].isRepeating = true;
      writer.addRowBatch(batch);
    }

    // add one more row to check the statistics for the jvm bug case
    batch.size = 1;
    String timeStr = String.format("%04d-12-12 12:34:56.0001", maxYear-1);
    timestampColVector.set(0, Timestamp.valueOf(timeStr));
    writer.addRowBatch(batch);
    writer.close();

    // check the stats to make sure they match up to the millisecond
    // ORC-611 update: nanoseconds are now supported!
    ColumnStatistics[] stats = writer.getStatistics();
    TimestampColumnStatistics tsStat = (TimestampColumnStatistics) stats[1];
    assertEquals(String.format("%04d-12-12 12:34:56.0001", maxYear - 1),
        tsStat.getMaximum().toString());
    assertEquals(String.format("%04d-05-05 12:34:56.0", minYear),
        tsStat.getMinimum().toString());

    // read back the rows
    Reader reader = OrcFile.createReader(file,
        OrcFile.readerOptions(conf));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch(1000);
    TimestampColumnVector times = (TimestampColumnVector) batch.cols[0];
    LongColumnVector dates = (LongColumnVector) batch.cols[1];
    for (int year = minYear; year < maxYear; ++year) {
      rows.nextBatch(batch);
      assertEquals(1000, batch.size);
      for(int row = 0; row < 1000; ++row) {
        Timestamp expected = Timestamp.valueOf(
            String.format("%04d-05-05 12:34:56.%04d", year, 2*row));
        assertEquals(expected.getTime(), times.time[row],
            "ms row " + row + " " + expected);
        assertEquals(expected.getNanos(), times.nanos[row],
            "nanos row " + row + " " + expected);
        assertEquals(
            Integer.toString(year) + "-12-25",
            new DateWritable((int) dates.vector[row]).toString(),
            "year " + year + " row " + row);
      }
    }
    rows.nextBatch(batch);
    assertEquals(1, batch.size);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testDate1900(Version fileFormat) throws Exception {
    createOrcDateFile(testFilePath, 1900, 1970, fileFormat);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testDate2038(Version fileFormat) throws Exception {
    createOrcDateFile(testFilePath, 2038, 2250, fileFormat);
  }

  private static void setUnion(VectorizedRowBatch batch, int rowId,
                               Timestamp ts, Integer tag, Integer i, String s,
                               HiveDecimalWritable dec, Timestamp instant) {
    UnionColumnVector union = (UnionColumnVector) batch.cols[1];
    if (ts != null) {
      TimestampColumnVector timestampColVector = (TimestampColumnVector) batch.cols[0];
      timestampColVector.set(rowId, ts);
    } else {
      batch.cols[0].isNull[rowId] = true;
      batch.cols[0].noNulls = false;
    }
    if (tag != null) {
      union.tags[rowId] = tag;
      if (tag == 0) {
        if (i != null) {
          ((LongColumnVector) union.fields[tag]).vector[rowId] = i;
        } else {
          union.fields[tag].isNull[rowId] = true;
          union.fields[tag].noNulls = false;
        }
      } else if (tag == 1) {
        if (s != null) {
          ((BytesColumnVector) union.fields[tag]).setVal(rowId, s.getBytes(StandardCharsets.UTF_8));
        } else {
          union.fields[tag].isNull[rowId] = true;
          union.fields[tag].noNulls = false;
        }
      } else {
        throw new IllegalArgumentException("Bad tag " + tag);
      }
    } else {
      batch.cols[1].isNull[rowId] = true;
      batch.cols[1].noNulls = false;
    }
    if (dec != null) {
      ((DecimalColumnVector) batch.cols[2]).vector[rowId] = dec;
    } else {
      batch.cols[2].isNull[rowId] = true;
      batch.cols[2].noNulls = false;
    }
    if (instant == null) {
      batch.cols[3].isNull[rowId] = true;
      batch.cols[3].noNulls = false;
    } else {
      ((TimestampColumnVector) batch.cols[3]).set(rowId, instant);
    }
  }

  /**
   * Test writing with the new decimal and reading with the new and old.
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testDecimal64Writing(Version fileFormat) throws Exception {
    TypeDescription schema = TypeDescription.fromString("struct<d:decimal(18,3)>");
    VectorizedRowBatch batch = schema.createRowBatchV2();
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .compress(CompressionKind.NONE)
            .version(fileFormat));
    Decimal64ColumnVector cv = (Decimal64ColumnVector) batch.cols[0];
    cv.precision = 18;
    cv.scale = 3;
    cv.vector[0] = 1;
    for(int r=1; r < 18; r++) {
      cv.vector[r] = cv.vector[r-1] * 10;
    }
    cv.vector[18] = -2000;
    batch.size = 19;
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals("count: 19 hasNull: false", reader.getStatistics()[0].toString());
    // the size of the column in the different formats
    int size = (fileFormat == OrcFile.Version.V_0_11 ? 89 :
      fileFormat == OrcFile.Version.V_0_12 ? 90 : 154);
    assertEquals("count: 19 hasNull: false bytesOnDisk: " + size +
            " min: -2 max: 100000000000000 sum: 111111111111109.111",
        reader.getStatistics()[1].toString());
    RecordReader rows = reader.rows();
    batch = schema.createRowBatchV2();
    cv = (Decimal64ColumnVector) batch.cols[0];
    assertTrue(rows.nextBatch(batch));
    assertEquals(19, batch.size);
    assertEquals(18, cv.precision);
    assertEquals(3, cv.scale);
    assertEquals(1, cv.vector[0], "row 0");
    for(int r=1; r < 18; ++r) {
      assertEquals(10 * cv.vector[r-1], cv.vector[r], "row " + r);
    }
    assertEquals(-2000, cv.vector[18]);
    assertFalse(rows.nextBatch(batch));

    // test with old batch
    rows = reader.rows();
    batch = schema.createRowBatch();
    DecimalColumnVector oldCv = (DecimalColumnVector) batch.cols[0];
    assertTrue(rows.nextBatch(batch));
    assertEquals(19, batch.size);
    assertEquals(18, oldCv.precision);
    assertEquals(3, oldCv.scale);
    assertEquals("0.001", oldCv.vector[0].toString());
    assertEquals("0.01", oldCv.vector[1].toString());
    assertEquals("0.1", oldCv.vector[2].toString());
    assertEquals("1", oldCv.vector[3].toString());
    assertEquals("10", oldCv.vector[4].toString());
    assertEquals("100", oldCv.vector[5].toString());
    assertEquals("1000", oldCv.vector[6].toString());
    assertEquals("10000", oldCv.vector[7].toString());
    assertEquals("100000", oldCv.vector[8].toString());
    assertEquals("1000000", oldCv.vector[9].toString());
    assertEquals("10000000", oldCv.vector[10].toString());
    assertEquals("100000000", oldCv.vector[11].toString());
    assertEquals("1000000000", oldCv.vector[12].toString());
    assertEquals("10000000000", oldCv.vector[13].toString());
    assertEquals("100000000000", oldCv.vector[14].toString());
    assertEquals("1000000000000", oldCv.vector[15].toString());
    assertEquals("10000000000000", oldCv.vector[16].toString());
    assertEquals("100000000000000", oldCv.vector[17].toString());
    assertEquals("-2", oldCv.vector[18].toString());
    assertFalse(rows.nextBatch(batch));
  }

  /**
   * Test writing with the old decimal and reading with the new and old.
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testDecimal64Reading(Version fileFormat) throws Exception {
    TypeDescription schema = TypeDescription.fromString("struct<d:decimal(18,4)>");
    VectorizedRowBatch batch = schema.createRowBatch();
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .compress(CompressionKind.NONE)
            .version(fileFormat));
    DecimalColumnVector cv = (DecimalColumnVector) batch.cols[0];
    cv.precision = 18;
    cv.scale = 3;
    long base = 1;
    for(int r=0; r < 18; r++) {
      cv.vector[r].setFromLongAndScale(base, 4);
      base *= 10;
    }
    cv.vector[18].setFromLong(-2);
    batch.size = 19;
    writer.addRowBatch(batch);
    writer.close();

    // test with new batch
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals("count: 19 hasNull: false", reader.getStatistics()[0].toString());
    // the size of the column in the different formats
    int size = (fileFormat == OrcFile.Version.V_0_11 ? 63 :
        fileFormat == OrcFile.Version.V_0_12 ? 65 : 154);
    assertEquals("count: 19 hasNull: false bytesOnDisk: " + size +
            " min: -2 max: 10000000000000 sum: 11111111111109.1111",
        reader.getStatistics()[1].toString());

    RecordReader rows = reader.rows();
    batch = schema.createRowBatchV2();
    Decimal64ColumnVector newCv = (Decimal64ColumnVector) batch.cols[0];
    assertTrue(rows.nextBatch(batch));
    assertEquals(19, batch.size);
    assertEquals(18, newCv.precision);
    assertEquals(4, newCv.scale);
    assertEquals(1, newCv.vector[0], "row 0");
    for(int r=1; r < 18; ++r) {
      assertEquals(10 * newCv.vector[r-1], newCv.vector[r], "row " + r);
    }
    assertEquals(-20000, newCv.vector[18]);
    assertFalse(rows.nextBatch(batch));

    // test with old batch
    rows = reader.rows();
    batch = schema.createRowBatch();
    cv = (DecimalColumnVector) batch.cols[0];
    assertTrue(rows.nextBatch(batch));
    assertEquals(19, batch.size);
    assertEquals(18, cv.precision);
    assertEquals(4, cv.scale);
    assertEquals("0.0001", cv.vector[0].toString());
    assertEquals("0.001", cv.vector[1].toString());
    assertEquals("0.01", cv.vector[2].toString());
    assertEquals("0.1", cv.vector[3].toString());
    assertEquals("1", cv.vector[4].toString());
    assertEquals("10", cv.vector[5].toString());
    assertEquals("100", cv.vector[6].toString());
    assertEquals("1000", cv.vector[7].toString());
    assertEquals("10000", cv.vector[8].toString());
    assertEquals("100000", cv.vector[9].toString());
    assertEquals("1000000", cv.vector[10].toString());
    assertEquals("10000000", cv.vector[11].toString());
    assertEquals("100000000", cv.vector[12].toString());
    assertEquals("1000000000", cv.vector[13].toString());
    assertEquals("10000000000", cv.vector[14].toString());
    assertEquals("100000000000", cv.vector[15].toString());
    assertEquals("1000000000000", cv.vector[16].toString());
    assertEquals("10000000000000", cv.vector[17].toString());
    assertEquals("-2", cv.vector[18].toString());
    assertFalse(rows.nextBatch(batch));
  }

  /**
     * We test union, timestamp, and decimal separately since we need to make the
     * object inspector manually. (The Hive reflection-based doesn't handle
     * them properly.)
     */
  @ParameterizedTest
  @MethodSource("data")
  public void testUnionAndTimestamp(Version fileFormat) throws Exception {
    final TimeZone original = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
    TypeDescription schema = TypeDescription.fromString(
        "struct<time:timestamp," +
               "union:uniontype<int,string>," +
               "decimal:decimal(38,18)," +
               "instant:timestamp with local time zone>"
    );
    HiveDecimal maxValue = HiveDecimal.create("10000000000000000000");
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .setSchema(schema)
                                         .stripeSize(1000)
                                         .compress(CompressionKind.NONE)
                                         .bufferSize(100)
                                         .blockPadding(false)
                                         .version(fileFormat));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 6;
    setUnion(batch, 0, Timestamp.valueOf("2000-03-12 15:00:00"), 0, 42, null,
             new HiveDecimalWritable("12345678.6547456"),
             Timestamp.valueOf("2014-12-12 6:00:00"));
    setUnion(batch, 1, Timestamp.valueOf("2000-03-20 12:00:00.123456789"),
        1, null, "hello", new HiveDecimalWritable("-5643.234"),
        Timestamp.valueOf("1996-12-11 11:00:00"));

    setUnion(batch, 2, null, null, null, null, null, null);
    setUnion(batch, 3, null, 0, null, null, null, null);
    setUnion(batch, 4, null, 1, null, null, null, null);

    setUnion(batch, 5, Timestamp.valueOf("1970-01-01 00:00:00"), 0, 200000,
        null, new HiveDecimalWritable("10000000000000000000"),
        Timestamp.valueOf("2011-07-01 09:00:00"));
    writer.addRowBatch(batch);

    batch.reset();
    Random rand = new Random(42);
    for(int i=1970; i < 2038; ++i) {
      Timestamp ts = Timestamp.valueOf(i + "-05-05 12:34:56." + i);
      HiveDecimal dec =
          HiveDecimal.create(new BigInteger(64, rand), rand.nextInt(18));
      if ((i & 1) == 0) {
        setUnion(batch, batch.size++, ts, 0, i*i, null,
            new HiveDecimalWritable(dec), null);
      } else {
        setUnion(batch, batch.size++, ts, 1, null, Integer.toString(i*i),
            new HiveDecimalWritable(dec), null);
      }
      if (maxValue.compareTo(dec) < 0) {
        maxValue = dec;
      }
    }
    writer.addRowBatch(batch);
    batch.reset();

    // let's add a lot of constant rows to test the rle
    batch.size = 1000;
    for(int c=0; c < batch.cols.length; ++c) {
      batch.cols[c].setRepeating(true);
    }
    ((UnionColumnVector) batch.cols[1]).fields[0].isRepeating = true;
    setUnion(batch, 0, null, 0, 1732050807, null, null, null);
    for(int i=0; i < 5; ++i) {
      writer.addRowBatch(batch);
    }

    batch.reset();
    batch.size = 3;
    setUnion(batch, 0, null, 0, 0, null, null, null);
    setUnion(batch, 1, null, 0, 10, null, null, null);
    setUnion(batch, 2, null, 0, 138, null, null, null);
    writer.addRowBatch(batch);
    // check the stats on the writer side
    ColumnStatistics[] stats = writer.getStatistics();
    assertEquals("1996-12-11 11:00:00.0",
        ((TimestampColumnStatistics) stats[6]).getMinimum().toString());
    assertEquals("1996-12-11 11:00:00.0",
        ((TimestampColumnStatistics) stats[6]).getMinimumUTC().toString());
    assertEquals("2014-12-12 06:00:00.0",
        ((TimestampColumnStatistics) stats[6]).getMaximum().toString());
    assertEquals("2014-12-12 06:00:00.0",
        ((TimestampColumnStatistics) stats[6]).getMaximumUTC().toString());

    writer.close();

    TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    stats = reader.getStatistics();

    // check the timestamp statistics
    assertEquals("1970-01-01 00:00:00.0",
        ((TimestampColumnStatistics) stats[1]).getMinimum().toString());
    assertEquals("1969-12-31 19:00:00.0",
        ((TimestampColumnStatistics) stats[1]).getMinimumUTC().toString());
    assertEquals("2037-05-05 12:34:56.2037",
        ((TimestampColumnStatistics) stats[1]).getMaximum().toString());
    assertEquals("2037-05-05 08:34:56.2037",
        ((TimestampColumnStatistics) stats[1]).getMaximumUTC().toString());

    // check the instant statistics
    assertEquals("1996-12-11 14:00:00.0",
        ((TimestampColumnStatistics) stats[6]).getMinimum().toString());
    assertEquals("1996-12-11 14:00:00.0",
        ((TimestampColumnStatistics) stats[6]).getMinimumUTC().toString());
    assertEquals("2014-12-12 09:00:00.0",
        ((TimestampColumnStatistics) stats[6]).getMaximum().toString());
    assertEquals("2014-12-12 09:00:00.0",
        ((TimestampColumnStatistics) stats[6]).getMaximumUTC().toString());


    schema = writer.getSchema();
    assertEquals(6, schema.getMaximumId());
    boolean[] expected = new boolean[] {false, false, false, false, false, false, false};
    boolean[] included = OrcUtils.includeColumns("", schema);
    assertTrue(Arrays.equals(expected, included));

    expected = new boolean[] {false, true, false, false, false, true, false};
    included = OrcUtils.includeColumns("time,decimal", schema);
    assertTrue(Arrays.equals(expected, included));

    expected = new boolean[] {false, false, true, true, true, false, false};
    included = OrcUtils.includeColumns("union", schema);
    assertTrue(Arrays.equals(expected, included));

    assertFalse(reader.getMetadataKeys().iterator().hasNext());
    assertEquals(5077, reader.getNumberOfRows());
    DecimalColumnStatistics decStats =
        (DecimalColumnStatistics) reader.getStatistics()[5];
    assertEquals(71, decStats.getNumberOfValues());
    assertEquals(HiveDecimal.create("-5643.234"), decStats.getMinimum());
    assertEquals(maxValue, decStats.getMaximum());
    // TODO: fix this
//    assertEquals(null,stats.getSum());
    int stripeCount = 0;
    int rowCount = 0;
    long currentOffset = -1;
    for(StripeInformation stripe: reader.getStripes()) {
      stripeCount += 1;
      rowCount += stripe.getNumberOfRows();
      if (currentOffset < 0) {
        currentOffset = stripe.getOffset() + stripe.getLength();
      } else {
        assertEquals(currentOffset, stripe.getOffset());
        currentOffset += stripe.getLength();
      }
    }
    assertEquals(reader.getNumberOfRows(), rowCount);
    assertEquals(2, stripeCount);
    assertEquals(reader.getContentLength(), currentOffset);
    RecordReader rows = reader.rows();
    assertEquals(0, rows.getRowNumber());
    assertEquals(0.0, rows.getProgress(), 0.000001);

    schema = reader.getSchema();
    batch = schema.createRowBatch(74);
    assertEquals(0, rows.getRowNumber());
    rows.nextBatch(batch);
    assertEquals(74, batch.size);
    assertEquals(74, rows.getRowNumber());
    TimestampColumnVector ts = (TimestampColumnVector) batch.cols[0];
    UnionColumnVector union = (UnionColumnVector) batch.cols[1];
    LongColumnVector longs = (LongColumnVector) union.fields[0];
    BytesColumnVector strs = (BytesColumnVector) union.fields[1];
    DecimalColumnVector decs = (DecimalColumnVector) batch.cols[2];
    TimestampColumnVector instant = (TimestampColumnVector) batch.cols[3];

    assertEquals("struct<time:timestamp,union:uniontype<int,string>,decimal:decimal(38,18)," +
                     "instant:timestamp with local time zone>",
        schema.toString());
    assertEquals("2000-03-12 15:00:00.0", ts.asScratchTimestamp(0).toString());
    assertEquals(0, union.tags[0]);
    assertEquals(42, longs.vector[0]);
    assertEquals("12345678.6547456", decs.vector[0].toString());
    assertEquals("2014-12-12 09:00:00.0", instant.asScratchTimestamp(0).toString());

    assertEquals("2000-03-20 12:00:00.123456789", ts.asScratchTimestamp(1).toString());
    assertEquals(1, union.tags[1]);
    assertEquals("hello", strs.toString(1));
    assertEquals("-5643.234", decs.vector[1].toString());
    assertEquals("1996-12-11 14:00:00.0", instant.asScratchTimestamp(1).toString());

    assertFalse(ts.noNulls);
    assertFalse(union.noNulls);
    assertFalse(decs.noNulls);
    assertTrue(ts.isNull[2]);
    assertTrue(union.isNull[2]);
    assertTrue(decs.isNull[2]);

    assertTrue(ts.isNull[3]);
    assertFalse(union.isNull[3]);
    assertEquals(0, union.tags[3]);
    assertTrue(longs.isNull[3]);
    assertTrue(decs.isNull[3]);

    assertTrue(ts.isNull[4]);
    assertFalse(union.isNull[4]);
    assertEquals(1, union.tags[4]);
    assertTrue(strs.isNull[4]);
    assertTrue(decs.isNull[4]);

    assertFalse(ts.isNull[5]);
    assertEquals("1970-01-01 00:00:00.0", ts.asScratchTimestamp(5).toString());
    assertFalse(union.isNull[5]);
    assertEquals(0, union.tags[5]);
    assertFalse(longs.isNull[5]);
    assertEquals(200000, longs.vector[5]);
    assertFalse(decs.isNull[5]);
    assertEquals("10000000000000000000", decs.vector[5].toString());
    assertEquals("2011-07-01 12:00:00.0", instant.asScratchTimestamp(5).toString());

    rand = new Random(42);
    for(int i=1970; i < 2038; ++i) {
      int row = 6 + i - 1970;
      assertEquals(Timestamp.valueOf(i + "-05-05 12:34:56." + i),
          ts.asScratchTimestamp(row));
      if ((i & 1) == 0) {
        assertEquals(0, union.tags[row]);
        assertEquals(i*i, longs.vector[row]);
      } else {
        assertEquals(1, union.tags[row]);
        assertEquals(Integer.toString(i * i), strs.toString(row));
      }
      assertEquals(new HiveDecimalWritable(HiveDecimal.create(new BigInteger(64, rand),
                                   rand.nextInt(18))), decs.vector[row]);
    }

    // rebuild the row batch, so that we can read by 1000 rows
    batch = schema.createRowBatch(1000);
    ts = (TimestampColumnVector) batch.cols[0];
    union = (UnionColumnVector) batch.cols[1];
    longs = (LongColumnVector) union.fields[0];
    strs = (BytesColumnVector) union.fields[1];
    decs = (DecimalColumnVector) batch.cols[2];

    for(int i=0; i < 5; ++i) {
      rows.nextBatch(batch);
      String msg = "batch " + i;
      assertEquals(1000, batch.size, msg);
      assertFalse(union.isRepeating, msg);
      assertTrue(union.noNulls, msg);
      for(int r=0; r < batch.size; ++r) {
        assertEquals(0, union.tags[r], "bad tag at " + i + "." + r);
      }
      assertTrue(longs.isRepeating, msg);
      assertEquals(1732050807, longs.vector[0], msg);
    }

    rows.nextBatch(batch);
    assertEquals(3, batch.size);
    assertEquals(0, union.tags[0]);
    assertEquals(0, longs.vector[0]);
    assertEquals(0, union.tags[1]);
    assertEquals(10, longs.vector[1]);
    assertEquals(0, union.tags[2]);
    assertEquals(138, longs.vector[2]);

    rows.nextBatch(batch);
    assertEquals(0, batch.size);
    assertEquals(1.0, rows.getProgress(), 0.00001);
    assertEquals(reader.getNumberOfRows(), rows.getRowNumber());
    rows.seekToRow(1);
    rows.nextBatch(batch);
    assertEquals(1000, batch.size);
    assertEquals(Timestamp.valueOf("2000-03-20 12:00:00.123456789"), ts.asScratchTimestamp(0));
    assertEquals(1, union.tags[0]);
    assertEquals("hello", strs.toString(0));
    assertEquals(new HiveDecimalWritable(HiveDecimal.create("-5643.234")), decs.vector[0]);
    rows.close();
    TimeZone.setDefault(original);
  }

  /**
   * Read and write a randomly generated snappy file.
   * @throws Exception
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testSnappy(Version fileFormat) throws Exception {
    TypeDescription schema = createInnerSchema();
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .setSchema(schema)
                                         .stripeSize(1000)
                                         .compress(CompressionKind.SNAPPY)
                                         .bufferSize(100)
                                         .version(fileFormat));
    VectorizedRowBatch batch = schema.createRowBatch();
    Random rand;
    writeRandomIntBytesBatches(writer, batch, 10, 1000);
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(CompressionKind.SNAPPY, reader.getCompressionKind());
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch(1000);
    rand = new Random(12);
    LongColumnVector longs = (LongColumnVector) batch.cols[0];
    BytesColumnVector strs = (BytesColumnVector) batch.cols[1];
    for(int b=0; b < 10; ++b) {
      rows.nextBatch(batch);
      assertEquals(1000, batch.size);
      for(int r=0; r < batch.size; ++r) {
        assertEquals(rand.nextInt(), longs.vector[r]);
        assertEquals(Integer.toHexString(rand.nextInt()), strs.toString(r));
      }
    }
    rows.nextBatch(batch);
    assertEquals(0, batch.size);
    rows.close();
  }

  /**
   * Read and write a randomly generated lzo file.
   * @throws Exception
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testLzo(Version fileFormat) throws Exception {
    TypeDescription schema =
        TypeDescription.fromString("struct<x:bigint,y:int,z:bigint>");
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(10000)
            .compress(CompressionKind.LZO)
            .bufferSize(1000)
            .version(fileFormat));
    VectorizedRowBatch batch = schema.createRowBatch();
    Random rand = new Random(69);
    batch.size = 1000;
    for(int b=0; b < 10; ++b) {
      for (int r=0; r < 1000; ++r) {
        ((LongColumnVector) batch.cols[0]).vector[r] = rand.nextInt();
        ((LongColumnVector) batch.cols[1]).vector[r] = b * 1000 + r;
        ((LongColumnVector) batch.cols[2]).vector[r] = rand.nextLong();
      }
      writer.addRowBatch(batch);
    }
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(CompressionKind.LZO, reader.getCompressionKind());
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch(1000);
    rand = new Random(69);
    for(int b=0; b < 10; ++b) {
      rows.nextBatch(batch);
      assertEquals(1000, batch.size);
      for(int r=0; r < batch.size; ++r) {
        assertEquals(rand.nextInt(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
        assertEquals(b * 1000 + r,
            ((LongColumnVector) batch.cols[1]).vector[r]);
        assertEquals(rand.nextLong(),
            ((LongColumnVector) batch.cols[2]).vector[r]);
      }
    }
    rows.nextBatch(batch);
    assertEquals(0, batch.size);
    rows.close();
  }

  /**
   * Read and write a randomly generated lz4 file.
   * @throws Exception
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testLz4(Version fileFormat) throws Exception {
    TypeDescription schema =
        TypeDescription.fromString("struct<x:bigint,y:int,z:bigint>");
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(10000)
            .compress(CompressionKind.LZ4)
            .bufferSize(1000)
            .version(fileFormat));
    VectorizedRowBatch batch = schema.createRowBatch();
    Random rand = new Random(3);
    batch.size = 1000;
    for(int b=0; b < 10; ++b) {
      for (int r=0; r < 1000; ++r) {
        ((LongColumnVector) batch.cols[0]).vector[r] = rand.nextInt();
        ((LongColumnVector) batch.cols[1]).vector[r] = b * 1000 + r;
        ((LongColumnVector) batch.cols[2]).vector[r] = rand.nextLong();
      }
      writer.addRowBatch(batch);
    }
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(CompressionKind.LZ4, reader.getCompressionKind());
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch(1000);
    rand = new Random(3);
    for(int b=0; b < 10; ++b) {
      rows.nextBatch(batch);
      assertEquals(1000, batch.size);
      for(int r=0; r < batch.size; ++r) {
        assertEquals(rand.nextInt(),
            ((LongColumnVector) batch.cols[0]).vector[r]);
        assertEquals(b * 1000 + r,
            ((LongColumnVector) batch.cols[1]).vector[r]);
        assertEquals(rand.nextLong(),
            ((LongColumnVector) batch.cols[2]).vector[r]);
      }
    }
    rows.nextBatch(batch);
    assertEquals(0, batch.size);
    rows.close();
  }

  /**
   * Read and write a randomly generated zstd file.
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testZstd(Version fileFormat) throws Exception {
    TypeDescription schema =
        TypeDescription.fromString("struct<x:bigint,y:int,z:bigint>");
    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .compress(CompressionKind.ZSTD)
            .bufferSize(1000)
            .version(fileFormat))) {
      VectorizedRowBatch batch = schema.createRowBatch();
      Random rand = new Random(3);
      batch.size = 1000;
      for (int b = 0; b < 10; ++b) {
        for (int r = 0; r < 1000; ++r) {
          ((LongColumnVector) batch.cols[0]).vector[r] = rand.nextInt();
          ((LongColumnVector) batch.cols[1]).vector[r] = b * 1000 + r;
          ((LongColumnVector) batch.cols[2]).vector[r] = rand.nextLong();
        }
        writer.addRowBatch(batch);
      }
    }
    try (Reader reader = OrcFile.createReader(testFilePath,
           OrcFile.readerOptions(conf).filesystem(fs));
         RecordReader rows = reader.rows()) {
      assertEquals(CompressionKind.ZSTD, reader.getCompressionKind());
      VectorizedRowBatch batch = reader.getSchema().createRowBatch(1000);
      Random rand = new Random(3);
      for (int b = 0; b < 10; ++b) {
        rows.nextBatch(batch);
        assertEquals(1000, batch.size);
        for (int r = 0; r < batch.size; ++r) {
          assertEquals(rand.nextInt(),
              ((LongColumnVector) batch.cols[0]).vector[r]);
          assertEquals(b * 1000 + r,
              ((LongColumnVector) batch.cols[1]).vector[r]);
          assertEquals(rand.nextLong(),
              ((LongColumnVector) batch.cols[2]).vector[r]);
        }
      }
      rows.nextBatch(batch);
      assertEquals(0, batch.size);
    }
  }

  /**
   * Read and write a file; verify codec usage.
   * @throws Exception
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testCodecPool(Version fileFormat) throws Exception {
    OrcCodecPool.clear();
    TypeDescription schema = createInnerSchema();
    VectorizedRowBatch batch = schema.createRowBatch();
    WriterOptions opts = OrcFile.writerOptions(conf)
        .setSchema(schema).stripeSize(1000).bufferSize(100).version(fileFormat);

    CompressionCodec snappyCodec, zlibCodec;
    snappyCodec = writeBatchesAndGetCodec(10, 1000, opts.compress(CompressionKind.SNAPPY), batch);
    assertEquals(1, OrcCodecPool.getPoolSize(CompressionKind.SNAPPY));
    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(CompressionKind.SNAPPY, reader.getCompressionKind());
    CompressionCodec codec = readBatchesAndGetCodec(reader, 10, 1000);
    assertEquals(1, OrcCodecPool.getPoolSize(CompressionKind.SNAPPY));
    assertSame(snappyCodec, codec);

    reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(CompressionKind.SNAPPY, reader.getCompressionKind());
    codec = readBatchesAndGetCodec(reader, 10, 1000);
    assertSame(snappyCodec, codec);
    assertEquals(1, OrcCodecPool.getPoolSize(CompressionKind.SNAPPY));

    zlibCodec = writeBatchesAndGetCodec(10, 1000, opts.compress(CompressionKind.ZLIB), batch);
    assertNotSame(snappyCodec, zlibCodec);
    assertEquals(1, OrcCodecPool.getPoolSize(CompressionKind.ZLIB));
    codec = writeBatchesAndGetCodec(10, 1000, opts.compress(CompressionKind.ZLIB), batch);
    assertEquals(1, OrcCodecPool.getPoolSize(CompressionKind.ZLIB));
    assertSame(zlibCodec, codec);

    assertSame(snappyCodec, OrcCodecPool.getCodec(CompressionKind.SNAPPY));
    CompressionCodec snappyCodec2 = writeBatchesAndGetCodec(
        10, 1000, opts.compress(CompressionKind.SNAPPY), batch);
    assertNotSame(snappyCodec, snappyCodec2);
    OrcCodecPool.returnCodec(CompressionKind.SNAPPY, snappyCodec);
    reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(CompressionKind.SNAPPY, reader.getCompressionKind());
    codec = readBatchesAndGetCodec(reader, 10, 1000);
    assertEquals(2, OrcCodecPool.getPoolSize(CompressionKind.SNAPPY));
    assertTrue(snappyCodec == codec || snappyCodec2 == codec);
  }

  private CompressionCodec writeBatchesAndGetCodec(int count,
                                                   int size,
                                                   WriterOptions opts,
                                                   VectorizedRowBatch batch
                                                   ) throws IOException {
    fs.delete(testFilePath, false);
    Writer writer = OrcFile.createWriter(testFilePath, opts);
    CompressionCodec codec = ((WriterImpl) writer).getCompressionCodec();
    writeRandomIntBytesBatches(writer, batch, count, size);
    writer.close();
    return codec;
  }

  private CompressionCodec readBatchesAndGetCodec(
      Reader reader, int count, int size) throws IOException {
    RecordReader rows = reader.rows();
    VectorizedRowBatch batch = reader.getSchema().createRowBatch(size);
    for (int b = 0; b < count; ++b) {
      rows.nextBatch(batch);
    }
    CompressionCodec codec = ((RecordReaderImpl)rows).getCompressionCodec();
    rows.close();
    return codec;
  }

  private void readRandomBatches(
      Reader reader, RecordReader rows, int count, int size) throws IOException {

  }

  private void writeRandomIntBytesBatches(
      Writer writer, VectorizedRowBatch batch, int count, int size) throws IOException {
    Random rand = new Random(12);
    batch.size = size;
    for(int b=0; b < count; ++b) {
      for (int r=0; r < size; ++r) {
        ((LongColumnVector) batch.cols[0]).vector[r] = rand.nextInt();
        ((BytesColumnVector) batch.cols[1]).setVal(r,
            Integer.toHexString(rand.nextInt()).getBytes(StandardCharsets.UTF_8));
      }
      writer.addRowBatch(batch);
    }
  }

  /**
   * Read and write a randomly generated snappy file.
   * @throws Exception
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testWithoutIndex(Version fileFormat) throws Exception {
    TypeDescription schema = createInnerSchema();
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .setSchema(schema)
                                         .stripeSize(5000)
                                         .compress(CompressionKind.SNAPPY)
                                         .bufferSize(1000)
                                         .rowIndexStride(0)
                                         .version(fileFormat));
    VectorizedRowBatch batch = schema.createRowBatch();
    Random rand = new Random(24);
    batch.size = 5;
    for(int c=0; c < batch.cols.length; ++c) {
      batch.cols[c].setRepeating(true);
    }
    for(int i=0; i < 10000; ++i) {
      ((LongColumnVector) batch.cols[0]).vector[0] = rand.nextInt();
      ((BytesColumnVector) batch.cols[1])
          .setVal(0, Integer.toBinaryString(rand.nextInt()).getBytes(StandardCharsets.UTF_8));
      writer.addRowBatch(batch);
    }
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(50000, reader.getNumberOfRows());
    assertEquals(0, reader.getRowIndexStride());
    StripeInformation stripe = reader.getStripes().iterator().next();
    assertTrue(stripe.getDataLength() != 0);
    assertEquals(0, stripe.getIndexLength());
    RecordReader rows = reader.rows();
    rand = new Random(24);
    batch = reader.getSchema().createRowBatch(1000);
    LongColumnVector longs = (LongColumnVector) batch.cols[0];
    BytesColumnVector strs = (BytesColumnVector) batch.cols[1];
    for(int i=0; i < 50; ++i) {
      rows.nextBatch(batch);
      assertEquals(1000, batch.size, "batch " + i);
      for(int j=0; j < 200; ++j) {
        int intVal = rand.nextInt();
        String strVal = Integer.toBinaryString(rand.nextInt());
        for (int k = 0; k < 5; ++k) {
          assertEquals(intVal, longs.vector[j * 5 + k]);
          assertEquals(strVal, strs.toString(j * 5 + k));
        }
      }
    }
    rows.nextBatch(batch);
    assertEquals(0, batch.size);
    rows.close();
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testSeek(Version fileFormat) throws Exception {
    TypeDescription schema = createBigRowSchema();
    Writer writer = OrcFile.createWriter(testFilePath,
                                         OrcFile.writerOptions(conf)
                                         .setSchema(schema)
                                         .stripeSize(200000)
                                         .bufferSize(65536)
                                         .rowIndexStride(1000)
                                         .version(fileFormat));
    VectorizedRowBatch batch = schema.createRowBatch();
    Random rand = new Random(42);
    final int COUNT=32768;
    long[] intValues= new long[COUNT];
    double[] doubleValues = new double[COUNT];
    String[] stringValues = new String[COUNT];
    BytesWritable[] byteValues = new BytesWritable[COUNT];
    String[] words = new String[128];
    for(int i=0; i < words.length; ++i) {
      words[i] = Integer.toHexString(rand.nextInt());
    }
    for(int i=0; i < COUNT/2; ++i) {
      intValues[2*i] = rand.nextLong();
      intValues[2*i+1] = intValues[2*i];
      stringValues[2*i] = words[rand.nextInt(words.length)];
      stringValues[2*i+1] = stringValues[2*i];
    }
    for(int i=0; i < COUNT; ++i) {
      doubleValues[i] = rand.nextDouble();
      byte[] buf = new byte[20];
      rand.nextBytes(buf);
      byteValues[i] = new BytesWritable(buf);
    }
    for(int i=0; i < COUNT; ++i) {
      appendRandomRow(batch, intValues, doubleValues, stringValues,
          byteValues, words, i);
      if (batch.size == 1024) {
        writer.addRowBatch(batch);
        batch.reset();
      }
    }
    if (batch.size != 0) {
      writer.addRowBatch(batch);
    }
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(COUNT, reader.getNumberOfRows());
    RecordReader rows = reader.rows();
    // get the row index
    InStream.StreamOptions options = InStream.options();
    if (reader.getCompressionKind() != CompressionKind.NONE) {
      options.withCodec(OrcCodecPool.getCodec(reader.getCompressionKind()))
          .withBufferSize(reader.getCompressionSize());
    }
    DataReader meta = RecordReaderUtils.createDefaultDataReader(
        DataReaderProperties.builder()
            .withFileSystem(fs)
            .withPath(testFilePath)
            .withCompression(options)
            .withZeroCopy(false)
            .build());
    StripePlanner planner = new StripePlanner(schema, new ReaderEncryption(),
        meta, reader.getWriterVersion(), true, Integer.MAX_VALUE);
    boolean[] columns = new boolean[schema.getMaximumId() + 1];
    Arrays.fill(columns, true);
    OrcIndex index = planner.parseStripe(reader.getStripes().get(0), columns)
                         .readRowIndex(null, null);
    // check the primitive columns to make sure they have the right number of
    // items in the first row group
    for(int c=1; c < 9; ++c) {
      OrcProto.RowIndex colIndex = index.getRowGroupIndex()[c];
      assertEquals(1000,
          colIndex.getEntry(0).getStatistics().getNumberOfValues());
    }
    batch = reader.getSchema().createRowBatch();
    int nextRowInBatch = -1;
    for(int i=COUNT-1; i >= 0; --i, --nextRowInBatch) {
      // if we have consumed the previous batch read a new one
      if (nextRowInBatch < 0) {
        long base = Math.max(i - 1023, 0);
        rows.seekToRow(base);
        assertTrue(rows.nextBatch(batch), "row " + i);
        nextRowInBatch = batch.size - 1;
      }
      checkRandomRow(batch, intValues, doubleValues,
          stringValues, byteValues, words, i, nextRowInBatch);
    }
    rows.close();
    Iterator<StripeInformation> stripeIterator =
      reader.getStripes().iterator();
    long offsetOfStripe2 = 0;
    long offsetOfStripe4 = 0;
    long lastRowOfStripe2 = 0;
    for(int i = 0; i < 5; ++i) {
      StripeInformation stripe = stripeIterator.next();
      if (i < 2) {
        lastRowOfStripe2 += stripe.getNumberOfRows();
      } else if (i == 2) {
        offsetOfStripe2 = stripe.getOffset();
        lastRowOfStripe2 += stripe.getNumberOfRows() - 1;
      } else if (i == 4) {
        offsetOfStripe4 = stripe.getOffset();
      }
    }
    Arrays.fill(columns, false);
    columns[5] = true; // long colulmn
    columns[9] = true; // text column
    rows = reader.rows(reader.options()
        .range(offsetOfStripe2, offsetOfStripe4 - offsetOfStripe2)
        .include(columns));
    rows.seekToRow(lastRowOfStripe2);
    // we only want two rows
    batch = reader.getSchema().createRowBatch(2);
    assertTrue(rows.nextBatch(batch));
    assertEquals(1, batch.size);
    assertEquals(intValues[(int) lastRowOfStripe2], getLong(batch, 0));
    assertEquals(stringValues[(int) lastRowOfStripe2],
        getText(batch, 0).toString());
    assertTrue(rows.nextBatch(batch));
    assertEquals(intValues[(int) lastRowOfStripe2 + 1], getLong(batch, 0));
    assertEquals(stringValues[(int) lastRowOfStripe2 + 1],
        getText(batch, 0).toString());
    rows.close();
  }

  private void appendRandomRow(VectorizedRowBatch batch,
                               long[] intValues, double[] doubleValues,
                               String[] stringValues,
                               BytesWritable[] byteValues,
                               String[] words, int i) {
    InnerStruct inner = new InnerStruct((int) intValues[i], stringValues[i]);
    InnerStruct inner2 = new InnerStruct((int) (intValues[i] >> 32),
        words[i % words.length] + "-x");
    setBigRow(batch, batch.size++, (intValues[i] & 1) == 0, (byte) intValues[i],
        (short) intValues[i], (int) intValues[i], intValues[i],
        (float) doubleValues[i], doubleValues[i], byteValues[i], stringValues[i],
        new MiddleStruct(inner, inner2), list(), map(inner, inner2));
  }

  private void checkRandomRow(VectorizedRowBatch batch,
                              long[] intValues, double[] doubleValues,
                              String[] stringValues,
                              BytesWritable[] byteValues,
                              String[] words, int i, int rowInBatch) {
    InnerStruct inner = new InnerStruct((int) intValues[i], stringValues[i]);
    InnerStruct inner2 = new InnerStruct((int) (intValues[i] >> 32),
        words[i % words.length] + "-x");
    checkBigRow(batch, rowInBatch, i, (intValues[i] & 1) == 0, (byte) intValues[i],
        (short) intValues[i], (int) intValues[i], intValues[i],
        (float) doubleValues[i], doubleValues[i], byteValues[i], stringValues[i],
        new MiddleStruct(inner, inner2), list(), map(inner, inner2));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testMemoryManagement(Version fileFormat) throws Exception {
    OrcConf.ROWS_BETWEEN_CHECKS.setLong(conf, 100);
    final long POOL_SIZE = 50_000;
    TypeDescription schema = createInnerSchema();
    MemoryManagerImpl memoryMgr = new MemoryManagerImpl(POOL_SIZE);

    // set up 10 files that all request the full size.
    MemoryManager.Callback ignore = newScale -> false;
    for(int f=0; f < 9; ++f) {
      memoryMgr.addWriter(new Path("file-" + f), POOL_SIZE, ignore);
    }

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .compress(CompressionKind.NONE)
            .stripeSize(POOL_SIZE)
            .bufferSize(100)
            .rowIndexStride(0)
            .memory(memoryMgr)
            .version(fileFormat));
    // check to make sure it is 10%
    assertEquals(0.1, memoryMgr.getAllocationScale(), 0.001);
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 1;
    for(int i=0; i < 2500; ++i) {
      ((LongColumnVector) batch.cols[0]).vector[0] = i * 300;
      ((BytesColumnVector) batch.cols[1]).setVal(0,
          Integer.toHexString(10*i).getBytes(StandardCharsets.UTF_8));
      writer.addRowBatch(batch);
    }
    writer.close();
    assertEquals(0.111, memoryMgr.getAllocationScale(), 0.001);
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    int i = 0;
    for(StripeInformation stripe: reader.getStripes()) {
      i += 1;
      assertTrue(stripe.getDataLength() < POOL_SIZE,
          "stripe " + i + " is too long at " + stripe.getDataLength());
    }
    // 0.11 always uses the dictionary, so ends up with a lot more stripes
    assertEquals(fileFormat == OrcFile.Version.V_0_11 ? 25 : 3, i);
    assertEquals(2500, reader.getNumberOfRows());
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testPredicatePushdown(Version fileFormat) throws Exception {
    TypeDescription schema = createInnerSchema();
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(400000L)
            .compress(CompressionKind.NONE)
            .bufferSize(500)
            .rowIndexStride(1000)
            .version(fileFormat));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.ensureSize(3500);
    batch.size = 3500;
    for(int i=0; i < 3500; ++i) {
      ((LongColumnVector) batch.cols[0]).vector[i] = i * 300;
      ((BytesColumnVector) batch.cols[1]).setVal(i,
          Integer.toHexString(10*i).getBytes(StandardCharsets.UTF_8));
    }
    writer.addRowBatch(batch);
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(3500, reader.getNumberOfRows());

    SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startAnd()
          .startNot()
             .lessThan("int1", PredicateLeaf.Type.LONG, 300000L)
          .end()
          .lessThan("int1", PredicateLeaf.Type.LONG, 600000L)
        .end()
        .build();
    RecordReader rows = reader.rows(reader.options()
        .range(0L, Long.MAX_VALUE)
        .include(new boolean[]{true, true, true})
        .searchArgument(sarg, new String[]{null, "int1", "string1"}));
    batch = reader.getSchema().createRowBatch(2000);
    LongColumnVector ints = (LongColumnVector) batch.cols[0];
    BytesColumnVector strs = (BytesColumnVector) batch.cols[1];

    assertEquals(1000L, rows.getRowNumber());
    assertTrue(rows.nextBatch(batch));
    assertEquals(1000, batch.size);

    for(int i=1000; i < 2000; ++i) {
      assertEquals(300 * i, ints.vector[i - 1000]);
      assertEquals(Integer.toHexString(10*i), strs.toString(i - 1000));
    }
    assertFalse(rows.nextBatch(batch));
    assertEquals(3500, rows.getRowNumber());

    // look through the file with no rows selected
    sarg = SearchArgumentFactory.newBuilder()
        .startAnd()
          .lessThan("int1", PredicateLeaf.Type.LONG, 0L)
        .end()
        .build();
    rows = reader.rows(reader.options()
        .range(0L, Long.MAX_VALUE)
        .include(new boolean[]{true, true, true})
        .searchArgument(sarg, new String[]{null, "int1", "string1"}));
    assertEquals(3500L, rows.getRowNumber());
    assertFalse(rows.nextBatch(batch));

    // select first 100 and last 100 rows
    sarg = SearchArgumentFactory.newBuilder()
        .startOr()
          .lessThan("int1", PredicateLeaf.Type.LONG, 300L * 100)
          .startNot()
            .lessThan("int1", PredicateLeaf.Type.LONG, 300L * 3400)
          .end()
        .end()
        .build();
    rows = reader.rows(reader.options()
        .range(0L, Long.MAX_VALUE)
        .include(new boolean[]{true, true, true})
        .searchArgument(sarg, new String[]{null, "int1", "string1"})
        .allowSARGToFilter(false));
    assertEquals(0, rows.getRowNumber());
    assertTrue(rows.nextBatch(batch));
    assertEquals(1000, batch.size);
    assertEquals(3000, rows.getRowNumber());
    for(int i=0; i < 1000; ++i) {
      assertEquals(300 * i, ints.vector[i]);
      assertEquals(Integer.toHexString(10*i), strs.toString(i));
    }

    assertTrue(rows.nextBatch(batch));
    assertEquals(500, batch.size);
    assertEquals(3500, rows.getRowNumber());
    for(int i=3000; i < 3500; ++i) {
      assertEquals(300 * i, ints.vector[i - 3000]);
      assertEquals(Integer.toHexString(10*i), strs.toString(i - 3000));
    }
    assertFalse(rows.nextBatch(batch));
    assertEquals(3500, rows.getRowNumber());
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testQuotedPredicatePushdown(Version fileFormat) throws Exception {
    TypeDescription schema = createQuotedSchema();
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(400000L)
            .compress(CompressionKind.NONE)
            .bufferSize(500)
            .rowIndexStride(1000)
            .version(fileFormat));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.ensureSize(3500);
    batch.size = 3500;
    for(int i=0; i < 3500; ++i) {
      ((LongColumnVector) batch.cols[0]).vector[i] = i * 300;
      ((BytesColumnVector) batch.cols[1]).setVal(i,
          Integer.toHexString(10*i).getBytes(StandardCharsets.UTF_8));
    }
    writer.addRowBatch(batch);
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(3500, reader.getNumberOfRows());

    SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startAnd()
        .startNot()
        .lessThan("`int1`", PredicateLeaf.Type.LONG, 300000L)
        .end()
        .lessThan("`int1`", PredicateLeaf.Type.LONG, 600000L)
        .end()
        .build();
    RecordReader rows = reader.rows(reader.options()
        .range(0L, Long.MAX_VALUE)
        .include(new boolean[]{true, true, true})
        .searchArgument(sarg, new String[]{null, "`int1`", "string1"}));
    batch = reader.getSchema().createRowBatch(2000);

    assertEquals(1000L, rows.getRowNumber());
    assertTrue(rows.nextBatch(batch));
    assertEquals(1000, batch.size);

    // Validate the same behaviour with schemaFromString
    fs.delete(testFilePath, false);
    TypeDescription qSchema = createQuotedSchemaFromString();
    // [`int1`, `string1`]
    assertEquals(schema.getFieldNames(), qSchema.getFieldNames());

    Writer writerSchemaFromStr = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(qSchema)
            .stripeSize(400000L)
            .compress(CompressionKind.NONE)
            .bufferSize(500)
            .rowIndexStride(1000)
            .version(fileFormat));
    batch = qSchema.createRowBatch();
    batch.ensureSize(3500);
    batch.size = 3500;
    for(int i=0; i < 3500; ++i) {
      ((LongColumnVector) batch.cols[0]).vector[i] = i * 300;
      ((BytesColumnVector) batch.cols[1]).setVal(i,
          Integer.toHexString(10*i).getBytes(StandardCharsets.UTF_8));
    }
    writerSchemaFromStr.addRowBatch(batch);
    writerSchemaFromStr.close();
    Reader readerSchemaFromStr = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(3500, readerSchemaFromStr.getNumberOfRows());

    sarg = SearchArgumentFactory.newBuilder()
        .startAnd()
        .startNot()
        .lessThan("`int1`", PredicateLeaf.Type.LONG, 300000L)
        .end()
        .lessThan("`int1`", PredicateLeaf.Type.LONG, 600000L)
        .end()
        .build();
    rows = readerSchemaFromStr.rows(readerSchemaFromStr.options()
        .range(0L, Long.MAX_VALUE)
        .include(new boolean[]{true, true, true})
        .searchArgument(sarg, new String[]{null, "`int1`", "string1"}));
    batch = readerSchemaFromStr.getSchema().createRowBatch(2000);

    assertEquals(1000L, rows.getRowNumber());
    assertTrue(rows.nextBatch(batch));
    assertEquals(1000, batch.size);

    assertEquals(reader.getSchema(), readerSchemaFromStr.getSchema());
    assertEquals(writer.getSchema(), writerSchemaFromStr.getSchema());
  }

  /**
   * Test all of the types that have distinct ORC writers using the vectorized
   * writer with different combinations of repeating and null values.
   * @throws Exception
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testRepeating(Version fileFormat) throws Exception {
    // create a row type with each type that has a unique writer
    // really just folds short, int, and long together
    TypeDescription schema = TypeDescription.createStruct()
        .addField("bin", TypeDescription.createBinary())
        .addField("bool", TypeDescription.createBoolean())
        .addField("byte", TypeDescription.createByte())
        .addField("long", TypeDescription.createLong())
        .addField("float", TypeDescription.createFloat())
        .addField("double", TypeDescription.createDouble())
        .addField("date", TypeDescription.createDate())
        .addField("time", TypeDescription.createTimestamp())
        .addField("dec", TypeDescription.createDecimal()
            .withPrecision(20).withScale(6))
        .addField("string", TypeDescription.createString())
        .addField("char", TypeDescription.createChar().withMaxLength(10))
        .addField("vc", TypeDescription.createVarchar().withMaxLength(10))
        .addField("struct", TypeDescription.createStruct()
            .addField("sub1", TypeDescription.createInt()))
        .addField("union", TypeDescription.createUnion()
            .addUnionChild(TypeDescription.createString())
            .addUnionChild(TypeDescription.createInt()))
        .addField("list", TypeDescription
            .createList(TypeDescription.createInt()))
        .addField("map",
            TypeDescription.createMap(TypeDescription.createString(),
                TypeDescription.createString()));
    VectorizedRowBatch batch = schema.createRowBatch();
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(1000)
            .version(fileFormat));

    // write 1024 repeating nulls
    batch.size = 1024;
    for(int c = 0; c < batch.cols.length; ++c) {
      batch.cols[c].setRepeating(true);
      batch.cols[c].noNulls = false;
      batch.cols[c].isNull[0] = true;
    }
    writer.addRowBatch(batch);

    // write 1024 repeating non-null
    for(int c =0; c < batch.cols.length; ++c) {
      batch.cols[c].isNull[0] = false;
    }
    ((BytesColumnVector) batch.cols[0]).setVal(0, "Horton".getBytes(StandardCharsets.UTF_8));
    ((LongColumnVector) batch.cols[1]).vector[0] = 1;
    ((LongColumnVector) batch.cols[2]).vector[0] = 130;
    ((LongColumnVector) batch.cols[3]).vector[0] = 0x123456789abcdef0L;
    ((DoubleColumnVector) batch.cols[4]).vector[0] = 1.125;
    ((DoubleColumnVector) batch.cols[5]).vector[0] = 0.0009765625;
    ((LongColumnVector) batch.cols[6]).vector[0] =
        new DateWritable(new Date(111, 6, 1)).getDays();
    ((TimestampColumnVector) batch.cols[7]).set(0,
        new Timestamp(115, 9, 23, 10, 11, 59,
            999999999));
    ((DecimalColumnVector) batch.cols[8]).vector[0] =
        new HiveDecimalWritable("1.234567");
    ((BytesColumnVector) batch.cols[9]).setVal(0, "Echelon".getBytes(StandardCharsets.UTF_8));
    ((BytesColumnVector) batch.cols[10]).setVal(0, "Juggernaut".getBytes(StandardCharsets.UTF_8));
    ((BytesColumnVector) batch.cols[11]).setVal(0, "Dreadnaught".getBytes(StandardCharsets.UTF_8));
    ((LongColumnVector) ((StructColumnVector) batch.cols[12]).fields[0])
        .vector[0] = 123;
    ((UnionColumnVector) batch.cols[13]).tags[0] = 1;
    ((LongColumnVector) ((UnionColumnVector) batch.cols[13]).fields[1])
        .vector[0] = 1234;
    ((ListColumnVector) batch.cols[14]).offsets[0] = 0;
    ((ListColumnVector) batch.cols[14]).lengths[0] = 3;
    ((ListColumnVector) batch.cols[14]).child.isRepeating = true;
    ((LongColumnVector) ((ListColumnVector) batch.cols[14]).child).vector[0]
        = 31415;
    ((MapColumnVector) batch.cols[15]).offsets[0] = 0;
    ((MapColumnVector) batch.cols[15]).lengths[0] = 3;
    ((MapColumnVector) batch.cols[15]).values.isRepeating = true;
    ((BytesColumnVector) ((MapColumnVector) batch.cols[15]).keys)
        .setVal(0, "ORC".getBytes(StandardCharsets.UTF_8));
    ((BytesColumnVector) ((MapColumnVector) batch.cols[15]).keys)
        .setVal(1, "Hive".getBytes(StandardCharsets.UTF_8));
    ((BytesColumnVector) ((MapColumnVector) batch.cols[15]).keys)
        .setVal(2, "LLAP".getBytes(StandardCharsets.UTF_8));
    ((BytesColumnVector) ((MapColumnVector) batch.cols[15]).values)
        .setVal(0, "fast".getBytes(StandardCharsets.UTF_8));
    writer.addRowBatch(batch);

    // write 1024 null without repeat
    for(int c = 0; c < batch.cols.length; ++c) {
      batch.cols[c].setRepeating(false);
      batch.cols[c].noNulls = false;
      Arrays.fill(batch.cols[c].isNull, true);
    }
    writer.addRowBatch(batch);

    // add 1024 rows of non-null, non-repeating
    batch.reset();
    batch.size = 1024;
    ((ListColumnVector) batch.cols[14]).child.ensureSize(3 * 1024, false);
    ((MapColumnVector) batch.cols[15]).keys.ensureSize(3 * 1024, false);
    ((MapColumnVector) batch.cols[15]).values.ensureSize(3 * 1024, false);
    for(int r=0; r < 1024; ++r) {
      ((BytesColumnVector) batch.cols[0]).setVal(r,
          Integer.toHexString(r).getBytes(StandardCharsets.UTF_8));
      ((LongColumnVector) batch.cols[1]).vector[r] = r % 2;
      ((LongColumnVector) batch.cols[2]).vector[r] = (r % 255);
      ((LongColumnVector) batch.cols[3]).vector[r] = 31415L * r;
      ((DoubleColumnVector) batch.cols[4]).vector[r] = 1.125 * r;
      ((DoubleColumnVector) batch.cols[5]).vector[r] = 0.0009765625 * r;
      ((LongColumnVector) batch.cols[6]).vector[r] =
          new DateWritable(new Date(111, 6, 1)).getDays() + r;

      Timestamp ts = new Timestamp(115, 9, 25, 10, 11, 59 + r, 999999999);
      ((TimestampColumnVector) batch.cols[7]).set(r, ts);
      ((DecimalColumnVector) batch.cols[8]).vector[r] =
          new HiveDecimalWritable("1.234567");
      ((BytesColumnVector) batch.cols[9]).setVal(r,
          Integer.toString(r).getBytes(StandardCharsets.UTF_8));
      ((BytesColumnVector) batch.cols[10]).setVal(r,
          Integer.toHexString(r).getBytes(StandardCharsets.UTF_8));
      ((BytesColumnVector) batch.cols[11]).setVal(r,
          Integer.toHexString(r * 128).getBytes(StandardCharsets.UTF_8));
      ((LongColumnVector) ((StructColumnVector) batch.cols[12]).fields[0])
          .vector[r] = r + 13;
      ((UnionColumnVector) batch.cols[13]).tags[r] = 1;
      ((LongColumnVector) ((UnionColumnVector) batch.cols[13]).fields[1])
          .vector[r] = r + 42;
      ((ListColumnVector) batch.cols[14]).offsets[r] = 3 * r;
      ((ListColumnVector) batch.cols[14]).lengths[r] = 3;
      for(int i=0; i < 3; ++i) {
        ((LongColumnVector) ((ListColumnVector) batch.cols[14]).child)
            .vector[3 * r + i] = 31415 + i;
      }
      ((MapColumnVector) batch.cols[15]).offsets[r] = 3 * r;
      ((MapColumnVector) batch.cols[15]).lengths[r] = 3;
      for(int i=0; i < 3; ++i) {
        ((BytesColumnVector) ((MapColumnVector) batch.cols[15]).keys)
            .setVal(3 * r + i, Integer.toHexString(3 * r + i).getBytes(StandardCharsets.UTF_8));
        ((BytesColumnVector) ((MapColumnVector) batch.cols[15]).values)
            .setVal(3 * r + i, Integer.toString(3 * r + i).getBytes(StandardCharsets.UTF_8));
      }
    }
    writer.addRowBatch(batch);

    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

    // check the stats
    ColumnStatistics[] stats = reader.getStatistics();
    assertArrayEquals(stats, writer.getStatistics());
    assertEquals(4096, stats[0].getNumberOfValues());
    assertFalse(stats[0].hasNull());
    for(TypeDescription colType: schema.getChildren()) {
      assertEquals(2048, stats[colType.getId()].getNumberOfValues(),
          "count on " + colType.getId());
      assertTrue(stats[colType.getId()].hasNull(), "hasNull on " + colType.getId());
    }
    assertEquals(8944, ((BinaryColumnStatistics) stats[1]).getSum());
    assertEquals(1536, ((BooleanColumnStatistics) stats[2]).getTrueCount());
    assertEquals(512, ((BooleanColumnStatistics) stats[2]).getFalseCount());
    assertFalse(((IntegerColumnStatistics) stats[4]).isSumDefined());
    assertEquals(0, ((IntegerColumnStatistics) stats[4]).getMinimum());
    assertEquals(0x123456789abcdef0L,
        ((IntegerColumnStatistics) stats[4]).getMaximum());
    assertEquals("0", ((StringColumnStatistics) stats[10]).getMinimum());
    assertEquals("Echelon", ((StringColumnStatistics) stats[10]).getMaximum());
    assertEquals(10154, ((StringColumnStatistics) stats[10]).getSum());
    assertEquals("0         ",
        ((StringColumnStatistics) stats[11]).getMinimum());
    assertEquals("ff        ",
        ((StringColumnStatistics) stats[11]).getMaximum());
    assertEquals(20480, ((StringColumnStatistics) stats[11]).getSum());
    assertEquals("0",
        ((StringColumnStatistics) stats[12]).getMinimum());
    assertEquals("ff80",
        ((StringColumnStatistics) stats[12]).getMaximum());
    assertEquals(14813, ((StringColumnStatistics) stats[12]).getSum());

    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch(1024);
    BytesColumnVector bins = (BytesColumnVector) batch.cols[0];
    LongColumnVector bools = (LongColumnVector) batch.cols[1];
    LongColumnVector bytes = (LongColumnVector) batch.cols[2];
    LongColumnVector longs = (LongColumnVector) batch.cols[3];
    DoubleColumnVector floats = (DoubleColumnVector) batch.cols[4];
    DoubleColumnVector doubles = (DoubleColumnVector) batch.cols[5];
    LongColumnVector dates = (LongColumnVector) batch.cols[6];
    TimestampColumnVector times = (TimestampColumnVector) batch.cols[7];
    DecimalColumnVector decs = (DecimalColumnVector) batch.cols[8];
    BytesColumnVector strs = (BytesColumnVector) batch.cols[9];
    BytesColumnVector chars = (BytesColumnVector) batch.cols[10];
    BytesColumnVector vcs = (BytesColumnVector) batch.cols[11];
    StructColumnVector structs = (StructColumnVector) batch.cols[12];
    UnionColumnVector unions = (UnionColumnVector) batch.cols[13];
    ListColumnVector lists = (ListColumnVector) batch.cols[14];
    MapColumnVector maps = (MapColumnVector) batch.cols[15];
    LongColumnVector structInts = (LongColumnVector) structs.fields[0];
    LongColumnVector unionInts = (LongColumnVector) unions.fields[1];
    LongColumnVector listInts = (LongColumnVector) lists.child;
    BytesColumnVector mapKeys = (BytesColumnVector) maps.keys;
    BytesColumnVector mapValues = (BytesColumnVector) maps.values;

    assertTrue(rows.nextBatch(batch));
    assertEquals(1024, batch.size);

    // read the 1024 nulls
    for(int f=0; f < batch.cols.length; ++f) {
      assertTrue(batch.cols[f].isRepeating, "field " + f);
      assertFalse(batch.cols[f].noNulls, "field " + f);
      assertTrue(batch.cols[f].isNull[0], "field " + f);
    }

    // read the 1024 repeat values
    assertTrue(rows.nextBatch(batch));
    assertEquals(1024, batch.size);
    for(int r=0; r < 1024; ++r) {
      String msg = "row " + r;
      assertEquals("Horton", bins.toString(r), msg);
      assertEquals(1, bools.vector[r], msg);
      assertEquals(-126, bytes.vector[r], msg);
      assertEquals(1311768467463790320L, longs.vector[r], msg);
      assertEquals(1.125, floats.vector[r], 0.00001, msg);
      assertEquals(9.765625E-4, doubles.vector[r], 0.000001, msg);
      assertEquals("2011-07-01", new DateWritable((int) dates.vector[r]).toString(), msg);
      assertEquals("2015-10-23 10:11:59.999999999", times.asScratchTimestamp(r).toString(), msg);
      assertEquals("1.234567", decs.vector[r].toString(), msg);
      assertEquals("Echelon", strs.toString(r), msg);
      assertEquals("Juggernaut", chars.toString(r), msg);
      assertEquals("Dreadnaugh", vcs.toString(r), msg);
      assertEquals(123, structInts.vector[r], msg);
      assertEquals(1, unions.tags[r], msg);
      assertEquals(1234, unionInts.vector[r], msg);
      assertEquals(3, lists.lengths[r], msg);
      assertTrue(listInts.isRepeating, msg);
      assertEquals(31415, listInts.vector[0], msg);
      assertEquals(3, maps.lengths[r], msg);
      assertEquals("ORC", mapKeys.toString((int) maps.offsets[r]), msg);
      assertEquals("Hive", mapKeys.toString((int) maps.offsets[r] + 1), msg);
      assertEquals("LLAP", mapKeys.toString((int) maps.offsets[r] + 2), msg);
      assertEquals("fast", mapValues.toString((int) maps.offsets[r]), msg);
      assertEquals("fast", mapValues.toString((int) maps.offsets[r] + 1), msg);
      assertEquals("fast", mapValues.toString((int) maps.offsets[r] + 2), msg);
    }

    // read the second set of 1024 nulls
    assertTrue(rows.nextBatch(batch));
    assertEquals(1024, batch.size);
    for(int f=0; f < batch.cols.length; ++f) {
      assertTrue(batch.cols[f].isRepeating, "field " + f);
      assertFalse(batch.cols[f].noNulls, "field " + f);
      assertTrue(batch.cols[f].isNull[0], "field " + f);
    }

    assertTrue(rows.nextBatch(batch));
    assertEquals(1024, batch.size);
    for(int r=0; r < 1024; ++r) {
      String hex = Integer.toHexString(r);
      String msg = "row " + r;
      assertEquals(hex, bins.toString(r), msg);
      assertEquals(r % 2 == 1 ? 1 : 0, bools.vector[r], msg);
      assertEquals((byte) (r % 255), bytes.vector[r], msg);
      assertEquals(31415L * r, longs.vector[r], msg);
      assertEquals(1.125F * r, floats.vector[r], 0.0001, msg);
      assertEquals(0.0009765625 * r, doubles.vector[r], 0.000001, msg);
      assertEquals(new DateWritable(new Date(111, 6, 1 + r)),
          new DateWritable((int) dates.vector[r]), msg);
      assertEquals(
          new Timestamp(115, 9, 25, 10, 11, 59 + r, 999999999),
          times.asScratchTimestamp(r), msg);
      assertEquals("1.234567", decs.vector[r].toString(), msg);
      assertEquals(Integer.toString(r), strs.toString(r), msg);
      assertEquals(Integer.toHexString(r), chars.toString(r), msg);
      assertEquals(Integer.toHexString(r * 128), vcs.toString(r), msg);
      assertEquals(r + 13, structInts.vector[r], msg);
      assertEquals(1, unions.tags[r], msg);
      assertEquals(r + 42, unionInts.vector[r], msg);
      assertEquals(3, lists.lengths[r], msg);
      assertEquals(31415, listInts.vector[(int) lists.offsets[r]], msg);
      assertEquals(31416, listInts.vector[(int) lists.offsets[r] + 1], msg);
      assertEquals(31417, listInts.vector[(int) lists.offsets[r] + 2], msg);
      assertEquals(3, maps.lengths[3], msg);
      assertEquals(Integer.toHexString(3 * r), mapKeys.toString((int) maps.offsets[r]), msg);
      assertEquals(Integer.toString(3 * r), mapValues.toString((int) maps.offsets[r]), msg);
      assertEquals(Integer.toHexString(3 * r + 1), mapKeys.toString((int) maps.offsets[r] + 1), msg);
      assertEquals(Integer.toString(3 * r + 1), mapValues.toString((int) maps.offsets[r] + 1), msg);
      assertEquals(Integer.toHexString(3 * r + 2), mapKeys.toString((int) maps.offsets[r] + 2), msg);
      assertEquals(Integer.toString(3 * r + 2), mapValues.toString((int) maps.offsets[r] + 2), msg);
    }

    // should have no more rows
    assertFalse(rows.nextBatch(batch));
  }

  private static String makeString(BytesColumnVector vector, int row) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      return new String(vector.vector[row], vector.start[row],
          vector.length[row], StandardCharsets.UTF_8);
    } else {
      return null;
    }
  }

  /**
   * Test the char and varchar padding and truncation.
   * @throws Exception
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testStringPadding(Version fileFormat) throws Exception {
    TypeDescription schema = TypeDescription.createStruct()
        .addField("char", TypeDescription.createChar().withMaxLength(10))
        .addField("varchar", TypeDescription.createVarchar().withMaxLength(10));
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema).version(fileFormat));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 4;
    for(int c=0; c < batch.cols.length; ++c) {
      ((BytesColumnVector) batch.cols[c]).setVal(0, "".getBytes(StandardCharsets.UTF_8));
      ((BytesColumnVector) batch.cols[c]).setVal(1, "xyz".getBytes(StandardCharsets.UTF_8));
      ((BytesColumnVector) batch.cols[c]).setVal(2, "0123456789".getBytes(StandardCharsets.UTF_8));
      ((BytesColumnVector) batch.cols[c]).setVal(3,
          "0123456789abcdef".getBytes(StandardCharsets.UTF_8));
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    assertTrue(rows.nextBatch(batch));
    assertEquals(4, batch.size);
    // ORC currently trims the output strings. See HIVE-12286
    assertEquals("",
        makeString((BytesColumnVector) batch.cols[0], 0));
    assertEquals("xyz",
        makeString((BytesColumnVector) batch.cols[0], 1));
    assertEquals("0123456789",
        makeString((BytesColumnVector) batch.cols[0], 2));
    assertEquals("0123456789",
        makeString((BytesColumnVector) batch.cols[0], 3));
    assertEquals("",
        makeString((BytesColumnVector) batch.cols[1], 0));
    assertEquals("xyz",
        makeString((BytesColumnVector) batch.cols[1], 1));
    assertEquals("0123456789",
        makeString((BytesColumnVector) batch.cols[1], 2));
    assertEquals("0123456789",
        makeString((BytesColumnVector) batch.cols[1], 3));
  }

  /**
   * A test case that tests the case where you add a repeating batch
   * to a column that isn't using dictionary encoding.
   * @throws Exception
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testNonDictionaryRepeatingString(Version fileFormat) throws Exception {
    assumeTrue(fileFormat != OrcFile.Version.V_0_11);
    TypeDescription schema = TypeDescription.createStruct()
        .addField("str", TypeDescription.createString());
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(1000)
            .version(fileFormat));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 1024;
    for(int r=0; r < batch.size; ++r) {
      ((BytesColumnVector) batch.cols[0]).setVal(r,
          Integer.toString(r * 10001).getBytes(StandardCharsets.UTF_8));
    }
    writer.addRowBatch(batch);
    batch.cols[0].isRepeating = true;
    ((BytesColumnVector) batch.cols[0]).setVal(0, "Halloween".getBytes(StandardCharsets.UTF_8));
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    assertTrue(rows.nextBatch(batch));
    assertEquals(1024, batch.size);
    for(int r=0; r < 1024; ++r) {
      assertEquals(Integer.toString(r * 10001),
          makeString((BytesColumnVector) batch.cols[0], r));
    }
    assertTrue(rows.nextBatch(batch));
    assertEquals(1024, batch.size);
    for(int r=0; r < 1024; ++r) {
      assertEquals("Halloween",
          makeString((BytesColumnVector) batch.cols[0], r));
    }
    assertFalse(rows.nextBatch(batch));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testStructs(Version fileFormat) throws Exception {
    TypeDescription schema = TypeDescription.createStruct()
        .addField("struct", TypeDescription.createStruct()
            .addField("inner", TypeDescription.createLong()));
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema).version(fileFormat));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 1024;
    StructColumnVector outer = (StructColumnVector) batch.cols[0];
    outer.noNulls = false;
    for(int r=0; r < 1024; ++r) {
      if (r < 200 || (r >= 400 && r < 600) || r >= 800) {
        outer.isNull[r] = true;
      }
      ((LongColumnVector) outer.fields[0]).vector[r] = r;
    }
    writer.addRowBatch(batch);
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    rows.nextBatch(batch);
    assertEquals(1024, batch.size);
    StructColumnVector inner = (StructColumnVector) batch.cols[0];
    LongColumnVector vec = (LongColumnVector) inner.fields[0];
    for(int r=0; r < 1024; ++r) {
      if (r < 200 || (r >= 400 && r < 600) || r >= 800) {
        assertTrue(inner.isNull[r], "row " + r);
      } else {
        assertFalse(inner.isNull[r], "row " + r);
        assertEquals(r, vec.vector[r], "row " + r);
      }
    }
    rows.nextBatch(batch);
    assertEquals(0, batch.size);
  }

  /**
   * Test Unions.
   * @throws Exception
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testUnions(Version fileFormat) throws Exception {
    TypeDescription schema = TypeDescription.createStruct()
        .addField("outer", TypeDescription.createUnion()
            .addUnionChild(TypeDescription.createInt())
            .addUnionChild(TypeDescription.createLong()));
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema).version(fileFormat));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 1024;
    UnionColumnVector outer = (UnionColumnVector) batch.cols[0];
    batch.cols[0].noNulls = false;
    for(int r=0; r < 1024; ++r) {
      if (r < 200) {
        outer.isNull[r] = true;
      } else if (r < 300) {
        outer.tags[r] = 0;
      } else if (r < 400) {
        outer.tags[r] = 1;
      } else if (r < 600) {
        outer.isNull[r] = true;
      } else if (r < 800) {
        outer.tags[r] = 1;
      } else if (r < 1000) {
        outer.isNull[r] = true;
      } else {
        outer.tags[r] = 1;
      }
      ((LongColumnVector) outer.fields[0]).vector[r] = r;
      ((LongColumnVector) outer.fields[1]).vector[r] = -r;
    }
    writer.addRowBatch(batch);
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch(1024);
    UnionColumnVector union = (UnionColumnVector) batch.cols[0];
    LongColumnVector ints = (LongColumnVector) union.fields[0];
    LongColumnVector longs = (LongColumnVector) union.fields[1];
    assertTrue(rows.nextBatch(batch));
    assertEquals(1024, batch.size);
    for(int r=0; r < 1024; ++r) {
      String msg = "row " + r;
      if (r < 200) {
        assertTrue(union.isNull[r], msg);
      } else if (r < 300) {
        assertFalse(union.isNull[r], msg);
        assertEquals(0, union.tags[r], msg);
        assertEquals(r, ints.vector[r], msg);
      } else if (r < 400) {
        assertFalse(union.isNull[r], msg);
        assertEquals(1, union.tags[r], msg);
        assertEquals(-r, longs.vector[r], msg);
      } else if (r < 600) {
        assertTrue(union.isNull[r], msg);
      } else if (r < 800) {
        assertFalse(union.isNull[r], msg);
        assertEquals(1, union.tags[r], msg);
        assertEquals(-r, longs.vector[r], msg);
      } else if (r < 1000) {
        assertTrue(union.isNull[r], msg);
      } else {
        assertFalse(union.isNull[r], msg);
        assertEquals(1, union.tags[r], msg);
        assertEquals(-r, longs.vector[r], msg);
      }
    }
    assertFalse(rows.nextBatch(batch));
  }

  /**
   * Test lists and how they interact with the child column. In particular,
   * put nulls between back to back lists and then make some lists that
   * oper lap.
   * @throws Exception
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testLists(Version fileFormat) throws Exception {
    TypeDescription schema = TypeDescription.createStruct()
        .addField("list",
            TypeDescription.createList(TypeDescription.createLong()));
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema).version(fileFormat));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 1024;
    ListColumnVector list = (ListColumnVector) batch.cols[0];
    list.noNulls = false;
    for(int r=0; r < 1024; ++r) {
      if (r < 200) {
        list.isNull[r] = true;
      } else if (r < 300) {
        list.offsets[r] = r - 200;
        list.lengths[r] = 1;
      } else if (r < 400) {
        list.isNull[r] = true;
      } else if (r < 500) {
        list.offsets[r] = r - 300;
        list.lengths[r] = 1;
      } else if (r < 600) {
        list.isNull[r] = true;
      } else if (r < 700) {
        list.offsets[r] = r;
        list.lengths[r] = 2;
      } else {
        list.isNull[r] = true;
      }
      ((LongColumnVector) list.child).vector[r] = r * 10;
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch(1024);
    list = (ListColumnVector) batch.cols[0];
    rows.nextBatch(batch);
    assertEquals(1024, batch.size);
    for(int r=0; r < 1024; ++r) {
      StringBuilder actual = new StringBuilder();
      list.stringifyValue(actual, r);
      String msg = "row " + r;
      if (r < 200) {
        assertEquals("null", actual.toString(), msg);
      } else if (r < 300) {
        assertEquals("[" + ((r - 200) * 10) + "]", actual.toString(), msg);
      } else if (r < 400) {
        assertEquals("null", actual.toString(), msg);
      } else if (r < 500) {
        assertEquals("[" + ((r - 300) * 10) + "]", actual.toString(), msg);
      } else if (r < 600) {
        assertEquals("null", actual.toString(), msg);
      } else if (r < 700) {
        assertEquals("[" + (10 * r) + ", " + (10 * (r + 1)) + "]",
            actual.toString(), msg);
      } else {
        assertEquals("null", actual.toString(), msg);
      }
    }
    assertFalse(rows.nextBatch(batch));
  }

  /**
   * Test maps and how they interact with the child column. In particular,
   * put nulls between back to back lists and then make some lists that
   * oper lap.
   * @throws Exception
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testMaps(Version fileFormat) throws Exception {
    TypeDescription schema = TypeDescription.createStruct()
        .addField("map",
            TypeDescription.createMap(TypeDescription.createLong(),
                TypeDescription.createLong()));
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema).version(fileFormat));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 1024;
    MapColumnVector map = (MapColumnVector) batch.cols[0];
    map.noNulls = false;
    for(int r=0; r < 1024; ++r) {
      if (r < 200) {
        map.isNull[r] = true;
      } else if (r < 300) {
        map.offsets[r] = r - 200;
        map.lengths[r] = 1;
      } else if (r < 400) {
        map.isNull[r] = true;
      } else if (r < 500) {
        map.offsets[r] = r - 300;
        map.lengths[r] = 1;
      } else if (r < 600) {
        map.isNull[r] = true;
      } else if (r < 700) {
        map.offsets[r] = r;
        map.lengths[r] = 2;
      } else {
        map.isNull[r] = true;
      }
      ((LongColumnVector) map.keys).vector[r] = r;
      ((LongColumnVector) map.values).vector[r] = r * 10;
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    map = (MapColumnVector) batch.cols[0];
    rows.nextBatch(batch);
    assertEquals(1024, batch.size);
    for(int r=0; r < 1024; ++r) {
      StringBuilder buffer = new StringBuilder();
      map.stringifyValue(buffer, r);
      String msg = "row " + r;
      String actual = buffer.toString();
      if (r < 200) {
        assertEquals("null", actual, msg);
      } else if (r < 300) {
        assertEquals("[{\"key\": " + (r - 200) +
                ", \"value\": " + ((r - 200) * 10) + "}]",
            actual, msg);
      } else if (r < 400) {
        assertEquals("null", actual, msg);
      } else if (r < 500) {
        assertEquals("[{\"key\": " + (r - 300) +
                ", \"value\": " + ((r - 300) * 10) + "}]", actual, msg);
      } else if (r < 600) {
        assertEquals("null", actual, msg);
      } else if (r < 700) {
        assertEquals("[{\"key\": " + r + ", \"value\": " + (r * 10)
                + "}, {\"key\": " + (r + 1) + ", \"value\": " + (10 * (r + 1))
                + "}]", actual, msg);
      } else {
        assertEquals("null", actual, msg);
      }
    }
    rows.nextBatch(batch);
    assertEquals(0, batch.size);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testExpansion(Version fileFormat) throws Exception {
    TypeDescription schema =
        TypeDescription.fromString(
            "struct<list1:array<string>," +
                    "list2:array<binary>>");
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema).version(fileFormat));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 2;
    ListColumnVector list1 = (ListColumnVector) batch.cols[0];
    BytesColumnVector str = (BytesColumnVector) list1.child;
    str.ensureSize(6000, false);
    ListColumnVector list2 = (ListColumnVector) batch.cols[1];
    BytesColumnVector bin = (BytesColumnVector) list2.child;
    bin.ensureSize(6000, false);
    list1.offsets[0] = 0;
    list1.lengths[0] = 2000;
    list2.offsets[1] = 2000;
    list2.lengths[1] = 3000;
    for(int v=0; v < 5000; ++v) {
      byte[] bytes = Long.toHexString(v).getBytes(StandardCharsets.UTF_8);
      str.setVal(v, bytes);
      bin.setVal(v, bytes);
    }
    writer.addRowBatch(batch);
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    assertTrue(rows.nextBatch(batch));
    assertEquals(2, batch.size);
    assertFalse(rows.nextBatch(batch));
    rows.close();
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testWriterVersion(Version fileFormat) throws Exception {
    assumeTrue(fileFormat == OrcFile.Version.V_0_11);

    // test writer implementation serialization
    assertEquals(OrcFile.WriterImplementation.ORC_JAVA,
        OrcFile.WriterImplementation.from(0));
    assertEquals(OrcFile.WriterImplementation.ORC_CPP,
        OrcFile.WriterImplementation.from(1));
    assertEquals(OrcFile.WriterImplementation.PRESTO,
        OrcFile.WriterImplementation.from(2));
    assertEquals(OrcFile.WriterImplementation.TRINO,
            OrcFile.WriterImplementation.from(4));
    assertEquals(OrcFile.WriterImplementation.UNKNOWN,
        OrcFile.WriterImplementation.from(99));

    // test writer version serialization
    assertEquals(OrcFile.WriterVersion.FUTURE,
        OrcFile.WriterVersion.from(OrcFile.WriterImplementation.ORC_JAVA, 99));
    assertEquals(OrcFile.WriterVersion.ORIGINAL,
        OrcFile.WriterVersion.from(OrcFile.WriterImplementation.ORC_JAVA, 0));
    assertEquals(OrcFile.WriterVersion.HIVE_4243,
        OrcFile.WriterVersion.from(OrcFile.WriterImplementation.ORC_JAVA, 2));
    assertEquals(OrcFile.WriterVersion.FUTURE,
        OrcFile.WriterVersion.from(OrcFile.WriterImplementation.ORC_CPP, 99));
    assertEquals(OrcFile.WriterVersion.ORC_CPP_ORIGINAL,
        OrcFile.WriterVersion.from(OrcFile.WriterImplementation.ORC_CPP, 6));
    assertEquals(OrcFile.WriterVersion.PRESTO_ORIGINAL,
        OrcFile.WriterVersion.from(OrcFile.WriterImplementation.PRESTO, 6));
    assertEquals(OrcFile.WriterVersion.TRINO_ORIGINAL,
            OrcFile.WriterVersion.from(OrcFile.WriterImplementation.TRINO, 6));
    assertEquals(OrcFile.WriterVersion.FUTURE,
        OrcFile.WriterVersion.from(OrcFile.WriterImplementation.UNKNOWN, 0));

    // test compatibility
    assertTrue(OrcFile.WriterVersion.FUTURE.includes(
        OrcFile.WriterVersion.ORC_CPP_ORIGINAL));
    assertTrue(OrcFile.WriterVersion.FUTURE.includes(
        OrcFile.WriterVersion.HIVE_8732));
    assertTrue(OrcFile.WriterVersion.HIVE_12055.includes(
        OrcFile.WriterVersion.HIVE_4243));
    assertTrue(OrcFile.WriterVersion.HIVE_12055.includes(
        OrcFile.WriterVersion.HIVE_12055));
    assertFalse(OrcFile.WriterVersion.HIVE_4243.includes(
        OrcFile.WriterVersion.HIVE_12055));
    assertTrue(OrcFile.WriterVersion.HIVE_12055.includes(
        OrcFile.WriterVersion.PRESTO_ORIGINAL));
    assertTrue(OrcFile.WriterVersion.HIVE_12055.includes(
        OrcFile.WriterVersion.TRINO_ORIGINAL));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testBadPrestoVersion(Version fileFormat) {
    assumeTrue(fileFormat == OrcFile.Version.V_0_11);
    assertThrows(IllegalArgumentException.class, () -> {
      OrcFile.WriterVersion.from(OrcFile.WriterImplementation.PRESTO, 0);
    });
  }

  /**
   * Test whether the file versions are translated correctly
   * @throws Exception
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testFileVersion(Version fileFormat) throws Exception {
    assumeTrue(fileFormat == OrcFile.Version.V_0_11);
    assertEquals(OrcFile.Version.V_0_11, ReaderImpl.getFileVersion(null));
    assertEquals(OrcFile.Version.V_0_11, ReaderImpl.getFileVersion(new ArrayList<Integer>()));
    assertEquals(OrcFile.Version.V_0_11,
        ReaderImpl.getFileVersion(Arrays.asList(new Integer[]{0, 11})));
    assertEquals(OrcFile.Version.V_0_12,
        ReaderImpl.getFileVersion(Arrays.asList(new Integer[]{0, 12})));
    assertEquals(OrcFile.Version.FUTURE,
        ReaderImpl.getFileVersion(Arrays.asList(new Integer[]{9999, 0})));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testMergeUnderstood(Version fileFormat) throws Exception {
    assumeTrue(fileFormat == OrcFile.Version.V_0_11);
    Path p = new Path("test.orc");
    Reader futureVersion = Mockito.mock(Reader.class);
    Mockito.when(futureVersion.getFileVersion()).thenReturn(OrcFile.Version.FUTURE);
    Mockito.when(futureVersion.getWriterVersion()).thenReturn(OrcFile.WriterVersion.HIVE_4243);
    assertFalse(OrcFile.understandFormat(p, futureVersion));
    Reader futureWriter = Mockito.mock(Reader.class);
    Mockito.when(futureWriter.getFileVersion()).thenReturn(OrcFile.Version.V_0_11);
    Mockito.when(futureWriter.getWriterVersion()).thenReturn(OrcFile.WriterVersion.FUTURE);
    assertFalse(OrcFile.understandFormat(p, futureWriter));
    Reader current = Mockito.mock(Reader.class);
    Mockito.when(current.getFileVersion()).thenReturn(OrcFile.Version.CURRENT);
    Mockito.when(current.getWriterVersion()).thenReturn(OrcFile.CURRENT_WRITER);
    assertTrue(OrcFile.understandFormat(p, current));
  }

  static ByteBuffer fromString(String s) {
    return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
  }

  static byte[] fromLong(long x) {
    return Long.toHexString(x).getBytes(StandardCharsets.UTF_8);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testMerge(Version fileFormat) throws Exception {
    Path input1 = new Path(workDir, "TestVectorOrcFile.testMerge1-" +
        fileFormat.getName() + ".orc");
    fs.delete(input1, false);
    Path input2 = new Path(workDir, "TestVectorOrcFile.testMerge2-" +
        fileFormat.getName() + ".orc");
    fs.delete(input2, false);
    Path input3 = new Path(workDir, "TestVectorOrcFile.testMerge3-" +
        fileFormat.getName() + ".orc");
    fs.delete(input3, false);
    TypeDescription schema = TypeDescription.fromString("struct<a:int,b:string>");
    // change all of the options away from default to find anything we
    // don't copy to the merged file
    OrcFile.WriterOptions opts = OrcFile.writerOptions(conf)
        .setSchema(schema)
        .compress(CompressionKind.LZO)
        .enforceBufferSize()
        .bufferSize(20*1024)
        .rowIndexStride(1000)
        .version(fileFormat)
        .writerVersion(OrcFile.WriterVersion.HIVE_8732);

    Writer writer = OrcFile.createWriter(input1, opts);
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 1024;
    for(int r=0; r < 1024; ++r) {
      ((LongColumnVector) batch.cols[0]).vector[r] = r;
      ((BytesColumnVector) batch.cols[1]).setVal(r, fromLong(r));
    }
    writer.addRowBatch(batch);
    writer.addUserMetadata("a", fromString("foo"));
    writer.addUserMetadata("b", fromString("bar"));
    writer.close();

    // increase the buffer size to 30k
    opts.bufferSize(30*1024);
    writer = OrcFile.createWriter(input2, opts);
    batch.size = 1024;
    for(int r=0; r < 1024; ++r) {
      ((LongColumnVector) batch.cols[0]).vector[r] = 2 * r;
      ((BytesColumnVector) batch.cols[1]).setVal(r, fromLong(2 * r));
    }
    writer.addRowBatch(batch);
    writer.addUserMetadata("a", fromString("foo"));
    writer.addUserMetadata("c", fromString("baz"));
    writer.close();

    // decrease the buffer size to 10k
    opts.bufferSize(10*1024);
    writer = OrcFile.createWriter(input3, opts);
    batch.size = 1024;
    for(int r=0; r < 1024; ++r) {
      ((LongColumnVector) batch.cols[0]).vector[r] = 3 * r;
      ((BytesColumnVector) batch.cols[1]).setVal(r, fromLong(3 * r));
    }
    writer.addRowBatch(batch);
    writer.addUserMetadata("c", fromString("baz"));
    writer.addUserMetadata("d", fromString("bat"));
    writer.close();

    Path output1 = new Path(workDir, "TestVectorOrcFile.testMerge.out1-" +
        fileFormat.getName() + ".orc");
    fs.delete(output1, false);
    List<Path> paths = OrcFile.mergeFiles(output1,
        OrcFile.writerOptions(conf), Arrays.asList(input1, input2, input3));
    assertEquals(3, paths.size());
    Reader reader = OrcFile.createReader(output1, OrcFile.readerOptions(conf));
    assertEquals(3 * 1024, reader.getNumberOfRows());
    assertEquals(CompressionKind.LZO, reader.getCompressionKind());
    assertEquals(30 * 1024, reader.getCompressionSize());
    assertEquals(1000, reader.getRowIndexStride());
    assertEquals(fileFormat, reader.getFileVersion());
    assertEquals(OrcFile.WriterVersion.HIVE_8732, reader.getWriterVersion());
    assertEquals(3, reader.getStripes().size());
    assertEquals(4, reader.getMetadataKeys().size());
    assertEquals(fromString("foo"), reader.getMetadataValue("a"));
    assertEquals(fromString("bar"), reader.getMetadataValue("b"));
    assertEquals(fromString("baz"), reader.getMetadataValue("c"));
    assertEquals(fromString("bat"), reader.getMetadataValue("d"));

    TypeDescription schema4 = TypeDescription.fromString("struct<a:int>");
    Path input4 = new Path(workDir, "TestVectorOrcFile.testMerge4-" +
        fileFormat.getName() + ".orc");
    fs.delete(input4, false);
    opts.setSchema(schema4);
    writer = OrcFile.createWriter(input4, opts);
    batch = schema4.createRowBatch();
    batch.size = 1024;
    for(int r=0; r < 1024; ++r) {
      ((LongColumnVector) batch.cols[0]).vector[r] = 4 * r;
    }
    writer.addRowBatch(batch);
    writer.close();

    Path input5 = new Path(workDir, "TestVectorOrcFile.testMerge5-" +
        fileFormat.getName() + ".orc");
    fs.delete(input5, false);
    opts.setSchema(schema)
        .compress(CompressionKind.NONE)
        .bufferSize(100*1024);
    writer = OrcFile.createWriter(input5, opts);
    batch = schema.createRowBatch();
    batch.size = 1024;
    for(int r=0; r < 1024; ++r) {
      ((LongColumnVector) batch.cols[0]).vector[r] = 4 * r;
      ((BytesColumnVector) batch.cols[1]).setVal(r, fromLong(5 * r));
    }
    writer.addRowBatch(batch);
    writer.close();

    Path output2 = new Path(workDir, "TestVectorOrcFile.testMerge.out2-" +
        fileFormat.getName() + ".orc");
    fs.delete(output2, false);
    paths = OrcFile.mergeFiles(output2, OrcFile.writerOptions(conf),
        Arrays.asList(input3, input4, input1, input5));
    assertEquals(2, paths.size());
    reader = OrcFile.createReader(output2, OrcFile.readerOptions(conf));
    assertEquals(2 * 1024, reader.getNumberOfRows());
    assertEquals(CompressionKind.LZO, reader.getCompressionKind());
    assertEquals(20 * 1024, reader.getCompressionSize());
    assertEquals(1000, reader.getRowIndexStride());
    assertEquals(fileFormat, reader.getFileVersion());
    assertEquals(OrcFile.WriterVersion.HIVE_8732, reader.getWriterVersion());
    assertEquals(2, reader.getStripes().size());
    assertEquals(4, reader.getMetadataKeys().size());
    assertEquals(fromString("foo"), reader.getMetadataValue("a"));
    assertEquals(fromString("bar"), reader.getMetadataValue("b"));
    assertEquals(fromString("baz"), reader.getMetadataValue("c"));
    assertEquals(fromString("bat"), reader.getMetadataValue("d"));
  }

  /**
   * Write a mergeable file to test merging files with column encryption.
   * @param path the path to write to
   * @param provider the key provider
   * @param startValue the base value for the columns
   * @param stripes the number of stripes to write
   * @param bufferSize the buffer size to use for the compression
   * @param encrypt the encryption string
   * @param mask the mask string
   * @return the locations of the intermediate stripes
   * @throws IOException
   */
  private long[] writeMergeableFile(Path path,
                                    KeyProvider provider,
                                    long startValue,
                                    int stripes,
                                    int bufferSize,
                                    String encrypt,
                                    String mask,
                                    Version fileFormat) throws IOException {
    fs.delete(path, false);
    TypeDescription schema = TypeDescription.fromString(
        "struct<a:int,b:struct<c:string,d:string>>");

    // change all of the options away from default to find anything we
    // don't copy to the merged file
    OrcFile.WriterOptions opts = OrcFile.writerOptions(conf)
                                     .setSchema(schema)
                                     .rowIndexStride(1000)
                                     .version(fileFormat)
                                     .bufferSize(bufferSize)
                                     .enforceBufferSize()
                                     .setKeyProvider(provider)
                                     .encrypt(encrypt)
                                     .masks(mask);
    long[] intermediateFooters = new long[stripes];

    Writer writer = OrcFile.createWriter(path, opts);
    VectorizedRowBatch batch = schema.createRowBatch();
    LongColumnVector a = (LongColumnVector) batch.cols[0];
    StructColumnVector b = (StructColumnVector) batch.cols[1];
    BytesColumnVector c = (BytesColumnVector) b.fields[0];
    BytesColumnVector d = (BytesColumnVector) b.fields[1];
    batch.size = 1024;
    for(int btch=0; btch < 3; ++btch) {
      for (int r = 0; r < 1024; ++r) {
        long value = startValue + btch * 1024 + r;
        a.vector[r] = value;
        c.setVal(r, fromLong(value));
        d.setVal(r, String.format("%010x", value * 1_000_001)
                        .getBytes(StandardCharsets.UTF_8));
      }
      writer.addRowBatch(batch);
      // write an intermediate footer to force a stripe
      intermediateFooters[btch] = writer.writeIntermediateFooter();
    }
    writer.close();
    return intermediateFooters;
  }

  static String computeSha(String value) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] digest = md.digest(value.getBytes(StandardCharsets.UTF_8));
      return printHexBinary(digest);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testEncryptMerge(Version fileFormat) throws Exception {
    assumeTrue(fileFormat != OrcFile.Version.V_0_11);
    Path input1 = new Path(workDir, "TestVectorOrcFile.testEncryptMerge1-" +
                                        fileFormat.getName() + ".orc");
    Path input2 = new Path(workDir, "TestVectorOrcFile.testEncryptMerge2-" +
                                        fileFormat.getName() + ".orc");
    Path input3 = new Path(workDir, "TestVectorOrcFile.testEncryptMerge3-" +
                                        fileFormat.getName() + ".orc");
    Path input4 = new Path(workDir, "TestVectorOrcFile.testEncryptMerge4-" +
                                        fileFormat.getName() + ".orc");
    Path input5 = new Path(workDir, "TestVectorOrcFile.testEncryptMerge5-" +
                                        fileFormat.getName() + ".orc");
    Random random = new Random(169);
    InMemoryKeystore keystore = new InMemoryKeystore(random);
    EncryptionAlgorithm algorithm = EncryptionAlgorithm.AES_CTR_128;
    byte[] piiKey = new byte[algorithm.keyLength()];
    byte[] topSecretKey = new byte[algorithm.keyLength()];
    random.nextBytes(piiKey);
    random.nextBytes(topSecretKey);
    keystore.addKey("pii", algorithm, piiKey)
            .addKey("top_secret", algorithm, topSecretKey);
    String encryption = "pii:a;top_secret:b";
    String mask = "sha256,`don't worry`:b";

    // write three files that should merge, each with 3 stripes of 1024 rows.
    long[] cuts = writeMergeableFile(input1, keystore, 0, 3, 0x400, encryption, mask, fileFormat);
    writeMergeableFile(input2, keystore, 3 * 1024, 3, 0x800, encryption, mask, fileFormat);
    writeMergeableFile(input3, keystore, 6 * 1024, 3, 0xc00, encryption, mask, fileFormat);
    // two files that aren't mergeable
    writeMergeableFile(input4, keystore, 9 * 1024, 3, 0x400, encryption, null, fileFormat);
    writeMergeableFile(input5, keystore, 12 * 1024, 3, 0x400, null, null, fileFormat);

    // make sure that we can read up to the intermediate footers
    try (Reader reader = OrcFile.createReader(input1, OrcFile.readerOptions(conf)
                                                     .maxLength(cuts[0]))) {
      assertEquals(1024, reader.getNumberOfRows());
    }
    try (Reader reader = OrcFile.createReader(input1, OrcFile.readerOptions(conf)
                                                          .maxLength(cuts[1]))) {
      assertEquals(2 * 1024, reader.getNumberOfRows());
    }
    try (Reader reader = OrcFile.createReader(input1, OrcFile.readerOptions(conf)
                                                          .maxLength(cuts[2]))) {
      assertEquals(3 * 1024, reader.getNumberOfRows());
    }

    // make a new version of the pii key
    keystore.addKey("pii", 1, algorithm, new byte[algorithm.keyLength()]);
    Path merge1 = new Path(workDir, "TestVectorOrcFile.testEncryptMerge.merge1-" +
                                         fileFormat.getName() + ".orc");

    // merge all three files together
    fs.delete(merge1, false);
    List<Path> paths = OrcFile.mergeFiles(merge1,
        OrcFile.writerOptions(conf).setKeyProvider(keystore),
        Arrays.asList(input1, input2, input3));
    assertEquals(3, paths.size());

    // test reading with no keys
    Reader reader = OrcFile.createReader(merge1, OrcFile.readerOptions(conf));
    assertEquals(9 * 1024, reader.getNumberOfRows());
    assertEquals(CompressionKind.ZLIB, reader.getCompressionKind());
    assertEquals(1000, reader.getRowIndexStride());
    assertEquals(0xc00, reader.getCompressionSize());
    assertEquals(fileFormat, reader.getFileVersion());
    assertEquals(9, reader.getStripes().size());
    EncryptionKey[] keys = reader.getColumnEncryptionKeys();
    assertEquals(2, keys.length);
    assertEquals("pii", keys[0].getKeyName());
    assertEquals(0, keys[0].getKeyVersion());
    assertFalse(keys[0].isAvailable());
    assertEquals("top_secret", keys[1].getKeyName());
    assertEquals(0, keys[1].getKeyVersion());
    assertFalse(keys[1].isAvailable());
    // check the file stats
    ColumnStatistics[] stats = reader.getStatistics();
    assertEquals(9 * 1024, stats[0].getNumberOfValues());
    assertEquals(0, stats[1].getNumberOfValues());
    assertEquals(9 * 1024, stats[2].getNumberOfValues());
    assertEquals(9 * 1024, stats[3].getNumberOfValues());
    assertEquals("00037F39CF870A1F49129F9C82D935665D352FFD25EA3296208F6F7B16FD654F",
        ((StringColumnStatistics) stats[3]).getMinimum());
    assertEquals("FFF60CF25C8E227396BC77DD808773DA69D767D6B0417ADB1A0CAC51CC168797",
        ((StringColumnStatistics) stats[3]).getMaximum());
    assertEquals(9 * 1024, stats[4].getNumberOfValues());
    assertEquals("001277C7986C02D9CDA490756055C6A81F3838D3394F18806DD3359AAD59862A",
        ((StringColumnStatistics) stats[4]).getMinimum());
    assertEquals("FFFF1E62E46263E623F704AC22C2F27E5BBDED8693546A2A11F011251A53D23D",
        ((StringColumnStatistics) stats[4]).getMaximum());
    // check the stripe stats
    List<StripeStatistics> stripeStats = reader.getStripeStatistics();
    for(int s=0; s < stripeStats.size(); ++s) {
      ColumnStatistics[] cs = stripeStats.get(s).getColumnStatistics();
      String msg = "stripe " + s;
      assertEquals(1024, cs[0].getNumberOfValues(), msg);
      assertEquals(0, cs[1].getNumberOfValues(), msg);
      assertEquals(1024, cs[2].getNumberOfValues(), msg);
      assertEquals(1024, cs[3].getNumberOfValues(), msg);
      assertEquals(64, ((StringColumnStatistics) cs[3]).getMinimum().length(), msg);
      assertEquals(64, ((StringColumnStatistics) cs[3]).getMaximum().length(), msg);
      assertEquals(1024, cs[4].getNumberOfValues(), msg);
      assertEquals(64, ((StringColumnStatistics) cs[4]).getMinimum().length(), msg);
      assertEquals(64, ((StringColumnStatistics) cs[4]).getMaximum().length(), msg);
    }
    // check the file contents
    RecordReader rows = reader.rows();
    VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
    LongColumnVector a = (LongColumnVector) batch.cols[0];
    StructColumnVector b = (StructColumnVector) batch.cols[1];
    BytesColumnVector c = (BytesColumnVector) b.fields[0];
    BytesColumnVector d = (BytesColumnVector) b.fields[1];
    for(int btch=0; btch < 9; ++btch) {
      assertTrue(rows.nextBatch(batch));
      assertEquals(1024, batch.size);
      for(int r=0; r < batch.size; ++r) {
        long value = btch * 1024 + r;
        String msg = "batch " + btch + " row " + r;
        assertTrue(a.isNull[r], msg);
        assertEquals(computeSha(Long.toHexString(value)), c.toString(r), msg);
        assertEquals(
            computeSha(String.format("%010x", value * 1_000_001)),
            d.toString(r), msg);
      }
    }
    assertFalse(rows.nextBatch(batch));
    rows.close();
    reader.close();

    // test reading with keys
    reader = OrcFile.createReader(merge1,
        OrcFile.readerOptions(conf).setKeyProvider(keystore));
    assertEquals(9 * 1024, reader.getNumberOfRows());
    keys = reader.getColumnEncryptionKeys();
    assertEquals(2, keys.length);
    assertEquals("pii", keys[0].getKeyName());
    assertEquals(0, keys[0].getKeyVersion());
    assertTrue(keys[0].isAvailable());
    assertEquals("top_secret", keys[1].getKeyName());
    assertEquals(0, keys[1].getKeyVersion());
    assertTrue(keys[1].isAvailable());
    // check the file stats
    stats = reader.getStatistics();
    assertEquals(9 * 1024, stats[0].getNumberOfValues());
    assertEquals(9 * 1024, stats[1].getNumberOfValues());
    assertEquals(0, ((IntegerColumnStatistics) stats[1]).getMinimum());
    assertEquals(9 * 1024 - 1, ((IntegerColumnStatistics) stats[1]).getMaximum());
    assertEquals(9 * 1024, stats[2].getNumberOfValues());
    assertEquals(9 * 1024, stats[3].getNumberOfValues());
    assertEquals("0", ((StringColumnStatistics) stats[3]).getMinimum());
    assertEquals("fff", ((StringColumnStatistics) stats[3]).getMaximum());
    assertEquals(9 * 1024, stats[4].getNumberOfValues());
    assertEquals("0000000000", ((StringColumnStatistics) stats[4]).getMinimum());
    assertEquals("022541e1bf", ((StringColumnStatistics) stats[4]).getMaximum());
    // check the stripe stats
    stripeStats = reader.getStripeStatistics();
    for(int s=0; s < stripeStats.size(); ++s) {
      long low = s * 1024;
      long high = s * 1024 + 1023;
      ColumnStatistics[] cs = stripeStats.get(s).getColumnStatistics();
      String msg = "stripe " + s;
      assertEquals(1024, cs[0].getNumberOfValues(), msg);
      assertEquals(1024, cs[1].getNumberOfValues(), msg);
      assertEquals(low, ((IntegerColumnStatistics) cs[1]).getMinimum(), msg);
      assertEquals(high, ((IntegerColumnStatistics) cs[1]).getMaximum(), msg);
      assertEquals(1024, cs[2].getNumberOfValues(), msg);
      assertEquals(1024, cs[3].getNumberOfValues(), msg);
      assertEquals(Long.toHexString(low),
          ((StringColumnStatistics) cs[3]).getMinimum(), msg);
      assertEquals(s == 0 ? "ff" : Long.toHexString(high),
          ((StringColumnStatistics) cs[3]).getMaximum(), msg);
      assertEquals(1024, cs[4].getNumberOfValues(), msg);
      assertEquals(String.format("%010x", 1_000_001 * low),
          ((StringColumnStatistics) cs[4]).getMinimum(), msg);
      assertEquals(String.format("%010x", 1_000_001 * high),
          ((StringColumnStatistics) cs[4]).getMaximum(), msg);
    }
    // check the file contents
    rows = reader.rows();
    for(int btch=0; btch < 9; ++btch) {
      assertTrue(rows.nextBatch(batch));
      assertEquals(1024, batch.size);
      for(int r=0; r < batch.size; ++r) {
        long value = btch * 1024 + r;
        String msg = "batch " + btch + " row " + r;
        assertEquals(value, a.vector[r], msg);
        assertEquals(Long.toHexString(value), c.toString(r), msg);
        assertEquals(String.format("%010x", value * 1_000_001), d.toString(r), msg);
      }
    }
    assertFalse(rows.nextBatch(batch));
    rows.close();
    reader.close();

    Path merge2 = new Path(workDir, "TestVectorOrcFile.testEncryptMerge.merge2-" +
                                         fileFormat.getName() + ".orc");
    fs.delete(merge2, false);
    paths = OrcFile.mergeFiles(merge2,
        OrcFile.writerOptions(conf).setKeyProvider(keystore),
        Arrays.asList(input2, input4, input1, input5));

    // make sure only input1 & input2 were merged
    assertEquals(2, paths.size());
    assertTrue(paths.contains(input1));
    assertTrue(paths.contains(input2));

    reader = OrcFile.createReader(merge2, OrcFile.readerOptions(conf));
    assertEquals(2 * 3 * 1024, reader.getNumberOfRows());
    assertEquals(CompressionKind.ZLIB, reader.getCompressionKind());
    assertEquals(0x800, reader.getCompressionSize());
    assertEquals(1000, reader.getRowIndexStride());
    assertEquals(fileFormat, reader.getFileVersion());
    assertEquals(6, reader.getStripes().size());
    assertEquals(2, reader.getColumnEncryptionKeys().length);
    assertEquals(2, reader.getDataMasks().length);
    assertEquals(2, reader.getEncryptionVariants().length);
    reader.close();
  }

  Path exampleDir = new Path(System.getProperty("example.dir",
      "../../examples/"));

  @ParameterizedTest
  @MethodSource("data")
  public void testZeroByteOrcFile(Version fileFormat) throws Exception {
    // we only have to run this test once, since it is a 0 byte file.
    assumeTrue(fileFormat == OrcFile.Version.V_0_11);
    Path zeroFile = new Path(exampleDir, "zero.orc");
    Reader reader = OrcFile.createReader(zeroFile, OrcFile.readerOptions(conf));
    assertEquals(0, reader.getNumberOfRows());
    assertEquals("struct<>", reader.getSchema().toString());
    assertEquals(CompressionKind.NONE, reader.getCompressionKind());
    assertEquals(0, reader.getRawDataSize());
    assertEquals(0, reader.getRowIndexStride());
    assertEquals(DEFAULT_COMPRESSION_BLOCK_SIZE, reader.getCompressionSize());
    assertEquals(0, reader.getMetadataSize());
    assertEquals(OrcFile.Version.CURRENT, reader.getFileVersion());
    assertEquals(0, reader.getStripes().size());
    assertEquals(0, reader.getStatistics().length);
    assertEquals(0, reader.getMetadataKeys().size());
    assertEquals(OrcFile.CURRENT_WRITER, reader.getWriterVersion());
    VectorizedRowBatch batch =
        TypeDescription.fromString("struct<>").createRowBatch();
    assertFalse(reader.rows().nextBatch(batch));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testFutureOrcFile(Version fileFormat) throws Exception {
    assumeTrue(fileFormat == OrcFile.Version.V_0_11);
    Path zeroFile = new Path(exampleDir, "version1999.orc");
    try {
      OrcFile.createReader(zeroFile, OrcFile.readerOptions(conf));
      fail("no exception for bad version");
    } catch (IOException e) {
      String m = e.getMessage();
      assertTrue(m.contains("version1999.orc was written by a future ORC version 19.99."));
      assertTrue(m.contains("This file is not readable by this version of ORC."));
      assertTrue(m.contains("Postscript: footerLength: 19 compression: NONE " +
          "compressionBlockSize: 65536 version: 19 version: 99 metadataLength: 0 " +
          "writerVersion: 1"));
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testEmptyDoubleStream(Version fileFormat) throws Exception {
    TypeDescription schema =
        TypeDescription.fromString("struct<list1:array<double>," +
            "list2:array<float>>");
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema).version(fileFormat));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 2;
    ListColumnVector list1 = (ListColumnVector) batch.cols[0];
    ListColumnVector list2 = (ListColumnVector) batch.cols[1];
    for(int r=0; r < batch.size; ++r) {
      list1.offsets[r] = 0;
      list1.lengths[r] = 0;
      list2.offsets[r] = 0;
      list2.lengths[r] = 0;
    }
    writer.addRowBatch(batch);
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    assertTrue(rows.nextBatch(batch));
    assertEquals(2, batch.size);
    list1 = (ListColumnVector) batch.cols[0];
    list2 = (ListColumnVector) batch.cols[1];
    for(int r=0; r < batch.size; ++r) {
      assertEquals(0, list1.lengths[r]);
      assertEquals(0, list2.lengths[r]);
    }
    assertFalse(rows.nextBatch(batch));
    rows.close();
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testPredicatePushdownForComplex(Version fileFormat) throws Exception {
    TypeDescription schema = createComplexInnerSchema();
    Writer writer = OrcFile.createWriter(testFilePath,
            OrcFile.writerOptions(conf)
                    .setSchema(schema)
                    .stripeSize(400000L)
                    .compress(CompressionKind.NONE)
                    .bufferSize(500)
                    .rowIndexStride(1000)
                    .version(fileFormat));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.ensureSize(3500);
    batch.size = 3500;
    for(int i=0; i < 3500; ++i) {
      ((LongColumnVector) batch.cols[0]).vector[i] = i;
      ((LongColumnVector)((StructColumnVector) batch.cols[1]).fields[0]).vector[i] = i * 300;
      ((BytesColumnVector)((StructColumnVector) batch.cols[1]).fields[1]).setVal(i,
              Integer.toHexString(10*i).getBytes(StandardCharsets.UTF_8));
    }
    writer.addRowBatch(batch);
    writer.close();
    Reader reader = OrcFile.createReader(testFilePath,
            OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(3500, reader.getNumberOfRows());

    SearchArgument sarg = SearchArgumentFactory.newBuilder()
            .startAnd()
            .startNot()
            .lessThan("complex.int2", PredicateLeaf.Type.LONG, 300000L)
            .end()
            .lessThan("complex.int2", PredicateLeaf.Type.LONG, 600000L)
            .end()
            .build();

    RecordReader rows = reader.rows(reader.options()
            .range(0L, Long.MAX_VALUE)
            .include(new boolean[]{true, true, true, true, true})
            .searchArgument(sarg, new String[]{null, "int1", "complex","int2","string1"}));
    batch = reader.getSchema().createRowBatch(2000);
    LongColumnVector ints1 = (LongColumnVector) batch.cols[0];
    StructColumnVector struct1 = (StructColumnVector) batch.cols[1];
    LongColumnVector ints2 = (LongColumnVector) struct1.fields[0];
    BytesColumnVector strs = (BytesColumnVector) struct1.fields[1];

    assertEquals(1000L, rows.getRowNumber());
    assertTrue(rows.nextBatch(batch));
    assertEquals(1000, batch.size);

    for(int i=1000; i < 2000; ++i) {
      assertEquals(i,ints1.vector[i-1000]);
      assertEquals(300 * i, ints2.vector[i - 1000]);
      assertEquals(Integer.toHexString(10*i), strs.toString(i - 1000));
    }
    assertFalse(rows.nextBatch(batch));
    assertEquals(3500, rows.getRowNumber());


    // look through the file with no rows selected
    sarg = SearchArgumentFactory.newBuilder()
            .startAnd()
            .lessThan("complex.int2", PredicateLeaf.Type.LONG, 0L)
            .end()
            .build();
    rows = reader.rows(reader.options()
            .range(0L, Long.MAX_VALUE)
            .include(new boolean[]{true, true, true, true, true})
            .searchArgument(sarg, new String[]{null, "int1",null,"int2","string1"}));
    assertEquals(3500L, rows.getRowNumber());
    assertFalse(rows.nextBatch(batch));

    // select first 100 and last 100 rows
    sarg = SearchArgumentFactory.newBuilder()
            .startOr()
            .lessThan("complex.int2", PredicateLeaf.Type.LONG, 300L * 100)
            .startNot()
            .lessThan("complex.int2", PredicateLeaf.Type.LONG, 300L * 3400)
            .end()
            .end()
            .build();
    rows = reader.rows(reader.options()
            .range(0L, Long.MAX_VALUE)
            .include(new boolean[]{true, true,true,true, true})
            .searchArgument(sarg, new String[]{null, "int1",null, "int2","string1"})
            .allowSARGToFilter(false));
    assertEquals(0, rows.getRowNumber());
    assertTrue(rows.nextBatch(batch));
    assertEquals(1000, batch.size);
    assertEquals(3000, rows.getRowNumber());

    for(int i=0; i < 1000; ++i) {
      assertEquals(300 * i, ints2.vector[i]);
      assertEquals(Integer.toHexString(10*i), strs.toString(i));
    }

    assertTrue(rows.nextBatch(batch));
    assertEquals(500, batch.size);
    assertEquals(3500, rows.getRowNumber());
    for(int i=3000; i < 3500; ++i) {
      assertEquals(300 * i, ints2.vector[i - 3000]);
      assertEquals(Integer.toHexString(10*i), strs.toString(i - 3000));
    }
    assertFalse(rows.nextBatch(batch));
    assertEquals(3500, rows.getRowNumber());
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testPredicatePushdownWithNan(Version fileFormat) throws Exception {
    TypeDescription schema = TypeDescription.createStruct()
        .addField("double1", TypeDescription.createDouble())
        .addField("float1", TypeDescription.createFloat());

    Writer writer = OrcFile.createWriter(testFilePath,
            OrcFile.writerOptions(conf)
                    .setSchema(schema)
                    .stripeSize(400000L)
                    .compress(CompressionKind.NONE)
                    .bufferSize(500)
                    .rowIndexStride(1000)
                    .version(fileFormat));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.ensureSize(3500);
    batch.size = 3500;
    batch.cols[0].noNulls = true;
    batch.cols[1].noNulls = true;

    DoubleColumnVector dbcol = ((DoubleColumnVector) batch.cols[0]);
    DoubleColumnVector fcol = ((DoubleColumnVector) batch.cols[1]);

    // first row NaN (resulting to min/max and sum columnStats of stride to be NaN)
    // NaN in the middle of a stride causes Sum of last stride to be NaN
    dbcol.vector[0] = Double.NaN;
    fcol.vector[0] = Double.NaN;
    for (int i=1; i < 3500; ++i) {
      dbcol.vector[i] = i == 3200 ? Double.NaN : i;
      fcol.vector[i] = i == 3200 ? Double.NaN : i;
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
            OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(3500, reader.getNumberOfRows());

    // Only the first stride matches the predicate, just need to make sure NaN stats are ignored
    // Test double category push down
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
            .startAnd()
            .lessThan("double1", PredicateLeaf.Type.FLOAT, 100d)
            .end()
            .build();

    RecordReader rows = reader.rows(reader.options()
            .range(0L, Long.MAX_VALUE)
            .searchArgument(sarg, new String[]{"double1"})
            .allowSARGToFilter(false));
    batch = reader.getSchema().createRowBatch(3500);

    rows.nextBatch(batch);
    // First stride should be read as NaN sum is ignored
    assertEquals(1000, batch.size);

    rows.nextBatch(batch);
    // Last stride should be read as NaN sum is ignored
    assertEquals(500, batch.size);

    rows.nextBatch(batch);
    assertEquals(0, batch.size);

    // Test float category push down
    sarg = SearchArgumentFactory.newBuilder()
        .startAnd()
        .lessThan("float1", PredicateLeaf.Type.FLOAT, 100d)
        .end()
        .build();

    rows = reader.rows(reader.options()
        .range(0L, Long.MAX_VALUE)
        .searchArgument(sarg, new String[]{"float1"})
        .allowSARGToFilter(false));
    batch = reader.getSchema().createRowBatch(3500);

    rows.nextBatch(batch);
    // First stride should be read as NaN sum is ignored
    assertEquals(1000, batch.size);

    rows.nextBatch(batch);
    // Last stride should be read as NaN sum is ignored
    assertEquals(500, batch.size);

    rows.nextBatch(batch);
    assertEquals(0, batch.size);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testPredicatePushdownWithSumOverflow(Version fileFormat) throws Exception {
    TypeDescription schema = TypeDescription.createStruct()
        .addField("double1", TypeDescription.createDouble())
        .addField("float1", TypeDescription.createFloat());

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(400000L)
            .compress(CompressionKind.NONE)
            .bufferSize(500)
            .rowIndexStride(1000)
            .version(fileFormat));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.ensureSize(3500);
    batch.size = 3500;
    batch.cols[0].noNulls = true;
    batch.cols[1].noNulls = true;

    DoubleColumnVector dbcol = ((DoubleColumnVector) batch.cols[0]);
    DoubleColumnVector fcol = ((DoubleColumnVector) batch.cols[1]);

    double largeNumber = Double.MAX_VALUE / 2 + Double.MAX_VALUE / 4;

    // Here we are writing 3500 rows of data, with stripeSize set to 400000
    // and rowIndexStride set to 1000, so 1 stripe will be written,
    // indexed in 4 strides.
    // Two large values are written in the first and fourth strides,
    // causing the statistical sum to overflow, sum is not a finite value,
    // but this does not prevent pushing down (range comparisons work fine)
    fcol.vector[0] = dbcol.vector[0] = largeNumber;
    fcol.vector[1] = dbcol.vector[1] = largeNumber;
    for (int i=2; i < 3500; ++i) {
      if (i >= 3200 && i<= 3201) {
        fcol.vector[i] = dbcol.vector[i] = largeNumber;
      } else {
        dbcol.vector[i] = i;
        fcol.vector[i] = i;
      }
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(3500, reader.getNumberOfRows());

    // Test double category push down
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startAnd()
        .lessThan("double1", PredicateLeaf.Type.FLOAT, 100d)
        .end()
        .build();

    RecordReader rows = reader.rows(reader.options()
        .range(0L, Long.MAX_VALUE)
        .searchArgument(sarg, new String[]{"double1"}));
    batch = reader.getSchema().createRowBatch(3500);

    rows.nextBatch(batch);
    // First stride should be read
    assertEquals(1000, batch.size);

    rows.nextBatch(batch);
    // Last stride should not be read, even if sum is not finite
    assertEquals(0, batch.size);

    // Test float category push down
    sarg = SearchArgumentFactory.newBuilder()
        .startAnd()
        .lessThan("float1", PredicateLeaf.Type.FLOAT, 100d)
        .end()
        .build();

    rows = reader.rows(reader.options()
        .range(0L, Long.MAX_VALUE)
        .searchArgument(sarg, new String[]{"float1"}));
    batch = reader.getSchema().createRowBatch(3500);

    rows.nextBatch(batch);
    // First stride should be read
    assertEquals(1000, batch.size);

    rows.nextBatch(batch);
    // Last stride should not be read, even if sum is not finite
    assertEquals(0, batch.size);
  }

  /**
   * Test predicate pushdown on nulls, with different combinations of
   * values and nulls.
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testPredicatePushdownAllNulls(Version fileFormat) throws Exception {
    TypeDescription schema = createInnerSchema();
    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema).rowIndexStride(1024).version(fileFormat))) {
      VectorizedRowBatch batch = schema.createRowBatch();
      batch.size = 1024;

      // write 1024 rows of (null, "val")
      batch.cols[0].noNulls = false;
      batch.cols[0].isNull[0] = true;
      batch.cols[0].isRepeating = true;
      batch.cols[1].isRepeating = true;
      ((BytesColumnVector) batch.cols[1]).setVal(0, "val".getBytes(StandardCharsets.UTF_8));
      writer.addRowBatch(batch);

      // write 1024 rows of (123, null)
      batch.cols[0].isNull[0] = false;
      ((LongColumnVector) batch.cols[0]).vector[0] = 123;
      batch.cols[1].noNulls = false;
      batch.cols[1].isNull[0] = true;
      writer.addRowBatch(batch);

      // write 1024 rows of (null, null)
      batch.cols[0].isNull[0] = true;
      writer.addRowBatch(batch);
    }

    try (Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs))) {
      assertEquals(3072, reader.getNumberOfRows());
      VectorizedRowBatch batch = reader.getSchema().createRowBatch();

      // int1 is not null
      SearchArgument sarg =
          SearchArgumentFactory.newBuilder()
              .startNot()
              .isNull("int1", PredicateLeaf.Type.LONG)
              .end()
              .build();
      // should find one row group
      try (RecordReader rows = reader.rows(reader.options().searchArgument(sarg, new String[]{}))) {
        rows.nextBatch(batch);
        assertEquals(1024, batch.size);
        assertTrue(batch.cols[0].isRepeating);
        assertEquals(123, ((LongColumnVector) batch.cols[0]).vector[0]);
        assertFalse(rows.nextBatch(batch));
      }

      // string1 is not null
      sarg = SearchArgumentFactory.newBuilder()
          .startNot()
          .isNull("string1", PredicateLeaf.Type.STRING)
          .end()
          .build();
      // should find one row group
      try (RecordReader rows = reader.rows(reader.options().searchArgument(sarg, new String[]{}))) {
        rows.nextBatch(batch);
        assertEquals(1024, batch.size);
        assertTrue(batch.cols[1].isRepeating);
        assertEquals("val", ((BytesColumnVector) batch.cols[1]).toString(0));
        assertFalse(rows.nextBatch(batch));
      }
    }
  }

  /**
   * Write three row groups, one with (null, null), one with (1, "val"), and one with
   * alternating rows.
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testPredicatePushdownMixedNulls(Version fileFormat) throws Exception {
    TypeDescription schema = createInnerSchema();
    try (Writer writer = OrcFile.createWriter(testFilePath,
                           OrcFile.writerOptions(conf)
                                  .setSchema(schema)
                                  .rowIndexStride(1024)
                                  .version(fileFormat))) {
      VectorizedRowBatch batch = schema.createRowBatch();
      batch.cols[0].noNulls = false;
      batch.cols[1].noNulls = false;
      batch.size = 1024;
      for (int b = 0; b < 3; ++b) {
        for (int i = 0; i < batch.size; ++i) {
          if (b == 0 || (b == 2 && i % 2 == 0)) {
            batch.cols[0].isNull[i] = true; // every other value is null or 1
            batch.cols[1].isNull[i] = true; // every other value is null or "val"
          } else {
            batch.cols[0].isNull[i] = false;
            ((LongColumnVector) batch.cols[0]).vector[i] = 1;
            batch.cols[1].isNull[i] = false;
            ((BytesColumnVector) batch.cols[1]).setVal(i, "val".getBytes(StandardCharsets.UTF_8));
          }
        }
        writer.addRowBatch(batch);
      }
    }

    try (Reader reader = OrcFile.createReader(testFilePath,
                          OrcFile.readerOptions(conf).filesystem(fs))) {
      assertEquals(3*1024, reader.getNumberOfRows());
      VectorizedRowBatch batch = reader.getSchema().createRowBatch();

      // int1 not in (1) -- should select 0 of the row groups
      SearchArgument sarg =
          SearchArgumentFactory.newBuilder()
              .startNot()
              .in("int1", PredicateLeaf.Type.LONG, 1L)
              .end().build();

      try (RecordReader rows = reader.rows(reader.options().searchArgument(sarg, new String[]{}))) {
        assertFalse(rows.nextBatch(batch));
      }


      // string1 not in ("val") -- should select 0 of the row groups
      sarg = SearchArgumentFactory.newBuilder()
          .startNot()
          .in("string1", PredicateLeaf.Type.STRING, "val")
          .end().build();

      try (RecordReader rows = reader.rows(reader.options().searchArgument(sarg, new String[]{}))) {
        assertFalse(rows.nextBatch(batch));
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testColumnEncryption(Version fileFormat) throws Exception {
    assumeTrue(fileFormat != OrcFile.Version.V_0_11);
    final int ROWS = 1000;
    final int SEED = 2;
    final Random random = new Random(SEED);

    TypeDescription schema =
        TypeDescription.fromString("struct<i:int,norm:int,x:array<string>,j:int>");

    byte[] piiKey = new byte[16];
    random.nextBytes(piiKey);
    byte[] creditKey = new byte[32];
    random.nextBytes(creditKey);
    InMemoryKeystore keys = new InMemoryKeystore(random)
        .addKey("pii", EncryptionAlgorithm.AES_CTR_128, piiKey)
        .addKey("credit", EncryptionAlgorithm.AES_CTR_256, creditKey);
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .version(fileFormat)
            .setKeyProvider(keys)
            .encrypt("pii:i,j;credit:x")
            .masks((String)OrcConf.DATA_MASK.getDefaultValue()));
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = ROWS;
    LongColumnVector i = (LongColumnVector) batch.cols[0];
    LongColumnVector norm = (LongColumnVector) batch.cols[1];
    ListColumnVector x = (ListColumnVector) batch.cols[2];
    BytesColumnVector xElem = (BytesColumnVector) x.child;
    xElem.ensureSize(3 * ROWS, false);
    LongColumnVector j = (LongColumnVector) batch.cols[3];
    for(int r=0; r < ROWS; ++r) {
      i.vector[r] = r * 3;
      j.vector[r] = r * 7;
      norm.vector[r] = r * 5;
      int start = x.childCount;
      x.offsets[r] = start;
      x.lengths[r] = 3;
      x.childCount += x.lengths[r];
      for(int child=0; child < x.lengths[r]; ++child) {
        xElem.setVal(start + child,
            String.format("%d.%d", r, child).getBytes(StandardCharsets.UTF_8));
      }
    }
    writer.addRowBatch(batch);
    writer.close();

    // Read without any keys
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf)
               .setKeyProvider(new InMemoryKeystore()));
    ColumnStatistics[] stats = reader.getStatistics();
    assertEquals(ROWS, stats[0].getNumberOfValues());

    assertEquals(0, stats[1].getNumberOfValues());
    assertTrue(stats[1].hasNull());
    assertEquals(ROWS, stats[2].getNumberOfValues());
    assertEquals(0, ((IntegerColumnStatistics) stats[2]).getMinimum());
    assertEquals(ROWS * 5 - 5, ((IntegerColumnStatistics) stats[2]).getMaximum());
    assertEquals(0, stats[3].getNumberOfValues());
    assertEquals(0, stats[4].getNumberOfValues());

    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    i = (LongColumnVector) batch.cols[0];
    norm = (LongColumnVector) batch.cols[1];
    x = (ListColumnVector) batch.cols[2];
    j = (LongColumnVector) batch.cols[3];

    // ensure that we get the right number of rows with all nulls
    assertTrue(rows.nextBatch(batch));
    assertEquals(ROWS, batch.size);
    assertTrue(i.isRepeating);
    assertFalse(i.noNulls);
    assertTrue(i.isNull[0]);
    assertTrue(j.isRepeating);
    assertFalse(j.noNulls);
    assertTrue(j.isNull[0]);
    assertTrue(x.isRepeating);
    assertFalse(x.noNulls);
    assertTrue(x.isNull[0]);
    for(int r=0; r < ROWS; ++r) {
      assertEquals(r * 5, norm.vector[r], "row " + r);
    }
    assertFalse(rows.nextBatch(batch));
    rows.close();

    // Add a new version of the pii key
    random.nextBytes(piiKey);
    keys.addKey("pii", 1, EncryptionAlgorithm.AES_CTR_128, piiKey);

    // Read with the keys
    reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf)
               .setKeyProvider(keys));
    stats = reader.getStatistics();
    assertEquals(ROWS, stats[0].getNumberOfValues());
    assertEquals(ROWS, stats[1].getNumberOfValues());
    assertEquals(0, ((IntegerColumnStatistics) stats[1]).getMinimum());
    assertEquals(3 * (ROWS - 1), ((IntegerColumnStatistics) stats[1]).getMaximum());
    assertEquals(0, ((IntegerColumnStatistics) stats[2]).getMinimum());
    assertEquals(5 * (ROWS - 1), ((IntegerColumnStatistics) stats[2]).getMaximum());
    assertEquals(ROWS, stats[3].getNumberOfValues());
    assertEquals(3 * ROWS, stats[4].getNumberOfValues());
    assertEquals("0.0", ((StringColumnStatistics)stats[4]).getMinimum());
    assertEquals("999.2", ((StringColumnStatistics)stats[4]).getMaximum());
    assertEquals(ROWS, stats[5].getNumberOfValues());
    assertEquals(0, ((IntegerColumnStatistics) stats[5]).getMinimum());
    assertEquals(7 * (ROWS - 1), ((IntegerColumnStatistics) stats[5]).getMaximum());

    rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    i = (LongColumnVector) batch.cols[0];
    norm = (LongColumnVector) batch.cols[1];
    x = (ListColumnVector) batch.cols[2];
    j = (LongColumnVector) batch.cols[3];
    xElem = (BytesColumnVector) x.child;
    assertTrue(rows.nextBatch(batch));
    assertEquals(ROWS, batch.size);
    assertFalse(i.isRepeating);
    assertFalse(x.isRepeating);
    assertFalse(xElem.isRepeating);
    assertFalse(j.isRepeating);
    assertTrue(i.noNulls);
    assertTrue(x.noNulls);
    assertTrue(xElem.noNulls);
    assertTrue(j.noNulls);
    for(int r=0; r < ROWS; ++r) {
      String msg = "row " + r;
      assertEquals(r * 3, i.vector[r], msg);
      assertEquals(r * 5, norm.vector[r], msg);
      assertEquals(r * 3, x.offsets[r], msg);
      assertEquals(3, x.lengths[r], msg);
      for(int child=0; child < x.lengths[r]; ++child) {
        assertEquals(String.format("%d.%d", r, child),
            xElem.toString((int) x.offsets[r] + child), msg);
      }
      assertEquals(r * 7, j.vector[r], msg);
    }
    assertFalse(rows.nextBatch(batch));
    rows.close();
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testMultiStripeColumnEncryption(Version fileFormat) throws Exception {
    assumeTrue(fileFormat != OrcFile.Version.V_0_11);
    final EncryptionAlgorithm algorithm = EncryptionAlgorithm.AES_CTR_128;
    final int BATCHES = 100;
    final int SEED = 3;
    final Random random = new Random(SEED);

    TypeDescription schema = TypeDescription.fromString(
        "struct<dec:decimal(20,4)," +
            "dt:date," +
            "time:timestamp," +
            "dbl:double," +
            "bool:boolean," +
            "bin:binary>");

    InMemoryKeystore allKeys = new InMemoryKeystore();
    byte[][] keys = new byte[6][];
    for(int k=0; k < keys.length; ++k) {
      keys[k] = new byte[algorithm.keyLength()];
      random.nextBytes(keys[k]);
      allKeys.addKey("key_" + k, algorithm, keys[k]);
    }

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .version(fileFormat)
            .stripeSize(10000)
            .setKeyProvider(allKeys)
            .encrypt("key_0:dec;key_1:dt;key_2:time;key_3:dbl;key_4:bool;key_5:bin"));
    // Set size to 1000 precisely so that stripes are exactly 5000 rows long.
    VectorizedRowBatch batch = schema.createRowBatch(1000);
    DecimalColumnVector dec = (DecimalColumnVector) batch.cols[0];
    LongColumnVector dt = (LongColumnVector) batch.cols[1];
    TimestampColumnVector time = (TimestampColumnVector) batch.cols[2];
    DoubleColumnVector dbl = (DoubleColumnVector) batch.cols[3];
    LongColumnVector bool = (LongColumnVector) batch.cols[4];
    BytesColumnVector bin = (BytesColumnVector) batch.cols[5];
    // Generate 100 batches of 1,000 rows each
    batch.size = 1000;
    dec.isRepeating = true;
    dt.isRepeating = true;
    time.isRepeating = true;
    dbl.isRepeating = true;
    bool.isRepeating = true;
    bin.isRepeating = true;
    for(int b=0; b < BATCHES; ++b) {
      dec.set(0, new HiveDecimalWritable(String.format("%d.%03d", b, b)));
      dt.vector[0] = new DateWritable(new Date(96 + b, 12, 11)).getDays();
      time.set(0, Timestamp.valueOf(String.format("2014-12-14 12:00:00.%04d", b)));
      dbl.vector[0] = b + 0.5;
      bool.vector[0] = b % 2;
      bin.setVal(0, Integer.toString(b).getBytes(StandardCharsets.UTF_8));
      writer.addRowBatch(batch);
    }
    writer.close();

    // Read without any keys
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf)
            .setKeyProvider(new InMemoryKeystore()));
    checkHasData(reader.rows(), batch, BATCHES,
        false, false, false, false, false, false);

    // read with all of the keys
    reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf)
            .setKeyProvider(allKeys));
    checkHasData(reader.rows(), batch, BATCHES,
        true, true, true, true, true, true);

    // try enabling each key by itself
    for(int c=0; c < 6; ++c) {
      InMemoryKeystore single = new InMemoryKeystore();
      single.addKey("key_" + c, algorithm, keys[c]);
      reader = OrcFile.createReader(testFilePath,
          OrcFile.readerOptions(conf).setKeyProvider(single));
      boolean[] hasData = new boolean[6];
      hasData[c] = true;
      checkHasData(reader.rows(), batch, BATCHES, hasData);
    }
  }

  private void checkHasData(RecordReader reader, VectorizedRowBatch batch,
                            int BATCHES, boolean... hasData) throws IOException {
    for(int b=0; b < BATCHES; ++b) {
      assertTrue(reader.nextBatch(batch), "batch " + b);
      for(int c=0; c < hasData.length; c++) {
        if (hasData[c]) {
          // the expected value
          String expected = null;
          // a function from the row to the value as a string
          IntFunction<String> actual = row -> null;
          switch (c) {
          case 0:
            expected = new HiveDecimalWritable(String.format("%d.%03d", b, b)).toString();
            actual = row -> ((DecimalColumnVector) batch.cols[0]).vector[row].toString();
            break;
          case 1:
            expected = Long.toString(new DateWritable(new Date(96 + b, 12, 11)).getDays());
            actual = row -> Long.toString(((LongColumnVector) batch.cols[1]).vector[row]);
            break;
          case 2:
            expected = Timestamp.valueOf(String.format("2014-12-14 12:00:00.%04d", b)).toString();
            actual = row -> ((TimestampColumnVector) batch.cols[2]).asScratchTimestamp(row).toString();
            break;
          case 3:
            expected = Double.toString(b + 0.5);
            actual = row -> Double.toString(((DoubleColumnVector) batch.cols[3]).vector[row]);
            break;
          case 4:
            expected = Long.toString(b % 2);
            actual = row -> Long.toString(((LongColumnVector) batch.cols[4]).vector[row]);
            break;
          default:
            expected = Integer.toString(b);
            actual = row -> ((BytesColumnVector) batch.cols[5]).toString(row);
            break;
          }
          assertTrue(batch.cols[c].noNulls, "batch " + b + " column " + c);
          assertEquals(expected, actual.apply(0), "batch " + b + " column " + c + " row 0");
          // Not all of the readers set isRepeating, so if it isn't set, check the values.
          if (!batch.cols[c].isRepeating) {
            for(int r=1; r < batch.size; ++r) {
              assertEquals(expected, actual.apply(r), "batch " + b + " column " + c + " row " + r);
            }
          }
        } else {
          assertTrue(batch.cols[c].isRepeating, "batch " + b + " column " + c);
          assertTrue(batch.cols[c].isNull[0], "batch " + b + " column " + c);
        }
      }
    }
    assertFalse(reader.nextBatch(batch), "end");
    reader.close();
  }
}
