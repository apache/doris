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

package org.apache.orc.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentImpl;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.CompressionCodec;
import org.apache.orc.CompressionKind;
import org.apache.orc.DataReader;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TestVectorOrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.impl.RecordReaderImpl.Location;
import org.apache.orc.impl.RecordReaderImpl.SargApplier;
import org.apache.orc.impl.reader.ReaderEncryption;
import org.apache.orc.impl.reader.StripePlanner;
import org.apache.orc.impl.reader.tree.TypeReader;
import org.apache.orc.impl.writer.StreamOptions;
import org.apache.orc.util.BloomFilter;
import org.apache.orc.util.BloomFilterIO;
import org.apache.orc.util.BloomFilterUtf8;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.chrono.ChronoLocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import static org.apache.orc.impl.RecordReaderUtils.MAX_BYTE_WIDTH;
import static org.apache.orc.impl.RecordReaderUtils.MAX_VALUES_LENGTH;
import static org.apache.orc.OrcFile.CURRENT_WRITER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestRecordReaderImpl {

  // This is a work around until we update storage-api to allow ChronoLocalDate in
  // predicates.
  static Date toDate(ChronoLocalDate date) {
    return new Date(date.atTime(LocalTime.MIDNIGHT).atZone(ZoneId.systemDefault())
        .toEpochSecond() * 1000);
  }

  @Test
  public void testFindColumn() throws Exception {
    Configuration conf = new Configuration();
    TypeDescription file = TypeDescription.fromString("struct<a:int,c:string,e:int>");
    TypeDescription reader = TypeDescription.fromString("struct<a:int,b:double,c:string,d:double,e:bigint>");
    SchemaEvolution evo = new SchemaEvolution(file, reader, new Reader.Options(conf));
    assertEquals(1, RecordReaderImpl.findColumns(evo, "a"));
    assertEquals(-1, RecordReaderImpl.findColumns(evo, "b"));
    assertEquals(2, RecordReaderImpl.findColumns(evo, "c"));
    assertEquals(-1, RecordReaderImpl.findColumns(evo, "d"));
    assertEquals(3, RecordReaderImpl.findColumns(evo, "e"));
  }

  @Test
  public void testFindColumnCaseInsensitively() throws Exception {
    Configuration conf = new Configuration();
    TypeDescription file = TypeDescription.fromString("struct<A:int>");
    TypeDescription reader = TypeDescription.fromString("struct<a:int>");
    conf.setBoolean("orc.schema.evolution.case.sensitive", false);
    SchemaEvolution evo = new SchemaEvolution(file, reader, new Reader.Options(conf));
    assertEquals(1, RecordReaderImpl.findColumns(evo, "A"));
  }

  @Test
  public void testForcePositionalEvolution() throws Exception {
    Configuration conf = new Configuration();

    Path oldFilePath = new Path(TestVectorOrcFile.getFileFromClasspath("orc-file-11-format.orc"));
    Reader reader = OrcFile.createReader(oldFilePath,
        OrcFile.readerOptions(conf).filesystem(FileSystem.getLocal(conf)));

    TypeDescription fileSchema =
        TypeDescription.fromString("struct<col0:boolean,col1:tinyint,col2:smallint,"
            + "col3:int,col4:bigint,col5:float,col6:double,col7:"
            + "binary,col8:string,col9:struct<list:array<struct<int1:int,"
            + "string1:string>>>,col10:array<struct<int1:int,string1:string>>,"
            + "col11:map<string,struct<int1:int,string1:string>>,col12:timestamp,"
            + "col13:decimal(38,10)>");

    SchemaEvolution evo = new SchemaEvolution(fileSchema, reader.getSchema(),
        new Reader.Options(conf).forcePositionalEvolution(true));
    assertEquals(4, RecordReaderImpl.findColumns(evo, "int1"));

    evo = new SchemaEvolution(fileSchema, reader.getSchema(),
        new Reader.Options(conf).forcePositionalEvolution(false));
    assertEquals(-1, RecordReaderImpl.findColumns(evo, "int1"));

    TypeDescription acidSchema = SchemaEvolution.createEventSchema(fileSchema);

    SchemaEvolution evoAcid =
        new SchemaEvolution(acidSchema, reader.getSchema(),
            new Reader.Options(conf).forcePositionalEvolution(true));
    // ahead by 6 for 1 struct + 5 for row-id
    assertEquals(6+4, RecordReaderImpl.findColumns(evoAcid, "int1"));

    evoAcid =
        new SchemaEvolution(acidSchema, reader.getSchema(),
            new Reader.Options(conf).forcePositionalEvolution(false));
    assertEquals(-1, RecordReaderImpl.findColumns(evoAcid, "int1"));
  }

  /**
   * Create a predicate leaf. This is used by another test.
   */
  public static PredicateLeaf createPredicateLeaf(PredicateLeaf.Operator operator,
                                                  PredicateLeaf.Type type,
                                                  String columnName,
                                                  Object literal,
                                                  List<Object> literalList) {
    if (literal instanceof ChronoLocalDate) {
      literal = toDate((ChronoLocalDate) literal);
    }
    return new SearchArgumentImpl.PredicateLeafImpl(operator, type, columnName,
        literal, literalList);
  }

  static class BufferInStream
      extends InputStream implements PositionedReadable, Seekable {
    private final byte[] buffer;
    private final int length;
    private int position = 0;

    BufferInStream(byte[] bytes, int length) {
      this.buffer = bytes;
      this.length = length;
    }

    @Override
    public int read() {
      if (position < length) {
        return buffer[position++];
      }
      return -1;
    }

    @Override
    public int read(byte[] bytes, int offset, int length) {
      int lengthToRead = Math.min(length, this.length - this.position);
      if (lengthToRead >= 0) {
        for(int i=0; i < lengthToRead; ++i) {
          bytes[offset + i] = buffer[position++];
        }
        return lengthToRead;
      } else {
        return -1;
      }
    }

    @Override
    public int read(long position, byte[] bytes, int offset, int length) {
      this.position = (int) position;
      return read(bytes, offset, length);
    }

    @Override
    public void readFully(long position, byte[] bytes, int offset,
                          int length) throws IOException {
      this.position = (int) position;
      while (length > 0) {
        int result = read(bytes, offset, length);
        offset += result;
        length -= result;
        if (result < 0) {
          throw new IOException("Read past end of buffer at " + offset);
        }
      }
    }

    @Override
    public void readFully(long position, byte[] bytes) throws IOException {
      readFully(position, bytes, 0, bytes.length);
    }

    @Override
    public void seek(long position) {
      this.position = (int) position;
    }

    @Override
    public long getPos() {
      return position;
    }

    @Override
    public boolean seekToNewSource(long position) throws IOException {
      this.position = (int) position;
      return false;
    }
  }

  @Test
  public void testMaxLengthToReader() throws Exception {
    Configuration conf = new Configuration();
    OrcProto.Type rowType = OrcProto.Type.newBuilder()
        .setKind(OrcProto.Type.Kind.STRUCT).build();
    OrcProto.Footer footer = OrcProto.Footer.newBuilder()
        .setHeaderLength(0).setContentLength(0).setNumberOfRows(0)
        .setRowIndexStride(0).addTypes(rowType).build();
    OrcProto.PostScript ps = OrcProto.PostScript.newBuilder()
        .setCompression(OrcProto.CompressionKind.NONE)
        .setFooterLength(footer.getSerializedSize())
        .setMagic("ORC").addVersion(0).addVersion(11).build();
    DataOutputBuffer buffer = new DataOutputBuffer();
    footer.writeTo(buffer);
    ps.writeTo(buffer);
    buffer.write(ps.getSerializedSize());
    FileSystem fs = mock(FileSystem.class);
    FSDataInputStream file =
        new FSDataInputStream(new BufferInStream(buffer.getData(),
            buffer.getLength()));
    Path p = new Path("/dir/file.orc");
    when(fs.open(eq(p))).thenReturn(file);
    OrcFile.ReaderOptions options = OrcFile.readerOptions(conf);
    options.filesystem(fs);
    options.maxLength(buffer.getLength());
    when(fs.getFileStatus(eq(p)))
        .thenReturn(new FileStatus(10, false, 3, 3000, 0, p));
    Reader reader = OrcFile.createReader(p, options);
    assertEquals(0, reader.getNumberOfRows());
  }

  static class StubPredicate implements PredicateLeaf {
    final PredicateLeaf.Type type;

    StubPredicate(PredicateLeaf.Type type) {
      this.type = type;
    }

    @Override
    public Operator getOperator() {
      return null;
    }

    @Override
    public Type getType() {
      return type;
    }

    @Override
    public String getColumnName() {
      return null;
    }

    @Override
    public Object getLiteral() {
      return null;
    }

    @Override
    public int getId() { return -1; }

    @Override
    public List<Object> getLiteralList() {
      return null;
    }
  }

  static Location compareToRange(PredicateLeaf.Type type, Comparable point,
                                 Comparable min, Comparable max) {
    PredicateLeaf predicate = new StubPredicate(type);
    return new RecordReaderImpl.ValueRange(predicate, min, max, true)
               .compare(point);
  }

  @Test
  public void testCompareToRangeInt() {
    assertEquals(Location.BEFORE,
        compareToRange(PredicateLeaf.Type.LONG, 19L, 20L, 40L));
    assertEquals(Location.AFTER,
        compareToRange(PredicateLeaf.Type.LONG, 41L, 20L, 40L));
    assertEquals(Location.MIN,
        compareToRange(PredicateLeaf.Type.LONG, 20L, 20L, 40L));
    assertEquals(Location.MIDDLE,
        compareToRange(PredicateLeaf.Type.LONG, 21L, 20L, 40L));
    assertEquals(Location.MAX,
        compareToRange(PredicateLeaf.Type.LONG, 40L, 20L, 40L));
    assertEquals(Location.BEFORE,
        compareToRange(PredicateLeaf.Type.LONG, 0L, 1L, 1L));
    assertEquals(Location.MIN,
        compareToRange(PredicateLeaf.Type.LONG, 1L, 1L, 1L));
    assertEquals(Location.AFTER,
        compareToRange(PredicateLeaf.Type.LONG, 2L, 1L, 1L));
  }

  @Test
  public void testCompareToRangeString() {
    assertEquals(Location.BEFORE,
        compareToRange(PredicateLeaf.Type.STRING, "a", "b", "c"));
    assertEquals(Location.AFTER,
        compareToRange(PredicateLeaf.Type.STRING, "d", "b", "c"));
    assertEquals(Location.MIN,
        compareToRange(PredicateLeaf.Type.STRING, "b", "b", "c"));
    assertEquals(Location.MIDDLE,
        compareToRange(PredicateLeaf.Type.STRING, "bb", "b", "c"));
    assertEquals(Location.MAX,
        compareToRange(PredicateLeaf.Type.STRING, "c", "b", "c"));
    assertEquals(Location.BEFORE,
        compareToRange(PredicateLeaf.Type.STRING, "a", "b", "b"));
    assertEquals(Location.MIN,
        compareToRange(PredicateLeaf.Type.STRING, "b", "b", "b"));
    assertEquals(Location.AFTER,
        compareToRange(PredicateLeaf.Type.STRING, "c", "b", "b"));
  }

  @Test
  public void testCompareToCharNeedConvert() {
    assertEquals(Location.BEFORE,
        compareToRange(PredicateLeaf.Type.STRING, "apple", "hello", "world"));
    assertEquals(Location.AFTER,
        compareToRange(PredicateLeaf.Type.STRING, "zombie", "hello", "world"));
    assertEquals(Location.MIN,
        compareToRange(PredicateLeaf.Type.STRING, "hello", "hello", "world"));
    assertEquals(Location.MIDDLE,
        compareToRange(PredicateLeaf.Type.STRING, "pilot", "hello", "world"));
    assertEquals(Location.MAX,
        compareToRange(PredicateLeaf.Type.STRING, "world", "hello", "world"));
    assertEquals(Location.BEFORE,
        compareToRange(PredicateLeaf.Type.STRING, "apple", "hello", "hello"));
    assertEquals(Location.MIN,
        compareToRange(PredicateLeaf.Type.STRING, "hello", "hello", "hello"));
    assertEquals(Location.AFTER,
        compareToRange(PredicateLeaf.Type.STRING, "zombie", "hello", "hello"));
  }

  @Test
  public void testGetMin() throws Exception {
    assertEquals(10L, RecordReaderImpl.getValueRange(
      ColumnStatisticsImpl.deserialize(null, createIntStats(10L, 100L)),
        new StubPredicate(PredicateLeaf.Type.LONG), true).lower);
    assertEquals(10.0d, RecordReaderImpl.getValueRange(
        ColumnStatisticsImpl.deserialize(null,
           OrcProto.ColumnStatistics.newBuilder()
               .setNumberOfValues(1)
               .setDoubleStatistics(OrcProto.DoubleStatistics.newBuilder()
                 .setMinimum(10.0d).setMaximum(100.0d).build()).build()),
        new StubPredicate(PredicateLeaf.Type.FLOAT), true).lower);
    assertNull(RecordReaderImpl.getValueRange(
        ColumnStatisticsImpl.deserialize(null,
            OrcProto.ColumnStatistics.newBuilder()
                .setNumberOfValues(1)
                .setStringStatistics(OrcProto.StringStatistics.newBuilder().build())
                .build()), new StubPredicate(PredicateLeaf.Type.STRING), true).lower);
    assertEquals("a", RecordReaderImpl.getValueRange(
        ColumnStatisticsImpl.deserialize(null,
          OrcProto.ColumnStatistics.newBuilder()
              .setNumberOfValues(1)
              .setStringStatistics(OrcProto.StringStatistics.newBuilder()
                .setMinimum("a").setMaximum("b").build()).build()),
        new StubPredicate(PredicateLeaf.Type.STRING), true).lower);
    assertEquals("hello", RecordReaderImpl.getValueRange(
        ColumnStatisticsImpl.deserialize(null, createStringStats("hello", "world")),
        new StubPredicate(PredicateLeaf.Type.STRING), true).lower);
    assertEquals(new HiveDecimalWritable("111.1"), RecordReaderImpl.getValueRange(
        ColumnStatisticsImpl.deserialize(null,
            createDecimalStats("111.1", "112.1")),
        new StubPredicate(PredicateLeaf.Type.DECIMAL), true).lower);
  }

  private static OrcProto.ColumnStatistics createIntStats(Long min,
                                                          Long max) {
    OrcProto.ColumnStatistics.Builder result =
        OrcProto.ColumnStatistics.newBuilder();
    OrcProto.IntegerStatistics.Builder intStats =
        OrcProto.IntegerStatistics.newBuilder();
    if (min != null) {
      intStats.setMinimum(min);
      result.setNumberOfValues(1);
    }
    if (max != null) {
      intStats.setMaximum(max);
    }
    return result.setIntStatistics(intStats.build()).build();
  }

  private static OrcProto.ColumnStatistics createBooleanStats(int n, int trueCount) {
    OrcProto.BucketStatistics.Builder boolStats = OrcProto.BucketStatistics.newBuilder();
    boolStats.addCount(trueCount);
    return OrcProto.ColumnStatistics.newBuilder()
               .setNumberOfValues(n).setBucketStatistics(
      boolStats.build()).build();
  }

  private static OrcProto.ColumnStatistics createIntStats(int min, int max) {
    OrcProto.IntegerStatistics.Builder intStats = OrcProto.IntegerStatistics.newBuilder();
    intStats.setMinimum(min);
    intStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder()
        .setNumberOfValues(1)
        .setIntStatistics(intStats.build()).build();
  }

  private static OrcProto.ColumnStatistics createDoubleStats(double min, double max) {
    OrcProto.DoubleStatistics.Builder dblStats = OrcProto.DoubleStatistics.newBuilder();
    dblStats.setMinimum(min);
    dblStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder()
        .setNumberOfValues(1)
        .setDoubleStatistics(dblStats.build()).build();
  }

  //fixme
  private static OrcProto.ColumnStatistics createStringStats(String min, String max,
      boolean hasNull) {
    OrcProto.StringStatistics.Builder strStats = OrcProto.StringStatistics.newBuilder();
    strStats.setMinimum(min);
    strStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder()
        .setNumberOfValues(1)
        .setStringStatistics(strStats.build())
        .setHasNull(hasNull).build();
  }

  private static OrcProto.ColumnStatistics createStringStats(String min, String max) {
    OrcProto.StringStatistics.Builder strStats = OrcProto.StringStatistics.newBuilder();
    strStats.setMinimum(min);
    strStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder()
        .setNumberOfValues(1)
        .setStringStatistics(strStats.build())
               .build();
  }

  private static OrcProto.ColumnStatistics createDateStats(int min, int max) {
    OrcProto.DateStatistics.Builder dateStats = OrcProto.DateStatistics.newBuilder();
    dateStats.setMinimum(min);
    dateStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder().setNumberOfValues(1)
               .setDateStatistics(dateStats.build()).build();
  }

  private static final TimeZone utcTz = TimeZone.getTimeZone("UTC");

  private static OrcProto.ColumnStatistics createTimestampStats(String min, String max) {
    OrcProto.TimestampStatistics.Builder tsStats = OrcProto.TimestampStatistics.newBuilder();
    tsStats.setMinimumUtc(getUtcTimestamp(min));
    tsStats.setMaximumUtc(getUtcTimestamp(max));
    return OrcProto.ColumnStatistics.newBuilder()
        .setNumberOfValues(1)
        .setTimestampStatistics(tsStats.build()).build();
  }

  private static OrcProto.ColumnStatistics createDecimalStats(String min, String max) {
    return createDecimalStats(min, max, true);
  }

  private static OrcProto.ColumnStatistics createDecimalStats(String min, String max,
      boolean hasNull) {
    OrcProto.DecimalStatistics.Builder decStats = OrcProto.DecimalStatistics.newBuilder();
    decStats.setMinimum(min);
    decStats.setMaximum(max);
    return OrcProto.ColumnStatistics.newBuilder()
        .setNumberOfValues(1)
        .setDecimalStatistics(decStats.build())
        .setHasNull(hasNull).build();
  }

  @Test
  public void testGetMax() {
    assertEquals(100L, RecordReaderImpl.getValueRange(
        ColumnStatisticsImpl.deserialize(null, createIntStats(10L, 100L)),
        new StubPredicate(PredicateLeaf.Type.LONG), true).upper);
    assertEquals(100.0d, RecordReaderImpl.getValueRange(
        ColumnStatisticsImpl.deserialize(null,
          OrcProto.ColumnStatistics.newBuilder()
              .setNumberOfValues(1)
              .setDoubleStatistics(OrcProto.DoubleStatistics.newBuilder()
                .setMinimum(10.0d).setMaximum(100.0d).build()).build()),
        new StubPredicate(PredicateLeaf.Type.FLOAT), true).upper);
    assertNull(RecordReaderImpl.getValueRange(
        ColumnStatisticsImpl.deserialize(null,
            OrcProto.ColumnStatistics.newBuilder()
                .setNumberOfValues(1)
                .setStringStatistics(OrcProto.StringStatistics.newBuilder().build())
                .build()), new StubPredicate(PredicateLeaf.Type.STRING), true).upper);
    assertEquals("b", RecordReaderImpl.getValueRange(
        ColumnStatisticsImpl.deserialize(null,
          OrcProto.ColumnStatistics.newBuilder()
              .setNumberOfValues(1)
              .setStringStatistics(OrcProto.StringStatistics.newBuilder()
                .setMinimum("a").setMaximum("b").build()).build()),
        new StubPredicate(PredicateLeaf.Type.STRING), true).upper);
    assertEquals("world", RecordReaderImpl.getValueRange(
        ColumnStatisticsImpl.deserialize(null,
            createStringStats("hello", "world")),
        new StubPredicate(PredicateLeaf.Type.STRING), true).upper);
    assertEquals(new HiveDecimalWritable("112.1"), RecordReaderImpl.getValueRange(
        ColumnStatisticsImpl.deserialize(null,
            createDecimalStats("111.1", "112.1")),
        new StubPredicate(PredicateLeaf.Type.DECIMAL), true).upper);
  }

  static TruthValue evaluateBoolean(OrcProto.ColumnStatistics stats,
                                    PredicateLeaf predicate) {
    OrcProto.ColumnEncoding encoding =
        OrcProto.ColumnEncoding.newBuilder()
            .setKind(OrcProto.ColumnEncoding.Kind.DIRECT)
            .build();
    return RecordReaderImpl.evaluatePredicateProto(stats, predicate, null,
        encoding, null,
        OrcFile.WriterVersion.ORC_135, TypeDescription.createBoolean());
  }

  static TruthValue evaluateInteger(OrcProto.ColumnStatistics stats,
                                    PredicateLeaf predicate) {
    OrcProto.ColumnEncoding encoding =
        OrcProto.ColumnEncoding.newBuilder()
            .setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2)
            .build();
    return RecordReaderImpl.evaluatePredicateProto(stats, predicate, null,
        encoding, null,
        OrcFile.WriterVersion.ORC_135, TypeDescription.createLong());
  }

  static TruthValue evaluateDouble(OrcProto.ColumnStatistics stats,
                                    PredicateLeaf predicate) {
    OrcProto.ColumnEncoding encoding =
        OrcProto.ColumnEncoding.newBuilder()
            .setKind(OrcProto.ColumnEncoding.Kind.DIRECT)
            .build();
    return RecordReaderImpl.evaluatePredicateProto(stats, predicate, null,
        encoding, null,
        OrcFile.WriterVersion.ORC_135, TypeDescription.createDouble());
  }

  static TruthValue evaluateTimestamp(OrcProto.ColumnStatistics stats,
                                      PredicateLeaf predicate,
                                      boolean include135,
                                      boolean useUTCTimestamp) {
    OrcProto.ColumnEncoding encoding =
        OrcProto.ColumnEncoding.newBuilder()
            .setKind(OrcProto.ColumnEncoding.Kind.DIRECT)
            .build();
    return RecordReaderImpl.evaluatePredicateProto(stats, predicate, null,
        encoding, null,
        include135 ? OrcFile.WriterVersion.ORC_135: OrcFile.WriterVersion.ORC_101,
        TypeDescription.createTimestamp(), true, useUTCTimestamp);
  }

  static TruthValue evaluateTimestampWithWriterCalendar(OrcProto.ColumnStatistics stats,
                                                        PredicateLeaf predicate,
                                                        boolean include135,
                                                        boolean writerUsedProlepticGregorian,
                                                        boolean useUTCTimestamp) {
    OrcProto.ColumnEncoding encoding =
        OrcProto.ColumnEncoding.newBuilder()
            .setKind(OrcProto.ColumnEncoding.Kind.DIRECT)
            .build();
    return RecordReaderImpl.evaluatePredicateProto(stats, predicate, null,
        encoding, null,
        include135 ? OrcFile.WriterVersion.ORC_135: OrcFile.WriterVersion.ORC_101,
        TypeDescription.createTimestamp(), writerUsedProlepticGregorian, useUTCTimestamp);
  }

  static TruthValue evaluateTimestampBloomfilter(OrcProto.ColumnStatistics stats,
                                                 PredicateLeaf predicate,
                                                 BloomFilter bloom,
                                                 OrcFile.WriterVersion version,
                                                 boolean useUTCTimestamp) {
    OrcProto.ColumnEncoding.Builder encoding =
        OrcProto.ColumnEncoding.newBuilder()
            .setKind(OrcProto.ColumnEncoding.Kind.DIRECT);
    if (version.includes(OrcFile.WriterVersion.ORC_135)) {
      encoding.setBloomEncoding(BloomFilterIO.Encoding.UTF8_UTC.getId());
    }
    OrcProto.Stream.Kind kind =
        version.includes(OrcFile.WriterVersion.ORC_101) ?
            OrcProto.Stream.Kind.BLOOM_FILTER_UTF8 :
            OrcProto.Stream.Kind.BLOOM_FILTER;
    OrcProto.BloomFilter.Builder builder =
        OrcProto.BloomFilter.newBuilder();
    BloomFilterIO.serialize(builder, bloom);
    return RecordReaderImpl.evaluatePredicateProto(stats, predicate, kind,
        encoding.build(), builder.build(), version,
        TypeDescription.createTimestamp(), true, useUTCTimestamp);
  }

  @Test
  public void testPredEvalWithBooleanStats() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.BOOLEAN, "x", true, null);
    assertEquals(TruthValue.YES_NO,
        evaluateBoolean(createBooleanStats(10, 10), pred));
    assertEquals(TruthValue.NO,
        evaluateBoolean(createBooleanStats(10, 0), pred));

    pred = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.BOOLEAN, "x", true, null);
    assertEquals(TruthValue.YES_NO,
        evaluateBoolean(createBooleanStats(10, 10), pred));
    assertEquals(TruthValue.NO,
        evaluateBoolean(createBooleanStats(10, 0), pred));

    pred = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.BOOLEAN, "x", false, null);
    assertEquals(TruthValue.NO,
      evaluateBoolean(createBooleanStats(10, 10), pred));
    assertEquals(TruthValue.YES_NO,
      evaluateBoolean(createBooleanStats(10, 0), pred));
  }

  @Test
  public void testPredEvalWithIntStats() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.LONG, "x", 15L, null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createIntStats(10, 100), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.FLOAT, "x", 15.0, null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createIntStats(10, 100), pred));

    // Stats gets converted to column type. "15" is outside of "10" and "100"
    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "15", null);
    assertEquals(TruthValue.NO,
        evaluateInteger(createIntStats(10, 100), pred));

    // Integer stats will not be converted date because of days/seconds/millis ambiguity
    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DATE, "x", LocalDate.ofEpochDay(15), null);
    try {
      evaluateInteger(createIntStats(10, 100), pred);
      fail("evaluate should throw");
    } catch (RecordReaderImpl.SargCastException ia) {
      assertEquals("ORC SARGS could not convert from Long to DATE", ia.getMessage());
    }

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DECIMAL, "x", new HiveDecimalWritable("15"), null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createIntStats(10, 100), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(15), null);
    try {
      evaluateInteger(createIntStats(10, 100), pred);
      fail("evaluate should throw");
    } catch (RecordReaderImpl.SargCastException ia) {
      assertEquals("ORC SARGS could not convert from Long to TIMESTAMP", ia.getMessage());
    }
  }

  @Test
  public void testPredEvalWithDoubleStats() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.LONG, "x", 15L, null);
    assertEquals(TruthValue.YES_NO,
        evaluateDouble(createDoubleStats(10.0, 100.0), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.FLOAT, "x", 15.0, null);
    assertEquals(TruthValue.YES_NO,
        evaluateDouble(createDoubleStats(10.0, 100.0), pred));

    // Stats gets converted to column type. "15.0" is outside of "10.0" and "100.0"
    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "15", null);
    assertEquals(TruthValue.NO,
        evaluateDouble(createDoubleStats(10.0, 100.0), pred));

    // Double is not converted to date type because of days/seconds/millis ambiguity
    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DATE, "x", LocalDate.ofEpochDay(15), null);
    try {
      evaluateDouble(createDoubleStats(10.0, 100.0), pred);
      fail("evaluate should throw");
    } catch (RecordReaderImpl.SargCastException ia) {
      assertEquals("ORC SARGS could not convert from Double to DATE", ia.getMessage());
    }

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DECIMAL, "x", new HiveDecimalWritable("15"), null);
    assertEquals(TruthValue.YES_NO,
        evaluateDouble(createDoubleStats(10.0, 100.0), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(15*1000L), null);
    assertEquals(TruthValue.YES_NO,
        evaluateDouble(createDoubleStats(10.0, 100.0), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(150*1000L), null);
    assertEquals(TruthValue.NO,
        evaluateDouble(createDoubleStats(10.0, 100.0), pred));
  }

  @Test
  public void testPredEvalWithStringStats() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.LONG, "x", 100L, null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createStringStats("10", "1000"), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.FLOAT, "x", 100.0, null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createStringStats("10", "1000"), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "100", null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createStringStats("10", "1000"), pred));

    // IllegalArgumentException is thrown when converting String to Date, hence YES_NO
    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DATE, "x", LocalDate.ofEpochDay(100), null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createDateStats(10, 1000), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DECIMAL, "x", new HiveDecimalWritable("100"), null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createStringStats("10", "1000"), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(100), null);
    try {
      evaluateInteger(createStringStats("10", "1000"), pred);
      fail("evaluate should throw");
    } catch (RecordReaderImpl.SargCastException ia) {
      assertEquals("ORC SARGS could not convert from String to TIMESTAMP", ia.getMessage());
    }
  }

  @Test
  public void testPredEvalWithDateStats() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.LONG, "x", 15L, null);
    // Date to Integer conversion is not possible.
    try {
      evaluateInteger(createDateStats(10, 100), pred);
      fail("evaluate should throw");
    } catch (RecordReaderImpl.SargCastException ia) {
      assertEquals("ORC SARGS could not convert from LocalDate to LONG", ia.getMessage());
    }

    // Date to Float conversion is also not possible.
    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.FLOAT, "x", 15.0, null);
    try {
      evaluateInteger(createDateStats(10, 100), pred);
      fail("evaluate should throw");
    } catch (RecordReaderImpl.SargCastException ia) {
      assertEquals("ORC SARGS could not convert from LocalDate to FLOAT", ia.getMessage());
    }

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "15", null);
    assertEquals(TruthValue.NO,
        evaluateInteger(createDateStats(10, 100), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "1970-01-11", null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createDateStats(10, 100), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "15.1", null);
    assertEquals(TruthValue.NO,
        evaluateInteger(createDateStats(10, 100), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "__a15__1", null);
    assertEquals(TruthValue.NO,
        evaluateInteger(createDateStats(10, 100), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "2000-01-16", null);
    assertEquals(TruthValue.NO,
        evaluateInteger(createDateStats(10, 100), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "1970-01-16", null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createDateStats(10, 100), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DATE, "x", LocalDate.ofEpochDay(15), null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createDateStats(10, 100), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DATE, "x", LocalDate.ofEpochDay(150), null);
    assertEquals(TruthValue.NO,
        evaluateInteger(createDateStats(10, 100), pred));

    // Date to Decimal conversion is also not possible.
    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DECIMAL, "x", new HiveDecimalWritable("15"), null);
    try {
      evaluateInteger(createDateStats(10, 100), pred);
      fail("evaluate should throw");
    } catch (RecordReaderImpl.SargCastException ia) {
      assertEquals("ORC SARGS could not convert from LocalDate to DECIMAL", ia.getMessage());
    }

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(15), null);
    assertEquals(TruthValue.NO,
        evaluateInteger(createDateStats(10, 100), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(15L * 24L * 60L * 60L * 1000L), null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createDateStats(10, 100), pred));
  }

  @Test
  public void testPredEvalWithDecimalStats() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.LONG, "x", 15L, null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createDecimalStats("10.0", "100.0"), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.FLOAT, "x", 15.0, null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createDecimalStats("10.0", "100.0"), pred));

    // "15" out of range of "10.0" and "100.0"
    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", "15", null);
    assertEquals(TruthValue.NO,
        evaluateInteger(createDecimalStats("10.0", "100.0"), pred));

    // Decimal to Date not possible.
    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DATE, "x", LocalDate.ofEpochDay(15), null);
    try {
      evaluateInteger(createDecimalStats("10.0", "100.0"), pred);
      fail("evaluate should throw");
    } catch (RecordReaderImpl.SargCastException ia) {
      assertEquals("ORC SARGS could not convert from HiveDecimal to DATE", ia.getMessage());
    }

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DECIMAL, "x", new HiveDecimalWritable("15"), null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createDecimalStats("10.0", "100.0"), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(15 * 1000L), null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createDecimalStats("10.0", "100.0"), pred));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(150 * 1000L), null);
    assertEquals(TruthValue.NO,
        evaluateInteger(createDecimalStats("10.0", "100.0"), pred));
  }

  @Test
  public void testPredEvalTimestampStatsDiffWriter() {
    // Proleptic - NoUTC
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.TIMESTAMP, "x",
        Timestamp.valueOf("1017-01-01 00:00:00"), null);
    assertEquals(TruthValue.YES_NO,
        evaluateTimestampWithWriterCalendar(createTimestampStats("1017-01-01 00:00:00", "1017-01-01 00:00:00"),
            pred, true, true, false));

    // NoProleptic - NoUTC -> 1016-12-26 00:00:00.0
    long predTime = DateUtils.convertTimeToProleptic(Timestamp.valueOf("1017-01-01 00:00:00").getTime(), false);
    PredicateLeaf pred2 = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(predTime), null);
    assertEquals(TruthValue.YES_NO,
        evaluateTimestampWithWriterCalendar(createTimestampStats("1017-01-01 00:00:00", "1017-01-01 00:00:00"),
            pred2, true, false, false));

    // NoProleptic - UTC -> 1016-12-25 16:00:00.0
    predTime = DateUtils.convertTimeToProleptic(getUtcTimestamp("1017-01-01 00:00:00"), true);
    PredicateLeaf pred3 = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(predTime), null);
    assertEquals(TruthValue.YES_NO,
        evaluateTimestampWithWriterCalendar(createTimestampStats("1017-01-01 00:00:00", "1017-01-01 00:00:00"),
            pred3, true, false, true));

    // Proleptic - UTC -> 1016-12-31 16:00:00.0
    predTime = getUtcTimestamp("1017-01-01 00:00:00");
    PredicateLeaf pred4 = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x", new Timestamp(predTime), null);
    assertEquals(TruthValue.YES_NO,
        evaluateTimestampWithWriterCalendar(createTimestampStats("1017-01-01 00:00:00", "1017-01-01 00:00:00"),
            pred4, true, true, true));
  }

  @Test
  public void testPredEvalWithTimestampStats() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.TIMESTAMP,
        "x", Timestamp.valueOf("2017-01-01 00:00:00"), null);
    assertEquals(TruthValue.YES_NO,
        evaluateTimestamp(createTimestampStats("2017-01-01 00:00:00",
            "2018-01-01 00:00:00"), pred, true, false));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.FLOAT, "x", 15.0, null);
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateTimestamp(createTimestampStats("2017-01-01 00:00:00", "2018-01-01 00:00:00"),
            pred, true, false));
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateTimestamp(createTimestampStats("2017-01-01 00:00:00", "2018-01-01 00:00:00"),
            pred, true, false));

    // pre orc-135 should always be yes_no_null.
    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.TIMESTAMP, "x",  Timestamp.valueOf("2017-01-01 00:00:00"), null);
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateTimestamp(createTimestampStats("2017-01-01 00:00:00", "2017-01-01 00:00:00"),
            pred, false, false));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.STRING, "x", Timestamp.valueOf("2017-01-01 00:00:00").toString(), null);
    assertEquals(TruthValue.YES_NO,
        evaluateTimestamp(createTimestampStats("2017-01-01 00:00:00", "2018-01-01 00:00:00"),
            pred, true, false));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DATE, "x", Date.valueOf("2016-01-01"), null);
    assertEquals(TruthValue.NO,
        evaluateTimestamp(createTimestampStats("2017-01-01 00:00:00", "2017-01-01 00:00:00"),
            pred, true, false));
    assertEquals(TruthValue.YES_NO,
        evaluateTimestamp(createTimestampStats("2015-01-01 00:00:00", "2016-01-01 00:00:00"),
            pred, true, false));

    pred = createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS,
        PredicateLeaf.Type.DECIMAL, "x", new HiveDecimalWritable("15"), null);
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateTimestamp(createTimestampStats("2015-01-01 00:00:00", "2016-01-01 00:00:00"),
            pred, true, false));
  }

  @Test
  public void testEquals() throws Exception {
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.LONG,
            "x", 15L, null);
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createIntStats(20L, 30L), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createIntStats(15L, 30L), pred)) ;
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createIntStats(10L, 30L), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createIntStats(10L, 15L), pred));
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createIntStats(0L, 10L), pred));
    assertEquals(TruthValue.YES_NULL,
        evaluateInteger(createIntStats(15L, 15L), pred));
  }

  @Test
  public void testNullSafeEquals() throws Exception {
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.LONG,
            "x", 15L, null);
    assertEquals(TruthValue.NO,
        evaluateInteger(createIntStats(20L, 30L), pred));
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createIntStats(15L, 30L), pred));
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createIntStats(10L, 30L), pred));
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createIntStats(10L, 15L), pred));
    assertEquals(TruthValue.NO,
        evaluateInteger(createIntStats(0L, 10L), pred));
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createIntStats(15L, 15L), pred));
  }

  @Test
  public void testLessThan() throws Exception {
    PredicateLeaf lessThan = createPredicateLeaf
        (PredicateLeaf.Operator.LESS_THAN, PredicateLeaf.Type.LONG,
            "x", 15L, null);
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createIntStats(20L, 30L), lessThan));
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createIntStats(15L, 30L), lessThan));
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createIntStats(10L, 30L), lessThan));
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createIntStats(10L, 15L), lessThan));
    assertEquals(TruthValue.YES_NULL,
        evaluateInteger(createIntStats(0L, 10L), lessThan));
  }

  @Test
  public void testLessThanEquals() throws Exception {
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.LESS_THAN_EQUALS, PredicateLeaf.Type.LONG,
            "x", 15L, null);
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createIntStats(20L, 30L), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createIntStats(15L, 30L), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createIntStats(10L, 30L), pred));
    assertEquals(TruthValue.YES_NULL,
        evaluateInteger(createIntStats(10L, 15L), pred));
    assertEquals(TruthValue.YES_NULL,
        evaluateInteger(createIntStats(0L, 10L), pred));
  }

  @Test
  public void testIn() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add(10L);
    args.add(20L);
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.LONG,
            "x", null, args);
    assertEquals(TruthValue.YES_NULL,
        evaluateInteger(createIntStats(20L, 20L), pred));
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createIntStats(30L, 30L), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createIntStats(10L, 30L), pred));
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createIntStats(12L, 18L), pred));
  }

  @Test
  public void testInDatePredConversion() {
    List<Object> args = new ArrayList<>();
    args.add(toDate(LocalDate.ofEpochDay(15)));
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.DATE,
            "x", null, args);
    assertEquals(TruthValue.YES_NULL,
        evaluateInteger(createDateStats(15, 15), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createDateStats(10, 30), pred));
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createDateStats(5, 10), pred));
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createDateStats(16, 30), pred));
  }

  @Test
  public void testBetween() {
    List<Object> args = new ArrayList<Object>();
    args.add(10L);
    args.add(20L);
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.BETWEEN, PredicateLeaf.Type.LONG,
            "x", null, args);
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createIntStats(0L, 5L), pred));
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createIntStats(30L, 40L), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createIntStats(5L, 15L), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createIntStats(15L, 25L), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createIntStats(5L, 25L), pred));
    assertEquals(TruthValue.YES_NULL,
        evaluateInteger(createIntStats(10L, 20L), pred));
    assertEquals(TruthValue.YES_NULL,
        evaluateInteger(createIntStats(12L, 18L), pred));

    // check with empty predicate list
    args.clear();
    pred = createPredicateLeaf
        (PredicateLeaf.Operator.BETWEEN, PredicateLeaf.Type.LONG,
            "x", null, args);
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createIntStats(0L, 5L), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createIntStats(30L, 40L), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createIntStats(5L, 15L), pred));
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createIntStats(10L, 20L), pred));
  }

  @Test
  public void testIsNull() {
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.IS_NULL, PredicateLeaf.Type.LONG,
            "x", null, null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createIntStats(20L, 30L), pred));
  }


  @Test
  public void testEqualsWithNullInStats() {
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.STRING,
            "x", "c", null);
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createStringStats("d", "e", true), pred)); // before
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createStringStats("a", "b", true), pred)); // after
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createStringStats("b", "c", true), pred)); // max
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createStringStats("c", "d", true), pred)); // min
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createStringStats("b", "d", true), pred)); // middle
    assertEquals(TruthValue.YES_NULL,
        evaluateInteger(createStringStats("c", "c", true), pred)); // same
  }

  @Test
  public void testNullSafeEqualsWithNullInStats() {
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.STRING,
            "x", "c", null);
    assertEquals(TruthValue.NO,
        evaluateInteger(createStringStats("d", "e", true), pred)); // before
    assertEquals(TruthValue.NO,
        evaluateInteger(createStringStats("a", "b", true), pred)); // after
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createStringStats("b", "c", true), pred)); // max
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createStringStats("c", "d", true), pred)); // min
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createStringStats("b", "d", true), pred)); // middle
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createStringStats("c", "c", true), pred)); // same
  }

  @Test
  public void testLessThanWithNullInStats() {
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.LESS_THAN, PredicateLeaf.Type.STRING,
            "x", "c", null);
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createStringStats("d", "e", true), pred)); // before
    assertEquals(TruthValue.YES_NULL,
        evaluateInteger(createStringStats("a", "b", true), pred)); // after
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createStringStats("b", "c", true), pred)); // max
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createStringStats("c", "d", true), pred)); // min
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createStringStats("b", "d", true), pred)); // middle
    assertEquals(TruthValue.NO_NULL, // min, same stats
        evaluateInteger(createStringStats("c", "c", true), pred));
  }

  @Test
  public void testLessThanEqualsWithNullInStats() throws Exception {
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.LESS_THAN_EQUALS, PredicateLeaf.Type.STRING,
            "x", "c", null);
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createStringStats("d", "e", true), pred)); // before
    assertEquals(TruthValue.YES_NULL,
        evaluateInteger(createStringStats("a", "b", true), pred)); // after
    assertEquals(TruthValue.YES_NULL,
        evaluateInteger(createStringStats("b", "c", true), pred)); // max
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createStringStats("c", "d", true), pred)); // min
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createStringStats("b", "d", true), pred)); // middle
    assertEquals(TruthValue.YES_NULL,
        evaluateInteger(createStringStats("c", "c", true), pred)); // same
  }

  @Test
  public void testInWithNullInStats() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add("c");
    args.add("f");
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.STRING,
            "x", null, args);
    assertEquals(TruthValue.NO_NULL, // before & after
        evaluateInteger(createStringStats("d", "e", true), pred));
    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createStringStats("a", "b", true), pred)); // after
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createStringStats("e", "f", true), pred)); // max
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createStringStats("c", "d", true), pred)); // min
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createStringStats("b", "d", true), pred)); // middle
    assertEquals(TruthValue.YES_NULL,
        evaluateInteger(createStringStats("c", "c", true), pred)); // same
  }

  @Test
  public void testBetweenWithNullInStats() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add("c");
    args.add("f");
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.BETWEEN, PredicateLeaf.Type.STRING,
            "x", null, args);
    assertEquals(TruthValue.YES_NULL, // before & after
        evaluateInteger(createStringStats("d", "e", true), pred));
    assertEquals(TruthValue.YES_NULL, // before & max
        evaluateInteger(createStringStats("e", "f", true), pred));
    assertEquals(TruthValue.NO_NULL, // before & before
        evaluateInteger(createStringStats("h", "g", true), pred));
    assertEquals(TruthValue.YES_NO_NULL, // before & min
        evaluateInteger(createStringStats("f", "g", true), pred));
    assertEquals(TruthValue.YES_NO_NULL, // before & middle
        evaluateInteger(createStringStats("e", "g", true), pred));

    assertEquals(TruthValue.YES_NULL, // min & after
        evaluateInteger(createStringStats("c", "e", true), pred));
    assertEquals(TruthValue.YES_NULL, // min & max
        evaluateInteger(createStringStats("c", "f", true), pred));
    assertEquals(TruthValue.YES_NO_NULL, // min & middle
        evaluateInteger(createStringStats("c", "g", true), pred));

    assertEquals(TruthValue.NO_NULL,
        evaluateInteger(createStringStats("a", "b", true), pred)); // after
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createStringStats("a", "c", true), pred)); // max
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateInteger(createStringStats("b", "d", true), pred)); // middle
    assertEquals(TruthValue.YES_NULL, // min & after, same stats
        evaluateInteger(createStringStats("c", "c", true), pred));
  }

  @Test
  public void testTimestampStatsOldFiles() throws Exception {
    PredicateLeaf pred = createPredicateLeaf
      (PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.TIMESTAMP,
        "x", Timestamp.valueOf("2000-01-01 00:00:00"), null);
    OrcProto.ColumnStatistics cs = createTimestampStats("2000-01-01 00:00:00", "2001-01-01 00:00:00");
    assertEquals(TruthValue.YES_NO_NULL,
      evaluateTimestampBloomfilter(cs, pred, new BloomFilterUtf8(10000, 0.01), OrcFile.WriterVersion.ORC_101, false));
    BloomFilterUtf8 bf = new BloomFilterUtf8(10, 0.05);
    bf.addLong(getUtcTimestamp("2000-06-01 00:00:00"));
    assertEquals(TruthValue.NO_NULL,
      evaluateTimestampBloomfilter(cs, pred, bf, OrcFile.WriterVersion.ORC_135, false));
    assertEquals(TruthValue.YES_NO_NULL,
      evaluateTimestampBloomfilter(cs, pred, bf, OrcFile.WriterVersion.ORC_101, false));
  }

  @Test
  public void testTimestampUTC() throws Exception {
    DateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    f.setTimeZone(TimeZone.getTimeZone("UTC"));
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.TIMESTAMP,
            "x", new Timestamp(f.parse("2015-01-01 00:00:00").getTime()), null);
    PredicateLeaf pred2 = createPredicateLeaf
        (PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.TIMESTAMP,
            "x", new Timestamp(f.parse("2014-12-31 23:59:59").getTime()), null);
    PredicateLeaf pred3 = createPredicateLeaf
        (PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.TIMESTAMP,
            "x", new Timestamp(f.parse("2016-01-01 00:00:01").getTime()), null);
    OrcProto.ColumnStatistics cs = createTimestampStats("2015-01-01 00:00:00", "2016-01-01 00:00:00");

    assertEquals(TruthValue.YES_NO_NULL,
        evaluateTimestamp(cs, pred, true, true));
    assertEquals(TruthValue.NO_NULL,
        evaluateTimestamp(cs, pred2, true, true));
    assertEquals(TruthValue.NO_NULL,
        evaluateTimestamp(cs, pred3, true, true));

    assertEquals(TruthValue.NO_NULL,
        evaluateTimestampBloomfilter(cs, pred, new BloomFilterUtf8(10000, 0.01), OrcFile.WriterVersion.ORC_135, true));
    assertEquals(TruthValue.NO_NULL,
        evaluateTimestampBloomfilter(cs, pred2, new BloomFilterUtf8(10000, 0.01), OrcFile.WriterVersion.ORC_135, true));

    BloomFilterUtf8 bf = new BloomFilterUtf8(10, 0.05);
    bf.addLong(getUtcTimestamp("2015-06-01 00:00:00"));
    assertEquals(TruthValue.NO_NULL,
        evaluateTimestampBloomfilter(cs, pred, bf, OrcFile.WriterVersion.ORC_135, true));

    bf.addLong(getUtcTimestamp("2015-01-01 00:00:00"));
    assertEquals(TruthValue.YES_NO_NULL,
        evaluateTimestampBloomfilter(cs, pred, bf, OrcFile.WriterVersion.ORC_135, true));
  }

  private static long getUtcTimestamp(String ts)  {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    dateFormat.setTimeZone(utcTz);
    try {
      return dateFormat.parse(ts).getTime();
    } catch (ParseException e) {
      throw new IllegalArgumentException("Can't parse " + ts, e);
    }
  }

  @Test
  public void testIsNullWithNullInStats() throws Exception {
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.IS_NULL, PredicateLeaf.Type.STRING,
            "x", null, null);
    assertEquals(TruthValue.YES_NO,
        evaluateInteger(createStringStats("c", "d", true), pred));
    assertEquals(TruthValue.NO,
        evaluateInteger(createStringStats("c", "d", false), pred));
  }

  @Test
  public void testOverlap() throws Exception {
    assertFalse(RecordReaderUtils.overlap(0, 10, -10, -1));
    assertTrue(RecordReaderUtils.overlap(0, 10, -1, 0));
    assertTrue(RecordReaderUtils.overlap(0, 10, -1, 1));
    assertTrue(RecordReaderUtils.overlap(0, 10, 2, 8));
    assertTrue(RecordReaderUtils.overlap(0, 10, 5, 10));
    assertTrue(RecordReaderUtils.overlap(0, 10, 10, 11));
    assertTrue(RecordReaderUtils.overlap(0, 10, 0, 10));
    assertTrue(RecordReaderUtils.overlap(0, 10, -1, 11));
    assertFalse(RecordReaderUtils.overlap(0, 10, 11, 12));
  }

  private static DiskRangeList diskRanges(Integer... points) {
    DiskRangeList head = null, tail = null;
    for(int i = 0; i < points.length; i += 2) {
      DiskRangeList range = new DiskRangeList(points[i], points[i+1]);
      if (tail == null) {
        head = tail = range;
      } else {
        tail = tail.insertAfter(range);
      }
    }
    return head;
  }

  @Test
  public void testGetIndexPosition() throws Exception {
    boolean uncompressed = false;
    boolean compressed = true;
    assertEquals(0, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, TypeDescription.Category.INT,
            OrcProto.Stream.Kind.PRESENT, compressed, true));
    assertEquals(4, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, TypeDescription.Category.INT,
            OrcProto.Stream.Kind.DATA, compressed, true));
    assertEquals(3, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, TypeDescription.Category.INT,
            OrcProto.Stream.Kind.DATA, uncompressed, true));
    assertEquals(0, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, TypeDescription.Category.INT,
            OrcProto.Stream.Kind.DATA, compressed, false));
    assertEquals(4, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DICTIONARY, TypeDescription.Category.STRING,
            OrcProto.Stream.Kind.DATA, compressed, true));
    assertEquals(4, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, TypeDescription.Category.BINARY,
            OrcProto.Stream.Kind.DATA, compressed, true));
    assertEquals(3, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, TypeDescription.Category.BINARY,
            OrcProto.Stream.Kind.DATA, uncompressed, true));
    assertEquals(6, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, TypeDescription.Category.BINARY,
            OrcProto.Stream.Kind.LENGTH, compressed, true));
    assertEquals(4, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, TypeDescription.Category.BINARY,
            OrcProto.Stream.Kind.LENGTH, uncompressed, true));
    assertEquals(4, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, TypeDescription.Category.DECIMAL,
            OrcProto.Stream.Kind.DATA, compressed, true));
    assertEquals(3, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, TypeDescription.Category.DECIMAL,
            OrcProto.Stream.Kind.DATA, uncompressed, true));
    assertEquals(6, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, TypeDescription.Category.DECIMAL,
            OrcProto.Stream.Kind.SECONDARY, compressed, true));
    assertEquals(4, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, TypeDescription.Category.DECIMAL,
            OrcProto.Stream.Kind.SECONDARY, uncompressed, true));
    assertEquals(4, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, TypeDescription.Category.TIMESTAMP,
            OrcProto.Stream.Kind.DATA, compressed, true));
    assertEquals(3, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, TypeDescription.Category.TIMESTAMP,
            OrcProto.Stream.Kind.DATA, uncompressed, true));
    assertEquals(7, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, TypeDescription.Category.TIMESTAMP,
            OrcProto.Stream.Kind.SECONDARY, compressed, true));
    assertEquals(5, RecordReaderUtils.getIndexPosition
        (OrcProto.ColumnEncoding.Kind.DIRECT, TypeDescription.Category.TIMESTAMP,
            OrcProto.Stream.Kind.SECONDARY, uncompressed, true));
  }

  @Test
  public void testPartialPlan() throws Exception {
    TypeDescription schema = TypeDescription.fromString("struct<x:int,y:int>");
    MockDataReader dataReader = new MockDataReader(schema)
        .addStream(1, OrcProto.Stream.Kind.ROW_INDEX,
                   createRowIndex(entry(0, -1, -1, 0),
                                  entry(100, -1, -1, 10000),
                                  entry(200, -1, -1, 20000),
                                  entry(300, -1, -1, 30000),
                                  entry(400, -1, -1, 40000),
                                  entry(500, -1, -1, 50000)))
        .addStream(2, OrcProto.Stream.Kind.ROW_INDEX,
                   createRowIndex(entry(0, -1, -1, 0),
                                  entry(200, -1, -1, 20000),
                                  entry(400, -1, -1, 40000),
                                  entry(600, -1, -1, 60000),
                                  entry(800, -1, -1, 80000),
                                  entry(1000, -1, -1, 100000)))
        .addStream(1, OrcProto.Stream.Kind.PRESENT, createDataStream(1, 1000))
        .addStream(1, OrcProto.Stream.Kind.DATA, createDataStream(2, 99000))
        .addStream(2, OrcProto.Stream.Kind.PRESENT, createDataStream(3, 2000))
        .addStream(2, OrcProto.Stream.Kind.DATA, createDataStream(4, 198000))
        .addEncoding(OrcProto.ColumnEncoding.Kind.DIRECT)
        .addEncoding(OrcProto.ColumnEncoding.Kind.DIRECT)
        .addEncoding(OrcProto.ColumnEncoding.Kind.DIRECT)
        .addStripeFooter(1000, null);
    MockStripe stripe = dataReader.getStripe(0);
    // get the start of the data streams
    final long START = stripe.getStream(1, OrcProto.Stream.Kind.PRESENT).offset;

    boolean[] columns = new boolean[]{true, true, false};
    boolean[] rowGroups = new boolean[]{true, true, false, false, true, false};

    // filter by rows and groups
    StripePlanner planner = new StripePlanner(schema, new ReaderEncryption(),
        dataReader, OrcFile.WriterVersion.ORC_14, false, Integer.MAX_VALUE);
    planner.parseStripe(stripe, columns);
    OrcIndex index = planner.readRowIndex(null, null);
    BufferChunkList result = planner.readData(index, rowGroups, false, TypeReader.ReadPhase.ALL);

    assertEquals(START, result.get(0).getOffset());
    assertEquals(1000, result.get(0).getLength());
    assertEquals(START + 1000, result.get(1).getOffset());
    assertEquals(20000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP, result.get(1).getLength());
    assertEquals(START + 41000, result.get(2).getOffset());
    assertEquals(10000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP, result.get(2).getLength());
    assertNull(result.get(3));

    // if we read no rows, don't read any bytes
    rowGroups = new boolean[]{false, false, false, false, false, false};
    result = planner.readData(index, rowGroups, false, TypeReader.ReadPhase.ALL);
    assertNull(result.get(0));

    // all rows, but only columns 0 and 2.
    rowGroups = null;
    columns = new boolean[]{true, false, true};
    planner.parseStripe(stripe, columns).readRowIndex(null, index);
    result = planner.readData(index, rowGroups, false, TypeReader.ReadPhase.ALL);
    assertEquals(START + 100000, result.get(0).getOffset());
    assertEquals(2000, result.get(0).getLength());
    assertEquals(START + 102000, result.get(1).getOffset());
    assertEquals(198000, result.get(1).getLength());
    assertNull(result.get(2));

    rowGroups = new boolean[]{false, true, false, false, false, false};
    result = planner.readData(index, rowGroups, false, TypeReader.ReadPhase.ALL);
    assertEquals(START + 100200, result.get(0).getOffset());
    assertEquals(1800, result.get(0).getLength());
    assertEquals(START + 122000, result.get(1).getOffset());
    assertEquals(20000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP,
        result.get(1).getLength());
    assertNull(result.get(2));

    rowGroups = new boolean[]{false, false, false, false, false, true};
    columns = new boolean[]{true, true, true};
    planner.parseStripe(stripe, columns).readRowIndex(null, index);
    result = planner.readData(index, rowGroups, false, TypeReader.ReadPhase.ALL);
    assertEquals(START + 500, result.get(0).getOffset());
    assertEquals(500, result.get(0).getLength());
    assertEquals(START + 51000, result.get(1).getOffset());
    assertEquals(49000, result.get(1).getLength());
    assertEquals(START + 101000, result.get(2).getOffset());
    assertEquals(1000, result.get(2).getLength());
    assertEquals(START + 202000, result.get(3).getOffset());
    assertEquals(98000, result.get(3).getLength());
    assertNull(result.get(4));
  }


  @Test
  public void testPartialPlanCompressed() throws Exception {
    TypeDescription schema = TypeDescription.fromString("struct<x:int,y:int>");
    InStream.StreamOptions options =
        new InStream.StreamOptions()
            .withCodec(OrcCodecPool.getCodec(CompressionKind.ZLIB))
            .withBufferSize(1024);
    int stretchFactor = 2 + (MAX_VALUES_LENGTH * MAX_BYTE_WIDTH - 1) / options.getBufferSize();
    final int SLOP = stretchFactor * (OutStream.HEADER_SIZE + options.getBufferSize());
    MockDataReader dataReader = new MockDataReader(schema, options)
      .addStream(1, OrcProto.Stream.Kind.ROW_INDEX,
          createRowIndex(options,
              entry(0,   -1, -1, -1, 0),
              entry(100, -1, -1, -1, 10000),
              entry(200, -1, -1, -1, 20000),
              entry(300, -1, -1, -1, 30000),
              entry(400, -1, -1, -1, 40000),
              entry(500, -1, -1, -1, 50000)))
      .addStream(2, OrcProto.Stream.Kind.ROW_INDEX,
          createRowIndex(options,
              entry(0,   -1, -1, -1, 0),
              entry(200, -1, -1, -1, 20000),
              entry(400, -1, -1, -1, 40000),
              entry(600, -1, -1, -1, 60000),
              entry(800, -1, -1, -1, 80000),
              entry(1000, -1, -1, -1, 100000)))
      .addStream(1, OrcProto.Stream.Kind.PRESENT, createDataStream(1, 1000))
      .addStream(1, OrcProto.Stream.Kind.DATA, createDataStream(2, 99000))
      .addStream(2, OrcProto.Stream.Kind.PRESENT, createDataStream(3, 2000))
      .addStream(2, OrcProto.Stream.Kind.DATA, createDataStream(4, 198000))
      .addEncoding(OrcProto.ColumnEncoding.Kind.DIRECT)
      .addEncoding(OrcProto.ColumnEncoding.Kind.DIRECT)
      .addEncoding(OrcProto.ColumnEncoding.Kind.DIRECT)
      .addStripeFooter(1000, null);
    MockStripe stripe = dataReader.getStripe(0);
    // get the start of the data streams
    final long START = stripe.getStream(1, OrcProto.Stream.Kind.PRESENT).offset;

    StripePlanner planner = new StripePlanner(schema, new ReaderEncryption(),
        dataReader, OrcFile.WriterVersion.ORC_14, false, Integer.MAX_VALUE);

    // filter by rows and groups
    boolean[] columns = new boolean[]{true, true, false};
    boolean[] rowGroups = new boolean[]{true, true, false, false, true, false};
    planner.parseStripe(stripe, columns);
    OrcIndex index = planner.readRowIndex(null, null);
    BufferChunkList result = planner.readData(index, rowGroups, false, TypeReader.ReadPhase.ALL);
    assertEquals(START, result.get(0).getOffset());
    assertEquals(1000, result.get(0).getLength());
    assertEquals(START + 1000, result.get(1).getOffset());
    assertEquals(20000 + SLOP, result.get(1).getLength());
    assertEquals(START + 41000, result.get(2).getOffset());
    assertEquals(10000 + SLOP, result.get(2).getLength());
    assertNull(result.get(3));

    rowGroups = new boolean[]{false, false, false, false, false, true};
    result = planner.readData(index, rowGroups, false, TypeReader.ReadPhase.ALL);
    assertEquals(START + 500, result.get(0).getOffset());
    assertEquals(500, result.get(0).getLength());
    assertEquals(START + 51000, result.get(1).getOffset());
    assertEquals(49000, result.get(1).getLength());
    assertNull(result.get(2));
  }

  @Test
  public void testPartialPlanString() throws Exception {
    TypeDescription schema = TypeDescription.fromString("struct<x:string,y:int>");
    MockDataReader dataReader =
        new MockDataReader(schema)
            .addStream(1, OrcProto.Stream.Kind.ROW_INDEX,
                createRowIndex(entry(0, -1, -1, 0),
                    entry(100, -1, -1, 10000),
                    entry(200, -1, -1, 20000),
                    entry(300, -1, -1, 30000),
                    entry(400, -1, -1, 40000),
                    entry(500, -1, -1, 50000)))
            .addStream(2, OrcProto.Stream.Kind.ROW_INDEX,
                createRowIndex(entry(0, -1, -1, 0),
                    entry(200, -1, -1, 20000),
                    entry(400, -1, -1, 40000),
                    entry(600, -1, -1, 60000),
                    entry(800, -1, -1, 80000),
                    entry(1000, -1, -1, 100000)))
            .addStream(1, OrcProto.Stream.Kind.PRESENT, createDataStream(1, 1000))
            .addStream(1, OrcProto.Stream.Kind.DATA, createDataStream(2, 94000))
            .addStream(1, OrcProto.Stream.Kind.LENGTH, createDataStream(3, 2000))
            .addStream(1, OrcProto.Stream.Kind.DICTIONARY_DATA, createDataStream(4, 3000))
            .addStream(2, OrcProto.Stream.Kind.PRESENT, createDataStream(5, 2000))
            .addStream(2, OrcProto.Stream.Kind.DATA, createDataStream(6, 198000))
            .addEncoding(OrcProto.ColumnEncoding.Kind.DIRECT)
            .addEncoding(OrcProto.ColumnEncoding.Kind.DICTIONARY)
            .addEncoding(OrcProto.ColumnEncoding.Kind.DIRECT)
            .addStripeFooter(1000, null);
    MockStripe stripe = dataReader.getStripe(0);
    // get the start of the data streams
    final long START = stripe.getStream(1, OrcProto.Stream.Kind.PRESENT).offset;

    // filter by rows and groups
    StripePlanner planner = new StripePlanner(schema, new ReaderEncryption(),
        dataReader, OrcFile.WriterVersion.ORC_14, false, Integer.MAX_VALUE);

    // filter by rows and groups
    boolean[] columns = new boolean[]{true, true, false};
    boolean[] rowGroups = new boolean[]{false, true, false, false, true, true};
    planner.parseStripe(stripe, columns);
    OrcIndex index = planner.readRowIndex(null, null);
    BufferChunkList result = planner.readData(index, rowGroups, false, TypeReader.ReadPhase.ALL);

    assertEquals(START + 100, result.get(0).getOffset());
    assertEquals(900, result.get(0).getLength());
    assertEquals(START + 11000, result.get(1).getOffset());
    assertEquals(10000 + RecordReaderUtils.WORST_UNCOMPRESSED_SLOP,
        result.get(1).getLength());
    assertEquals(START + 41000, result.get(2).getOffset());
    assertEquals(54000, result.get(2).getLength());
    assertEquals(START + 95000, result.get(3).getOffset());
    assertEquals(2000, result.get(3).getLength());
    assertEquals(START + 97000, result.get(4).getOffset());
    assertEquals(3000, result.get(4).getLength());
    assertNull(result.get(5));

    // Don't read anything if no groups are selected
    rowGroups = new boolean[6];
    result = planner.readData(index, rowGroups, false, TypeReader.ReadPhase.ALL);
    assertNull(result.get(0));
  }

  @Test
  public void testIntNullSafeEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.LONG, "x", 15L, null);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addLong(i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createIntStats(10, 100));
    assertEquals(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong(15);
    assertEquals(TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testIntEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.LONG, "x", 15L, null);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addLong(i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createIntStats(10, 100));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong(15);
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testIntInBloomFilter() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add(15L);
    args.add(19L);
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.LONG,
            "x", null, args);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addLong(i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createIntStats(10, 100));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong(19);
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong(15);
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testDoubleNullSafeEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.FLOAT, "x", 15.0, null);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addDouble(i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createDoubleStats(10.0, 100.0));
    assertEquals(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addDouble(15.0);
    assertEquals(TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testDoubleEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.FLOAT, "x", 15.0, null);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addDouble(i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createDoubleStats(10.0, 100.0));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addDouble(15.0);
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testDoubleInBloomFilter() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add(15.0);
    args.add(19.0);
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.FLOAT,
            "x", null, args);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addDouble(i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createDoubleStats(10.0, 100.0));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addDouble(19.0);
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addDouble(15.0);
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testStringNullSafeEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.STRING, "x", "str_15", null);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addString("str_" + i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createStringStats("str_10", "str_200"));
    assertEquals(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString("str_15");
    assertEquals(TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testStringEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.STRING, "x", "str_15", null);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addString("str_" + i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createStringStats("str_10", "str_200"));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString("str_15");
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testStringInBloomFilter() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add("str_15");
    args.add("str_19");
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.STRING,
            "x", null, args);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addString("str_" + i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createStringStats("str_10", "str_200"));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString("str_19");
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString("str_15");
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testDateWritableNullSafeEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.DATE, "x",
        LocalDate.ofEpochDay(15), null);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addLong(i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createDateStats(10, 100));
    assertEquals(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong(15);
    assertEquals(TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testDateWritableEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.DATE, "x",
        LocalDate.ofEpochDay(15), null);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addLong(i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createDateStats(10, 100));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong(15);
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testDateWritableInBloomFilter() throws Exception {
    List<Object> args = new ArrayList<>();
    args.add(toDate(LocalDate.ofEpochDay(15)));
    args.add(toDate(LocalDate.ofEpochDay(19)));
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.DATE,
            "x", null, args);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addLong(i);
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createDateStats(10, 100));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong(19);
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addLong(15);
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testDecimalEqualsBloomFilter() throws Exception {
    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.DECIMAL, "x",
        new HiveDecimalWritable("15"),
        null);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addString(HiveDecimal.create(i).toString());
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createDecimalStats("10", "200"));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString(HiveDecimal.create(15).toString());
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testDecimalInBloomFilter() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add(new HiveDecimalWritable("15"));
    args.add(new HiveDecimalWritable("19"));
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.DECIMAL,
            "x", null, args);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addString(HiveDecimal.create(i).toString());
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createDecimalStats("10", "200"));
    assertEquals(TruthValue.NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString(HiveDecimal.create(19).toString());
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString(HiveDecimal.create(15).toString());
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testNullsInBloomFilter() throws Exception {
    List<Object> args = new ArrayList<Object>();
    args.add(new HiveDecimalWritable("15"));
    args.add(null);
    args.add(new HiveDecimalWritable("19"));
    PredicateLeaf pred = createPredicateLeaf
        (PredicateLeaf.Operator.IN, PredicateLeaf.Type.DECIMAL,
            "x", null, args);
    BloomFilter bf = new BloomFilter(10000);
    for (int i = 20; i < 1000; i++) {
      bf.addString(HiveDecimal.create(i).toString());
    }
    ColumnStatistics cs = ColumnStatisticsImpl.deserialize(null, createDecimalStats("10", "200", false));
    // hasNull is false, so bloom filter should return NO
    assertEquals(TruthValue.NO, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    cs = ColumnStatisticsImpl.deserialize(null, createDecimalStats("10", "200", true));
    // hasNull is true, so bloom filter should return YES_NO_NULL
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString(HiveDecimal.create(19).toString());
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));

    bf.addString(HiveDecimal.create(15).toString());
    assertEquals(TruthValue.YES_NO_NULL, RecordReaderImpl.evaluatePredicate(cs, pred, bf));
  }

  @Test
  public void testClose() throws Exception {
    DataReader mockedDataReader = mock(DataReader.class);
    DataReader cloned = mock(DataReader.class);
    when(mockedDataReader.clone()).thenReturn(cloned);
    closeMockedRecordReader(mockedDataReader);

    verify(cloned, atLeastOnce()).close();
  }

  @Test
  public void testCloseWithException() throws Exception {
    DataReader mockedDataReader = mock(DataReader.class);
    DataReader cloned = mock(DataReader.class);
    when(mockedDataReader.clone()).thenReturn(cloned);
    doThrow(IOException.class).when(cloned).close();

    try {
      closeMockedRecordReader(mockedDataReader);
      fail("Exception should have been thrown when Record Reader was closed");
    } catch (IOException expected) {

    }

    verify(cloned, atLeastOnce()).close();
  }

  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  private void closeMockedRecordReader(DataReader mockedDataReader) throws IOException {
    Configuration conf = new Configuration();
    Path path = new Path(workDir, "empty.orc");
    FileSystem.get(conf).delete(path, true);
    Writer writer = OrcFile.createWriter(path, OrcFile.writerOptions(conf)
        .setSchema(TypeDescription.createLong()));
    writer.close();
    Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));

    RecordReader recordReader = reader.rows(reader.options()
        .dataReader(mockedDataReader));

    recordReader.close();
  }

  static ByteBuffer createDataStream(int tag, int bytes) {
    ByteBuffer result = ByteBuffer.allocate(bytes);
    IntBuffer iBuf = result.asIntBuffer();
    for(int i= 0; i < bytes; i += 4) {
      iBuf.put((tag << 24) + i);
    }
    result.limit(bytes);
    return result;
  }

  static OrcProto.RowIndexEntry entry(int... values) {
    OrcProto.RowIndexEntry.Builder builder = OrcProto.RowIndexEntry.newBuilder();
    for(int v: values) {
      builder.addPositions(v);
    }
    return builder.build();
  }

  static ByteBuffer createRowIndex(InStream.StreamOptions options,
                                   OrcProto.RowIndexEntry... entries
                                   ) throws IOException {
    ByteBuffer uncompressed = createRowIndex(entries);
    if (options.getCodec() != null) {
      CompressionCodec codec = options.getCodec();
      PhysicalFsWriter.BufferedStream buffer =
          new PhysicalFsWriter.BufferedStream();
      StreamOptions writerOptions = new StreamOptions(options.getBufferSize())
          .withCodec(codec, codec.getDefaultOptions());
      try (OutStream out = new OutStream("row index", writerOptions, buffer)) {
        out.write(uncompressed.array(),
            uncompressed.arrayOffset() + uncompressed.position(),
            uncompressed.remaining());
        out.flush();
      }
      return buffer.getByteBuffer();
    } else {
      return uncompressed;
    }
  }

  static ByteBuffer createRowIndex(OrcProto.RowIndexEntry... entries) {
    OrcProto.RowIndex.Builder builder = OrcProto.RowIndex.newBuilder();
    for(OrcProto.RowIndexEntry entry: entries) {
      builder.addEntry(entry);
    }
    return ByteBuffer.wrap(builder.build().toByteArray());
  }

  static ByteBuffer createRowIndex(int value) {
    OrcProto.RowIndexEntry entry =
        OrcProto.RowIndexEntry.newBuilder().addPositions(value).build();
    return ByteBuffer.wrap(OrcProto.RowIndex.newBuilder().addEntry(entry)
                               .build().toByteArray());
  }

  static ByteBuffer createBloomFilter(int value) {
    OrcProto.BloomFilter entry =
        OrcProto.BloomFilter.newBuilder().setNumHashFunctions(value).build();
    return ByteBuffer.wrap(OrcProto.BloomFilterIndex.newBuilder()
                               .addBloomFilter(entry).build().toByteArray());
  }

  static MockDataReader createOldBlooms(TypeDescription schema) {
    return new MockDataReader(schema)
               .addStream(1, OrcProto.Stream.Kind.ROW_INDEX, createRowIndex(10))
               .addStream(1, OrcProto.Stream.Kind.BLOOM_FILTER, createBloomFilter(11))
               .addStream(2, OrcProto.Stream.Kind.ROW_INDEX, createRowIndex(20))
               .addStream(2, OrcProto.Stream.Kind.BLOOM_FILTER, createBloomFilter(21))
               .addStream(3, OrcProto.Stream.Kind.ROW_INDEX, createRowIndex(30))
               .addStream(3, OrcProto.Stream.Kind.BLOOM_FILTER, createBloomFilter(31))
               .addStripeFooter(1000, null);
  }

  static MockDataReader createMixedBlooms(TypeDescription schema) {
    return new MockDataReader(schema)
        .addStream(1, OrcProto.Stream.Kind.ROW_INDEX, createRowIndex(10))
        .addStream(1, OrcProto.Stream.Kind.BLOOM_FILTER, createBloomFilter(11))
        .addStream(2, OrcProto.Stream.Kind.ROW_INDEX, createRowIndex(20))
        .addStream(2, OrcProto.Stream.Kind.BLOOM_FILTER, createBloomFilter(21))
        .addStream(2, OrcProto.Stream.Kind.BLOOM_FILTER_UTF8, createBloomFilter(22))
        .addStream(3, OrcProto.Stream.Kind.ROW_INDEX, createRowIndex(30))
        .addStream(3, OrcProto.Stream.Kind.BLOOM_FILTER, createBloomFilter(31))
        .addStream(3, OrcProto.Stream.Kind.BLOOM_FILTER_UTF8, createBloomFilter(32))
        .addStripeFooter(1000, null);
  }

  static MockDataReader createNewBlooms(TypeDescription schema) {
    return new MockDataReader(schema)
               .addStream(1, OrcProto.Stream.Kind.ROW_INDEX, createRowIndex(10))
               .addStream(1, OrcProto.Stream.Kind.BLOOM_FILTER, createBloomFilter(11))
               .addStream(2, OrcProto.Stream.Kind.ROW_INDEX, createRowIndex(20))
               .addStream(2, OrcProto.Stream.Kind.BLOOM_FILTER_UTF8, createBloomFilter(22))
               .addStream(3, OrcProto.Stream.Kind.ROW_INDEX, createRowIndex(30))
               .addStream(3, OrcProto.Stream.Kind.BLOOM_FILTER_UTF8, createBloomFilter(32))
               .addStripeFooter(1000, null);
  }

  @Test
  public void testOldBloomFilters() throws Exception {
    TypeDescription schema = TypeDescription.fromString("struct<x:int,y:decimal(10,2),z:string>");
    MockDataReader dataReader = createOldBlooms(schema);
    MockStripe stripe = dataReader.getStripe(0);

    // use old blooms
    ReaderEncryption encryption = new ReaderEncryption();
    StripePlanner planner = new StripePlanner(schema, encryption, dataReader,
        OrcFile.WriterVersion.HIVE_4243, false, Integer.MAX_VALUE);
    planner.parseStripe(stripe, new boolean[]{true, true, false, true});
    OrcIndex index =
        planner.readRowIndex(new boolean[]{false, true, false, true}, null);
    OrcProto.Stream.Kind[] bloomFilterKinds = index.getBloomFilterKinds();
    assertEquals(OrcProto.Stream.Kind.BLOOM_FILTER, bloomFilterKinds[1]);
    assertEquals(OrcProto.Stream.Kind.BLOOM_FILTER, bloomFilterKinds[3]);
    assertEquals(1, stripe.getStream(1, OrcProto.Stream.Kind.ROW_INDEX).readCount);
    assertEquals(1, stripe.getStream(1, OrcProto.Stream.Kind.BLOOM_FILTER).readCount);
    assertEquals(0, stripe.getStream(2, OrcProto.Stream.Kind.ROW_INDEX).readCount);
    assertEquals(0, stripe.getStream(2, OrcProto.Stream.Kind.BLOOM_FILTER).readCount);
    assertEquals(1, stripe.getStream(3, OrcProto.Stream.Kind.ROW_INDEX).readCount);
    assertEquals(1, stripe.getStream(3, OrcProto.Stream.Kind.BLOOM_FILTER).readCount);

    // ignore non-utf8 bloom filter
    dataReader.resetCounts();
    Arrays.fill(bloomFilterKinds, null);
    planner = new StripePlanner(schema, encryption, dataReader,
        OrcFile.WriterVersion.HIVE_4243, true, Integer.MAX_VALUE);
    planner.parseStripe(stripe, new boolean[]{true, true, true, false});
    planner.readRowIndex(new boolean[]{false, true, true, false}, index);
    assertEquals(OrcProto.Stream.Kind.BLOOM_FILTER, bloomFilterKinds[1]);
    assertNull(bloomFilterKinds[2]);
    assertEquals(1, stripe.getStream(1, OrcProto.Stream.Kind.ROW_INDEX).readCount);
    assertEquals(1, stripe.getStream(1, OrcProto.Stream.Kind.BLOOM_FILTER).readCount);
    assertEquals(1, stripe.getStream(2, OrcProto.Stream.Kind.ROW_INDEX).readCount);
    assertEquals(0, stripe.getStream(2, OrcProto.Stream.Kind.BLOOM_FILTER).readCount);
    assertEquals(0, stripe.getStream(3, OrcProto.Stream.Kind.ROW_INDEX).readCount);
    assertEquals(0, stripe.getStream(3, OrcProto.Stream.Kind.BLOOM_FILTER).readCount);

    // check that we are handling the post hive-12055 strings correctly
    dataReader.resetCounts();
    Arrays.fill(bloomFilterKinds, null);
    planner = new StripePlanner(schema, encryption, dataReader,
        OrcFile.WriterVersion.HIVE_12055, true, Integer.MAX_VALUE);
    planner.parseStripe(stripe, new boolean[]{true, true, true, true});
    planner.readRowIndex(new boolean[]{false, true, true, true}, index);
    assertEquals(OrcProto.Stream.Kind.BLOOM_FILTER, bloomFilterKinds[1]);
    assertNull(bloomFilterKinds[2]);
    assertEquals(OrcProto.Stream.Kind.BLOOM_FILTER, bloomFilterKinds[3]);
    assertEquals(1, stripe.getStream(1, OrcProto.Stream.Kind.ROW_INDEX).readCount);
    assertEquals(1, stripe.getStream(1, OrcProto.Stream.Kind.BLOOM_FILTER).readCount);
    assertEquals(1, stripe.getStream(2, OrcProto.Stream.Kind.ROW_INDEX).readCount);
    assertEquals(0, stripe.getStream(2, OrcProto.Stream.Kind.BLOOM_FILTER).readCount);
    assertEquals(1, stripe.getStream(3, OrcProto.Stream.Kind.ROW_INDEX).readCount);
    assertEquals(1, stripe.getStream(3, OrcProto.Stream.Kind.BLOOM_FILTER).readCount);

    // ignore non-utf8 bloom filter on decimal
    dataReader.resetCounts();
    Arrays.fill(bloomFilterKinds, null);
    planner = new StripePlanner(schema, encryption, dataReader,
        OrcFile.WriterVersion.HIVE_4243, true, Integer.MAX_VALUE);
    planner.parseStripe(stripe, new boolean[]{true, false, true, false});
    planner.readRowIndex(new boolean[]{false, false, true, false}, index);
    assertNull(bloomFilterKinds[1]);
    assertNull(bloomFilterKinds[2]);
    assertEquals(0, stripe.getStream(1, OrcProto.Stream.Kind.ROW_INDEX).readCount);
    assertEquals(0, stripe.getStream(1, OrcProto.Stream.Kind.BLOOM_FILTER).readCount);
    assertEquals(1, stripe.getStream(2, OrcProto.Stream.Kind.ROW_INDEX).readCount);
    assertEquals(0, stripe.getStream(2, OrcProto.Stream.Kind.BLOOM_FILTER).readCount);
    assertEquals(0, stripe.getStream(3, OrcProto.Stream.Kind.ROW_INDEX).readCount);
    assertEquals(0, stripe.getStream(3, OrcProto.Stream.Kind.BLOOM_FILTER).readCount);
  }

  @Test
  public void testCompatibleBloomFilters() throws Exception {
    TypeDescription schema = TypeDescription.fromString("struct<x:int,y:decimal(10,2),z:string>");
    MockDataReader dataReader = createMixedBlooms(schema);
    MockStripe stripe = dataReader.getStripe(0);

    // use old bloom filters
    ReaderEncryption encryption = new ReaderEncryption();
    StripePlanner planner = new StripePlanner(schema, encryption, dataReader,
        OrcFile.WriterVersion.HIVE_4243, true, Integer.MAX_VALUE);
    planner.parseStripe(stripe, new boolean[]{true, true, false, true});
    OrcIndex index =
        planner.readRowIndex(new boolean[]{false, true, false, true}, null);

    OrcProto.Stream.Kind[] bloomFilterKinds = index.getBloomFilterKinds();
    assertEquals(OrcProto.Stream.Kind.BLOOM_FILTER, bloomFilterKinds[1]);
    assertEquals(OrcProto.Stream.Kind.BLOOM_FILTER_UTF8, bloomFilterKinds[3]);
    assertEquals(1, stripe.getStream(1, OrcProto.Stream.Kind.ROW_INDEX).readCount);
    assertEquals(1, stripe.getStream(1, OrcProto.Stream.Kind.BLOOM_FILTER).readCount);
    assertEquals(0, stripe.getStream(2, OrcProto.Stream.Kind.ROW_INDEX).readCount);
    assertEquals(0, stripe.getStream(2, OrcProto.Stream.Kind.BLOOM_FILTER).readCount);
    assertEquals(0, stripe.getStream(2, OrcProto.Stream.Kind.BLOOM_FILTER_UTF8).readCount);
    assertEquals(1, stripe.getStream(3, OrcProto.Stream.Kind.ROW_INDEX).readCount);
    assertEquals(0, stripe.getStream(3, OrcProto.Stream.Kind.BLOOM_FILTER).readCount);
    assertEquals(1, stripe.getStream(3, OrcProto.Stream.Kind.BLOOM_FILTER_UTF8).readCount);

    // ignore non-utf8 bloom filter
    Arrays.fill(bloomFilterKinds, null);
    dataReader.resetCounts();
    planner = new StripePlanner(schema, encryption, dataReader,
        OrcFile.WriterVersion.HIVE_4243, true, Integer.MAX_VALUE);
    planner.parseStripe(stripe, new boolean[]{true, true, true, false});
    planner.readRowIndex(new boolean[]{false, true, true, false}, index);
    assertEquals(OrcProto.Stream.Kind.BLOOM_FILTER, bloomFilterKinds[1]);
    assertEquals(OrcProto.Stream.Kind.BLOOM_FILTER_UTF8, bloomFilterKinds[2]);
    assertEquals(1, stripe.getStream(1, OrcProto.Stream.Kind.ROW_INDEX).readCount);
    assertEquals(1, stripe.getStream(1, OrcProto.Stream.Kind.BLOOM_FILTER).readCount);
    assertEquals(1, stripe.getStream(2, OrcProto.Stream.Kind.ROW_INDEX).readCount);
    assertEquals(0, stripe.getStream(2, OrcProto.Stream.Kind.BLOOM_FILTER).readCount);
    assertEquals(1, stripe.getStream(2, OrcProto.Stream.Kind.BLOOM_FILTER_UTF8).readCount);
    assertEquals(0, stripe.getStream(3, OrcProto.Stream.Kind.ROW_INDEX).readCount);
    assertEquals(0, stripe.getStream(3, OrcProto.Stream.Kind.BLOOM_FILTER).readCount);
    assertEquals(0, stripe.getStream(3, OrcProto.Stream.Kind.BLOOM_FILTER_UTF8).readCount);
  }

  @Test
  public void testNewBloomFilters() throws Exception {
    TypeDescription schema = TypeDescription.fromString("struct<x:int,y:decimal(10,2),z:string>");
    MockDataReader dataReader = createNewBlooms(schema);
    MockStripe stripe = dataReader.getStripe(0);

    // use old bloom filters
    ReaderEncryption encryption = new ReaderEncryption();
    StripePlanner planner = new StripePlanner(schema, encryption, dataReader,
        OrcFile.WriterVersion.HIVE_4243, true, Integer.MAX_VALUE);
    planner.parseStripe(stripe, new boolean[]{true, true, false, true});
    OrcIndex index =
        planner.readRowIndex(new boolean[]{false, true, false, true}, null);

    OrcProto.Stream.Kind[] bloomFilterKinds = index.getBloomFilterKinds();
    assertEquals(OrcProto.Stream.Kind.BLOOM_FILTER, bloomFilterKinds[1]);
    assertEquals(OrcProto.Stream.Kind.BLOOM_FILTER_UTF8, bloomFilterKinds[3]);
    assertEquals(1, stripe.getStream(1, OrcProto.Stream.Kind.ROW_INDEX).readCount);
    assertEquals(1, stripe.getStream(1, OrcProto.Stream.Kind.BLOOM_FILTER).readCount);
    assertEquals(0, stripe.getStream(2, OrcProto.Stream.Kind.ROW_INDEX).readCount);
    assertEquals(0, stripe.getStream(2, OrcProto.Stream.Kind.BLOOM_FILTER_UTF8).readCount);
    assertEquals(1, stripe.getStream(3, OrcProto.Stream.Kind.ROW_INDEX).readCount);
    assertEquals(1, stripe.getStream(3, OrcProto.Stream.Kind.BLOOM_FILTER_UTF8).readCount);

    // ignore non-utf8 bloom filter
    Arrays.fill(bloomFilterKinds, null);
    dataReader.resetCounts();
    planner = new StripePlanner(schema, encryption, dataReader,
        OrcFile.WriterVersion.HIVE_4243, true, Integer.MAX_VALUE);
    planner.parseStripe(stripe, new boolean[]{true, true, true, false});
    planner.readRowIndex(new boolean[]{false, true, true, false}, index);
    assertEquals(OrcProto.Stream.Kind.BLOOM_FILTER, bloomFilterKinds[1]);
    assertEquals(OrcProto.Stream.Kind.BLOOM_FILTER_UTF8, bloomFilterKinds[2]);
    assertEquals(1, stripe.getStream(1, OrcProto.Stream.Kind.ROW_INDEX).readCount);
    assertEquals(1, stripe.getStream(1, OrcProto.Stream.Kind.BLOOM_FILTER).readCount);
    assertEquals(1, stripe.getStream(2, OrcProto.Stream.Kind.ROW_INDEX).readCount);
    assertEquals(1, stripe.getStream(2, OrcProto.Stream.Kind.BLOOM_FILTER_UTF8).readCount);
    assertEquals(0, stripe.getStream(3, OrcProto.Stream.Kind.ROW_INDEX).readCount);
    assertEquals(0, stripe.getStream(3, OrcProto.Stream.Kind.BLOOM_FILTER_UTF8).readCount);
  }

  static OrcProto.RowIndexEntry createIndexEntry(Long min, Long max) {
    return OrcProto.RowIndexEntry.newBuilder()
             .setStatistics(createIntStats(min, max)).build();
  }

  @Test
  public void testPickRowGroups() throws Exception {
    Configuration conf = new Configuration();
    TypeDescription schema = TypeDescription.fromString("struct<x:int,y:int>");
    SchemaEvolution evolution = new SchemaEvolution(schema, schema,
        new Reader.Options(conf));
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
            .startAnd()
            .equals("x", PredicateLeaf.Type.LONG, 100L)
            .equals("y", PredicateLeaf.Type.LONG, 10L)
            .end().build();
    RecordReaderImpl.SargApplier applier =
        new RecordReaderImpl.SargApplier(sarg, 1000, evolution,
            OrcFile.WriterVersion.ORC_135, false, false, false);
    OrcProto.StripeInformation stripe =
        OrcProto.StripeInformation.newBuilder().setNumberOfRows(4000).build();
    OrcProto.RowIndex[] indexes = new OrcProto.RowIndex[3];
    indexes[1] = OrcProto.RowIndex.newBuilder()
        .addEntry(createIndexEntry(0L, 10L))
        .addEntry(createIndexEntry(100L, 200L))
        .addEntry(createIndexEntry(300L, 500L))
        .addEntry(createIndexEntry(100L, 100L))
        .build();
    indexes[2] = OrcProto.RowIndex.newBuilder()
        .addEntry(createIndexEntry(0L, 9L))
        .addEntry(createIndexEntry(11L, 20L))
        .addEntry(createIndexEntry(10L, 10L))
        .addEntry(createIndexEntry(0L, 100L))
        .build();
    List<OrcProto.ColumnEncoding> encodings = new ArrayList<>();
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2).build());
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2).build());
    boolean[] rows = applier.pickRowGroups(
        new ReaderImpl.StripeInformationImpl(stripe, 1, -1, null),
        indexes, null, encodings, null, false);
    assertEquals(4, rows.length);
    assertFalse(rows[0]);
    assertFalse(rows[1]);
    assertFalse(rows[2]);
    assertTrue(rows[3]);
    assertEquals(0, applier.getExceptionCount()[0]);
    assertEquals(0, applier.getExceptionCount()[1]);
  }

  @Test
  public void testPickRowGroupsError() throws Exception {
    Configuration conf = new Configuration();
    TypeDescription schema = TypeDescription.fromString("struct<x:int,y:int>");
    SchemaEvolution evolution = new SchemaEvolution(schema, schema,
        new Reader.Options(conf));
    SearchArgument sarg =
        SearchArgumentFactory.newBuilder()
            .startAnd()
              .equals("x", PredicateLeaf.Type.DATE, Date.valueOf("2017-01-02"))
              .equals("y", PredicateLeaf.Type.LONG, 10L)
            .end().build();
    RecordReaderImpl.SargApplier applier =
        new RecordReaderImpl.SargApplier(sarg, 1000, evolution,
            OrcFile.WriterVersion.ORC_135, false, false, false);
    OrcProto.StripeInformation stripe =
        OrcProto.StripeInformation.newBuilder().setNumberOfRows(3000).build();
    OrcProto.RowIndex[] indexes = new OrcProto.RowIndex[3];
    indexes[1] = OrcProto.RowIndex.newBuilder()
        .addEntry(createIndexEntry(0L, 10L))
        .addEntry(createIndexEntry(10L, 20L))
        .addEntry(createIndexEntry(20L, 30L))
        .build();
    indexes[2] = OrcProto.RowIndex.newBuilder()
        .addEntry(createIndexEntry(0L, 9L))
        .addEntry(createIndexEntry(10L, 20L))
        .addEntry(createIndexEntry(0L, 30L))
        .build();
    List<OrcProto.ColumnEncoding> encodings = new ArrayList<>();
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2).build());
    encodings.add(OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2).build());
    boolean[] rows = applier.pickRowGroups(
        new ReaderImpl.StripeInformationImpl(stripe, 1, -1, null),
        indexes, null, encodings, null, false);
    assertEquals(3, rows.length);
    assertFalse(rows[0]);
    assertTrue(rows[1]);
    assertTrue(rows[2]);
    assertEquals(1, applier.getExceptionCount()[0]);
    assertEquals(0, applier.getExceptionCount()[1]);
  }

  @Test
  public void testPositionalEvolutionAddColumnPPD() throws IOException {
    Reader.Options opts = new Reader.Options();
    opts.forcePositionalEvolution(true);

    TypeDescription file = TypeDescription.fromString("struct<x:int>");
    // new column added on reader side
    TypeDescription read = TypeDescription.fromString("struct<x:int,y:boolean>");
    opts.include(includeAll(read));

    SchemaEvolution evo = new SchemaEvolution(file, read, opts);

    SearchArgument sarg = SearchArgumentFactory.newBuilder().startAnd()
        .equals("y", PredicateLeaf.Type.BOOLEAN, true).end().build();

    RecordReaderImpl.SargApplier applier =
        new RecordReaderImpl.SargApplier(sarg, 1000, evo,
            OrcFile.WriterVersion.ORC_135, false, false, false);

    OrcProto.StripeInformation stripe =
        OrcProto.StripeInformation.newBuilder().setNumberOfRows(2000).build();

    OrcProto.RowIndex[] indexes = new OrcProto.RowIndex[3];
    indexes[1] = OrcProto.RowIndex.newBuilder() // index for original x column
        .addEntry(createIndexEntry(0L, 10L))
        .addEntry(createIndexEntry(100L, 200L))
        .build();
    indexes[2] = null; // no-op, just for clarifying that new reader column doesn't have an index

    List<OrcProto.ColumnEncoding> encodings = new ArrayList<>();
    encodings.add(OrcProto.ColumnEncoding.newBuilder().setKind(OrcProto.ColumnEncoding.Kind.DIRECT).build());

    boolean[] rows = applier.pickRowGroups(new ReaderImpl.StripeInformationImpl(stripe,  1, -1, null),
        indexes, null, encodings, null, false);
    assertTrue(Arrays.equals(SargApplier.READ_ALL_RGS, rows)); //cannot filter for new column, return all rows
  }

  private boolean[] includeAll(TypeDescription readerType) {
    int numColumns = readerType.getMaximumId() + 1;
    boolean[] result = new boolean[numColumns];
    Arrays.fill(result, true);
    return result;
  }

  @Test
  public void testSkipDataReaderOpen() throws Exception {
    IOException ioe = new IOException("Don't open when there is no stripe");

    DataReader mockedDataReader = mock(DataReader.class);
    doThrow(ioe).when(mockedDataReader).open();
    when(mockedDataReader.clone()).thenReturn(mockedDataReader);
    doNothing().when(mockedDataReader).close();

    Configuration conf = new Configuration();
    Path path = new Path(workDir, "empty.orc");
    FileSystem.get(conf).delete(path, true);
    OrcFile.WriterOptions options = OrcFile.writerOptions(conf).setSchema(TypeDescription.createLong());
    Writer writer = OrcFile.createWriter(path, options);
    writer.close();

    Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    Reader.Options readerOptions = reader.options().dataReader(mockedDataReader);
    RecordReader recordReader = reader.rows(readerOptions);
    recordReader.close();
  }

  @Test
  public void testCloseAtConstructorException() throws Exception {
    Configuration conf = new Configuration();
    Path path = new Path(workDir, "oneRow.orc");
    FileSystem.get(conf).delete(path, true);

    TypeDescription schema = TypeDescription.createLong();
    OrcFile.WriterOptions options = OrcFile.writerOptions(conf).setSchema(schema);
    Writer writer = OrcFile.createWriter(path, options);
    VectorizedRowBatch writeBatch = schema.createRowBatch();
    int row = writeBatch.size++;
    ((LongColumnVector) writeBatch.cols[0]).vector[row] = 0;
    writer.addRowBatch(writeBatch);
    writer.close();

    DataReader mockedDataReader = mock(DataReader.class);
    when(mockedDataReader.clone()).thenReturn(mockedDataReader);
    doThrow(new IOException()).when(mockedDataReader).readStripeFooter(any());

    Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    Reader.Options readerOptions = reader.options().dataReader(mockedDataReader);
    boolean isCalled = false;
    try {
      reader.rows(readerOptions);
    } catch (IOException ie) {
      isCalled = true;
    }
    assertTrue(isCalled);
    verify(mockedDataReader, times(1)).close();
  }

  @Test
  public void testSargApplier() throws Exception {
    Configuration conf = new Configuration();
    TypeDescription schema = TypeDescription.createLong();
    SearchArgument sarg = SearchArgumentFactory.newBuilder().build();
    SchemaEvolution evo = new SchemaEvolution(schema, schema, new Reader.Options(conf));
    RecordReaderImpl.SargApplier applier1 =
      new RecordReaderImpl.SargApplier(sarg, 0, evo, OrcFile.WriterVersion.ORC_135, false);

    Field f1 = RecordReaderImpl.SargApplier.class.getDeclaredField("writerUsedProlepticGregorian");
    f1.setAccessible(true);
    assertFalse((boolean)f1.get(applier1));

    Field f2 = RecordReaderImpl.SargApplier.class.getDeclaredField("convertToProlepticGregorian");
    f2.setAccessible(true);
    assertFalse((boolean)f2.get(applier1));
  }

  @Test
  public void testWithoutStatistics() {
    OrcProto.ColumnEncoding encoding = OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2)
        .build();

    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.LONG, "x", 2L, null);

    TruthValue truthValue = RecordReaderImpl.evaluatePredicateProto(
        RecordReaderImpl.EMPTY_COLUMN_STATISTICS,
        pred, null, encoding, null,
        CURRENT_WRITER, TypeDescription.createInt());

    assertEquals(TruthValue.YES_NO_NULL, truthValue);
  }

  @Test
  public void testMissMinOrMaxInStatistics() {
    OrcProto.ColumnEncoding encoding = OrcProto.ColumnEncoding.newBuilder()
        .setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2)
        .build();

    PredicateLeaf pred = createPredicateLeaf(
        PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.LONG, "x", 2L, null);

    OrcProto.ColumnStatistics hasValuesAndHasNullStatistics =
        OrcProto.ColumnStatistics.newBuilder().setNumberOfValues(10)
            .setHasNull(true)
            .setBytesOnDisk(40)
            .build();

    OrcProto.ColumnStatistics hasValuesAndNoHasNullStatistics =
        OrcProto.ColumnStatistics.newBuilder().setNumberOfValues(5)
            .setHasNull(false)
            .setBytesOnDisk(20)
            .build();

    OrcProto.ColumnStatistics noHasValuesAndHasNullStatistics =
        OrcProto.ColumnStatistics.newBuilder().setNumberOfValues(0)
            .setHasNull(true)
            .setBytesOnDisk(0)
            .build();

    TruthValue whenHasValuesAndHasNullTruthValue = RecordReaderImpl.evaluatePredicateProto(
        hasValuesAndHasNullStatistics,
        pred, null, encoding, null,
        CURRENT_WRITER, TypeDescription.createInt());

    assertEquals(TruthValue.YES_NO_NULL, whenHasValuesAndHasNullTruthValue);

    TruthValue whenHasValuesAndNoHasNullTruthValue = RecordReaderImpl.evaluatePredicateProto(
        hasValuesAndNoHasNullStatistics,
        pred, null, encoding, null,
        CURRENT_WRITER, TypeDescription.createInt());

    assertEquals(TruthValue.YES_NO, whenHasValuesAndNoHasNullTruthValue);

    TruthValue whenNoHasValuesAndHasNullStatistics = RecordReaderImpl.evaluatePredicateProto(
        noHasValuesAndHasNullStatistics,
        pred, null, encoding, null,
        CURRENT_WRITER, TypeDescription.createInt());

    assertEquals(TruthValue.NULL, whenNoHasValuesAndHasNullStatistics);
  }

  @Test
  public void testRgEndOffset() throws IOException {
    for (int compressionSize = 64; compressionSize < 4096; compressionSize *= 2) {
      testSmallCompressionSizeOrc(compressionSize);
    }
  }

  private void testSmallCompressionSizeOrc(int compressionSize) throws IOException {
    Configuration conf = new Configuration();
    Path path = new Path(workDir, "smallCompressionSize.orc");
    FileSystem.get(conf).delete(path, true);

    TypeDescription schema = TypeDescription.fromString("struct<x:int>");
    conf.setLong(OrcConf.BUFFER_SIZE.getAttribute(), compressionSize);
    OrcFile.WriterOptions options = OrcFile.writerOptions(conf).setSchema(schema);
    Writer writer = OrcFile.createWriter(path, options);
    VectorizedRowBatch writeBatch = schema.createRowBatch();
    LongColumnVector writeX = (LongColumnVector) writeBatch.cols[0];
    for (int row = 0; row < 30_000; ++row) {
      int idx = writeBatch.size++;
      writeX.vector[idx] = row >= 10_000 && row < 20_000 ? row + 100_000 : row;
      if (writeBatch.size == writeBatch.getMaxSize()) {
        writer.addRowBatch(writeBatch);
        writeBatch.reset();
      }
    }
    if (writeBatch.size != 0) {
      writer.addRowBatch(writeBatch);
    }
    writer.close();

    Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    // only the second row group will be selected
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
        .startNot()
        .lessThan("x", PredicateLeaf.Type.LONG, 100000L)
        .end().build();
    VectorizedRowBatch batch = reader.getSchema().createRowBatch();
    LongColumnVector readX = (LongColumnVector) batch.cols[0];
    try (RecordReader rows = reader.rows(reader.options().searchArgument(sarg, null))) {
      int row = 10_000;
      while (rows.nextBatch(batch)) {
        for (int i = 0; i < batch.size; i++) {
          final int current_row = row++;
          final int expectedVal = current_row >= 10_000 && current_row < 20_000 ? current_row + 100_000 : current_row;
          assertEquals(expectedVal, readX.vector[i]);
        }
      }
    }
  }

  @Test
  public void testRowIndexStrideNegativeFilter() throws Exception {
    Path testFilePath = new Path(workDir, "rowIndexStrideNegative.orc");
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    fs.delete(testFilePath, true);

    TypeDescription schema =
        TypeDescription.fromString("struct<str:string>");
    Writer writer = OrcFile.createWriter(
        testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema).rowIndexStride(-1));
    VectorizedRowBatch batch = schema.createRowBatch();
    BytesColumnVector strVector = (BytesColumnVector) batch.cols[0];
    for (int i = 0; i < 32 * 1024; i++) {
      if (batch.size == batch.getMaxSize()) {
        writer.addRowBatch(batch);
        batch.reset();
      }
      byte[] value = String.format("row %06d", i).getBytes(StandardCharsets.UTF_8);
      strVector.setRef(batch.size, value, 0, value.length);
      ++batch.size;
    }
    writer.addRowBatch(batch);
    batch.reset();
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf).filesystem(fs));
    SearchArgument sarg = SearchArgumentFactory.newBuilder(conf)
        .startNot().isNull("str", PredicateLeaf.Type.STRING).end()
        .build();
    RecordReader recordReader = reader.rows(reader.options().searchArgument(sarg, null));
    batch = reader.getSchema().createRowBatch();
    strVector = (BytesColumnVector) batch.cols[0];
    long base = 0;
    while (recordReader.nextBatch(batch)) {
      for (int r = 0; r < batch.size; ++r) {
        String value = String.format("row %06d", r + base);
        assertEquals(value, strVector.toString(r), "row " + (r + base));
      }
      base += batch.size;
    }
  }
}
