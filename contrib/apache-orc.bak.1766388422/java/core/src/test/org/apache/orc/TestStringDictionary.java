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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.impl.OutStream;
import org.apache.orc.impl.RecordReaderImpl;
import org.apache.orc.impl.StreamName;
import org.apache.orc.impl.TestInStream;
import org.apache.orc.impl.writer.StreamOptions;
import org.apache.orc.impl.writer.StringTreeWriter;
import org.apache.orc.impl.writer.TreeWriter;
import org.apache.orc.impl.writer.WriterContext;
import org.apache.orc.impl.writer.WriterEncryptionVariant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class TestStringDictionary {

  private Path workDir = new Path(System.getProperty("test.tmp.dir", "target" + File.separator + "test"
      + File.separator + "tmp"));

  private Configuration conf;
  private FileSystem fs;
  private Path testFilePath;

  @BeforeEach
  public void openFileSystem(TestInfo testInfo) throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestStringDictionary." +
        testInfo.getTestMethod().get().getName() + ".orc");
    fs.delete(testFilePath, false);
  }

  private static Stream<Arguments> data() {
    return Stream.of(Arguments.of("RBTREE"), Arguments.of("HASH"));
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testTooManyDistinct(String dictImpl) throws Exception {
    OrcConf.DICTIONARY_IMPL.setString(conf, dictImpl);
    TypeDescription schema = TypeDescription.createString();

    Writer writer = OrcFile.createWriter(
        testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema)
                                   .compress(CompressionKind.NONE)
                                   .bufferSize(10000));
    VectorizedRowBatch batch = schema.createRowBatch();
    BytesColumnVector col = (BytesColumnVector) batch.cols[0];
    for (int i = 0; i < 20000; i++) {
      if (batch.size == batch.getMaxSize()) {
        writer.addRowBatch(batch);
        batch.reset();
      }
      col.setVal(batch.size++, String.valueOf(i).getBytes(StandardCharsets.UTF_8));
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    col = (BytesColumnVector) batch.cols[0];
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(String.valueOf(idx++), col.toString(r));
      }
    }

    // make sure the encoding type is correct
    for (StripeInformation stripe : reader.getStripes()) {
      // hacky but does the job, this casting will work as long this test resides
      // within the same package as ORC reader
      OrcProto.StripeFooter footer = ((RecordReaderImpl) rows).readStripeFooter(stripe);
      for (int i = 0; i < footer.getColumnsCount(); ++i) {
        OrcProto.ColumnEncoding encoding = footer.getColumns(i);
        assertEquals(OrcProto.ColumnEncoding.Kind.DIRECT_V2, encoding.getKind());
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testHalfDistinct(String dictImpl) throws Exception {
    OrcConf.DICTIONARY_IMPL.setString(conf, dictImpl);
    final int totalSize = 20000;
    final int bound = 10000;

    TypeDescription schema = TypeDescription.createString();
    Writer writer = OrcFile.createWriter(
        testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema).compress(CompressionKind.NONE)
            .bufferSize(bound));
    Random rand = new Random(123);
    int[] input = new int[totalSize];
    for (int i = 0; i < totalSize; i++) {
      input[i] = rand.nextInt(bound);
    }

    VectorizedRowBatch batch = schema.createRowBatch();
    BytesColumnVector col = (BytesColumnVector) batch.cols[0];
    for (int i = 0; i < totalSize; i++) {
      if (batch.size == batch.getMaxSize()) {
        writer.addRowBatch(batch);
        batch.reset();
      }
      col.setVal(batch.size++, String.valueOf(input[i]).getBytes(StandardCharsets.UTF_8));
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    col = (BytesColumnVector) batch.cols[0];
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(String.valueOf(input[idx++]), col.toString(r));
      }
    }

    // make sure the encoding type is correct
    for (StripeInformation stripe : reader.getStripes()) {
      // hacky but does the job, this casting will work as long this test resides
      // within the same package as ORC reader
      OrcProto.StripeFooter footer = ((RecordReaderImpl) rows).readStripeFooter(stripe);
      for (int i = 0; i < footer.getColumnsCount(); ++i) {
        OrcProto.ColumnEncoding encoding = footer.getColumns(i);
        assertEquals(OrcProto.ColumnEncoding.Kind.DICTIONARY_V2, encoding.getKind());
      }
    }
  }

  static class WriterContextImpl implements WriterContext {
    private final TypeDescription schema;
    private final Configuration conf;
    private final Map<StreamName, TestInStream.OutputCollector> streams =
        new HashMap<>();

    WriterContextImpl(TypeDescription schema, Configuration conf) {
      this.schema = schema;
      this.conf = conf;
    }

    @Override
    public OutStream createStream(StreamName name) {
      TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
      streams.put(name, collect);
      return new OutStream("test", new StreamOptions(1000), collect);
    }

    @Override
    public int getRowIndexStride() {
      return 10000;
    }

    @Override
    public boolean buildIndex() {
      return OrcConf.ENABLE_INDEXES.getBoolean(conf);
    }

    @Override
    public boolean isCompressed() {
      return false;
    }

    @Override
    public OrcFile.EncodingStrategy getEncodingStrategy() {
      return OrcFile.EncodingStrategy.SPEED;
    }

    @Override
    public boolean[] getBloomFilterColumns() {
      return new boolean[schema.getMaximumId() + 1];
    }

    @Override
    public double getBloomFilterFPP() {
      return 0;
    }

    @Override
    public Configuration getConfiguration() {
      return conf;
    }

    @Override
    public OrcFile.Version getVersion() {
      return OrcFile.Version.V_0_12;
    }

    @Override
    public PhysicalWriter getPhysicalWriter() {
      return null;
    }

    @Override
    public void setEncoding(int column, WriterEncryptionVariant variant, OrcProto.ColumnEncoding encoding) {

    }

    @Override
    public void writeStatistics(StreamName name, OrcProto.ColumnStatistics.Builder stats) {

    }

    @Override
    public OrcFile.BloomFilterVersion getBloomFilterVersion() {
      return OrcFile.BloomFilterVersion.UTF8;
    }

    @Override
    public void writeIndex(StreamName name, OrcProto.RowIndex.Builder index) {

    }

    @Override
    public void writeBloomFilter(StreamName name,
                                 OrcProto.BloomFilterIndex.Builder bloom) {

    }

    @Override
    public DataMask getUnencryptedMask(int columnId) {
      return null;
    }

    @Override
    public WriterEncryptionVariant getEncryption(int columnId) {
      return null;
    }

    @Override
    public boolean getUseUTCTimestamp() {
      return true;
    }

    @Override
    public double getDictionaryKeySizeThreshold(int column) {
      return OrcConf.DICTIONARY_KEY_SIZE_THRESHOLD.getDouble(conf);
    }

    @Override
    public boolean getProlepticGregorian() {
      return false;
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testNonDistinctDisabled(String dictImpl) throws Exception {
    OrcConf.DICTIONARY_IMPL.setString(conf, dictImpl);
    TypeDescription schema = TypeDescription.createString();

    conf.set(OrcConf.DICTIONARY_KEY_SIZE_THRESHOLD.getAttribute(), "0.0");
    WriterContextImpl writerContext = new WriterContextImpl(schema, conf);
    StringTreeWriter writer = (StringTreeWriter)
        TreeWriter.Factory.create(schema, null, writerContext);

    VectorizedRowBatch batch = schema.createRowBatch();
    BytesColumnVector col = (BytesColumnVector) batch.cols[0];
    batch.size = 1024;
    col.isRepeating = true;
    col.setVal(0, "foobar".getBytes(StandardCharsets.UTF_8));
    writer.writeBatch(col, 0, batch.size);
    TestInStream.OutputCollector output = writerContext.streams.get(
        new StreamName(0, OrcProto.Stream.Kind.DATA));
    // Check to make sure that the strings are being written to the stream,
    // even before we get to the first rowGroup. (6 * 1024 / 1000 * 1000)
    assertEquals(6000, output.buffer.size());
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testTooManyDistinctCheckDisabled(String dictImpl) throws Exception {
    OrcConf.DICTIONARY_IMPL.setString(conf, dictImpl);
    TypeDescription schema = TypeDescription.createString();

    conf.setBoolean(OrcConf.ROW_INDEX_STRIDE_DICTIONARY_CHECK.getAttribute(), false);
    Writer writer = OrcFile.createWriter(
        testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema).compress(CompressionKind.NONE)
            .bufferSize(10000));
    VectorizedRowBatch batch = schema.createRowBatch();
    BytesColumnVector string = (BytesColumnVector) batch.cols[0];
    for (int i = 0; i < 20000; i++) {
      if (batch.size == batch.getMaxSize()) {
        writer.addRowBatch(batch);
        batch.reset();
      }
      string.setVal(batch.size++, String.valueOf(i).getBytes(StandardCharsets.UTF_8));
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    string = (BytesColumnVector) batch.cols[0];
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(String.valueOf(idx++), string.toString(r));
      }
    }

    // make sure the encoding type is correct
    for (StripeInformation stripe : reader.getStripes()) {
      // hacky but does the job, this casting will work as long this test resides
      // within the same package as ORC reader
      OrcProto.StripeFooter footer = ((RecordReaderImpl) rows).readStripeFooter(stripe);
      for (int i = 0; i < footer.getColumnsCount(); ++i) {
        OrcProto.ColumnEncoding encoding = footer.getColumns(i);
        assertEquals(OrcProto.ColumnEncoding.Kind.DIRECT_V2, encoding.getKind());
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testHalfDistinctCheckDisabled(String dictImpl) throws Exception {
    OrcConf.DICTIONARY_IMPL.setString(conf, dictImpl);
    TypeDescription schema = TypeDescription.createString();

    conf.setBoolean(OrcConf.ROW_INDEX_STRIDE_DICTIONARY_CHECK.getAttribute(),
        false);
    Writer writer = OrcFile.createWriter(
        testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema)
            .compress(CompressionKind.NONE)
            .bufferSize(10000));
    Random rand = new Random(123);
    int[] input = new int[20000];
    for (int i = 0; i < 20000; i++) {
      input[i] = rand.nextInt(10000);
    }
    VectorizedRowBatch batch = schema.createRowBatch();
    BytesColumnVector string = (BytesColumnVector) batch.cols[0];
    for (int i = 0; i < 20000; i++) {
      if (batch.size == batch.getMaxSize()) {
        writer.addRowBatch(batch);
        batch.reset();
      }
      string.setVal(batch.size++, String.valueOf(input[i]).getBytes(StandardCharsets.UTF_8));
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    string = (BytesColumnVector) batch.cols[0];
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(String.valueOf(input[idx++]), string.toString(r));
      }
    }

    // make sure the encoding type is correct
    for (StripeInformation stripe : reader.getStripes()) {
      // hacky but does the job, this casting will work as long this test resides
      // within the same package as ORC reader
      OrcProto.StripeFooter footer = ((RecordReaderImpl) rows).readStripeFooter(stripe);
      for (int i = 0; i < footer.getColumnsCount(); ++i) {
        OrcProto.ColumnEncoding encoding = footer.getColumns(i);
        assertEquals(OrcProto.ColumnEncoding.Kind.DICTIONARY_V2, encoding.getKind());
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testTooManyDistinctV11AlwaysDictionary(String dictImpl) throws Exception {
    OrcConf.DICTIONARY_IMPL.setString(conf, dictImpl);
    TypeDescription schema = TypeDescription.createString();

    Writer writer = OrcFile.createWriter(
        testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema)
            .compress(CompressionKind.NONE)
            .version(OrcFile.Version.V_0_11).bufferSize(10000));
    VectorizedRowBatch batch = schema.createRowBatch();
    BytesColumnVector string = (BytesColumnVector) batch.cols[0];
    for (int i = 0; i < 20000; i++) {
      if (batch.size == batch.getMaxSize()) {
        writer.addRowBatch(batch);
        batch.reset();
      }
      string.setVal(batch.size++, String.valueOf(i).getBytes(StandardCharsets.UTF_8));
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf).filesystem(fs));
    batch = reader.getSchema().createRowBatch();
    string = (BytesColumnVector) batch.cols[0];
    RecordReader rows = reader.rows();
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(String.valueOf(idx++), string.toString(r));
      }
    }

    // make sure the encoding type is correct
    for (StripeInformation stripe : reader.getStripes()) {
      // hacky but does the job, this casting will work as long this test resides
      // within the same package as ORC reader
      OrcProto.StripeFooter footer = ((RecordReaderImpl) rows).readStripeFooter(stripe);
      for (int i = 0; i < footer.getColumnsCount(); ++i) {
        OrcProto.ColumnEncoding encoding = footer.getColumns(i);
        assertEquals(OrcProto.ColumnEncoding.Kind.DICTIONARY, encoding.getKind());
      }
    }

  }

  /**
   * Test that dictionaries can be disabled, per column. In this test, we want to disable DICTIONARY_V2 for the
   * `longString` column (presumably for a low hit-ratio), while preserving DICTIONARY_V2 for `shortString`.
   * @throws Exception on unexpected failure
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testDisableDictionaryForSpecificColumn(String dictImpl) throws Exception {
    OrcConf.DICTIONARY_IMPL.setString(conf, dictImpl);
    final String SHORT_STRING_VALUE = "foo";
    final String  LONG_STRING_VALUE = "BAAAAAAAAR!!";

    TypeDescription schema =
        TypeDescription.fromString("struct<shortString:string,longString:string>");

    Writer writer = OrcFile.createWriter(
        testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema)
            .compress(CompressionKind.NONE)
            .bufferSize(10000)
            .directEncodingColumns("longString"));

    VectorizedRowBatch batch = schema.createRowBatch();
    BytesColumnVector shortStringColumnVector = (BytesColumnVector) batch.cols[0];
    BytesColumnVector longStringColumnVector  = (BytesColumnVector) batch.cols[1];

    for (int i = 0; i < 20000; i++) {
      if (batch.size == batch.getMaxSize()) {
        writer.addRowBatch(batch);
        batch.reset();
      }
      shortStringColumnVector.setVal(batch.size, SHORT_STRING_VALUE.getBytes(StandardCharsets.UTF_8));
      longStringColumnVector.setVal( batch.size, LONG_STRING_VALUE.getBytes(StandardCharsets.UTF_8));
      ++batch.size;
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf).filesystem(fs));
    RecordReader recordReader = reader.rows();
    batch = reader.getSchema().createRowBatch();
    shortStringColumnVector = (BytesColumnVector) batch.cols[0];
    longStringColumnVector  = (BytesColumnVector) batch.cols[1];
    while (recordReader.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(SHORT_STRING_VALUE, shortStringColumnVector.toString(r));
        assertEquals(LONG_STRING_VALUE,   longStringColumnVector.toString(r));
      }
    }

    // make sure the encoding type is correct
    for (StripeInformation stripe : reader.getStripes()) {
      // hacky but does the job, this casting will work as long this test resides
      // within the same package as ORC reader
      OrcProto.StripeFooter footer = ((RecordReaderImpl) recordReader).readStripeFooter(stripe);
      for (int i = 0; i < footer.getColumnsCount(); ++i) {
        assertEquals(3, footer.getColumnsCount(),
            "Expected 3 columns in the footer: One for the Orc Struct, and two for its members.");
        assertEquals(
            OrcProto.ColumnEncoding.Kind.DIRECT, footer.getColumns(0).getKind(),
            "The ORC schema struct should be DIRECT encoded."
        );
        assertEquals(
            OrcProto.ColumnEncoding.Kind.DICTIONARY_V2, footer.getColumns(1).getKind(),
            "The shortString column must be DICTIONARY_V2 encoded"
        );
        assertEquals(
            OrcProto.ColumnEncoding.Kind.DIRECT_V2, footer.getColumns(2).getKind(),
            "The longString column must be DIRECT_V2 encoded"
        );
      }
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testForcedNonDictionary(String dictImpl) throws Exception {
    OrcConf.DICTIONARY_IMPL.setString(conf, dictImpl);
    // Set the row stride to 16k so that it is a multiple of the batch size
    final int INDEX_STRIDE = 16 * 1024;
    final int NUM_BATCHES = 50;
    // Explicitly turn off dictionary encoding.
    OrcConf.DICTIONARY_KEY_SIZE_THRESHOLD.setDouble(conf, 0);
    TypeDescription schema = TypeDescription.fromString("struct<str:string>");
    try (Writer writer = OrcFile.createWriter(testFilePath,
           OrcFile.writerOptions(conf)
               .setSchema(schema)
               .rowIndexStride(INDEX_STRIDE))) {
      // Write 50 batches where each batch has a single value for str.
      VectorizedRowBatch batch = schema.createRowBatchV2();
      BytesColumnVector col = (BytesColumnVector) batch.cols[0];
      for(int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = 1024;
        col.setVal(0, ("Value for " + b).getBytes(StandardCharsets.UTF_8));
        col.isRepeating = true;
        writer.addRowBatch(batch);
      }
    }

    try (Reader reader = OrcFile.createReader(testFilePath,
                                              OrcFile.readerOptions(conf));
         RecordReaderImpl rows = (RecordReaderImpl) reader.rows()) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      BytesColumnVector col = (BytesColumnVector) batch.cols[0];

      // Get the index for the str column
      OrcProto.RowIndex index = rows.readRowIndex(0, null, null)
                                      .getRowGroupIndex()[1];
      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());
      // There are 4 entries, because ceil(NUM_BATCHES * 1024 / INDEX_STRIDE) = 4.
      assertEquals(4, index.getEntryCount());
      for(int e=0; e < index.getEntryCount(); ++e) {
        OrcProto.RowIndexEntry entry = index.getEntry(e);
        // For a string column with direct encoding, compression & no nulls, we
        // should have 5 positions in each entry.
        assertEquals(5, entry.getPositionsCount(), "position count entry " + e);
        // make sure we can seek and get the right data
        int row = e * INDEX_STRIDE;
        rows.seekToRow(row);
        assertTrue(rows.nextBatch(batch), "entry " + e);
        assertEquals(1024, batch.size, "entry " + e);
        assertTrue(col.noNulls, "entry " + e);
        assertEquals("Value for " + (row / 1024), col.toString(0), "entry " + e);
      }
    }
  }

  /**
   * That when we disable dictionaries, we don't get broken row indexes.
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testRowIndex(String dictImpl) throws Exception {
    OrcConf.DICTIONARY_IMPL.setString(conf, dictImpl);
    TypeDescription schema =
        TypeDescription.fromString("struct<str:string>");
    // turn off the dictionaries
    OrcConf.DICTIONARY_KEY_SIZE_THRESHOLD.setDouble(conf, 0);
    Writer writer = OrcFile.createWriter(
        testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema).rowIndexStride(4 * 1024));

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
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf).filesystem(fs));
    SearchArgument sarg = SearchArgumentFactory.newBuilder(conf)
        .lessThan("str", PredicateLeaf.Type.STRING, "row 001000")
        .build();
    Reader.Options options = reader.options().searchArgument(sarg, null).allowSARGToFilter(false);
    RecordReader recordReader = reader.rows(options);
    batch = reader.getSchema().createRowBatch();
    strVector = (BytesColumnVector) batch.cols[0];
    long base = 0;
    while (recordReader.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        String value = String.format("row %06d", r + base);
        assertEquals(value, strVector.toString(r), "row " + (r + base));
      }
      base += batch.size;
    }
    // We should only read the first row group.
    assertEquals(4 * 1024, base);
  }

  /**
   * Test that files written before ORC-569 are read correctly.
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testRowIndexPreORC569(String dictImpl) throws Exception {
    OrcConf.DICTIONARY_IMPL.setString(conf, dictImpl);
    testFilePath = new Path(System.getProperty("example.dir"), "TestStringDictionary.testRowIndex.orc");
    SearchArgument sarg = SearchArgumentFactory.newBuilder(conf)
        .lessThan("str", PredicateLeaf.Type.STRING, "row 001000")
        .build();
    try (Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf).filesystem(fs))) {
      Reader.Options options = reader.options().searchArgument(sarg, null).allowSARGToFilter(false);
      try (RecordReader recordReader = reader.rows(options)) {
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        BytesColumnVector strVector = (BytesColumnVector) batch.cols[0];
        long base = 0;
        while (recordReader.nextBatch(batch)) {
          for (int r = 0; r < batch.size; ++r) {
            String value = String.format("row %06d", r + base);
            assertEquals(value, strVector.toString(r), "row " + (r + base));
          }
          base += batch.size;
        }
        // We should only read the first row group.
        assertEquals(4 * 1024, base);
      }

      try (RecordReader recordReader = reader.rows()) {
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        recordReader.seekToRow(4 * 1024);
        assertTrue(recordReader.nextBatch(batch));
        recordReader.seekToRow(0);
        assertTrue(recordReader.nextBatch(batch));
      }
    }
  }
}
