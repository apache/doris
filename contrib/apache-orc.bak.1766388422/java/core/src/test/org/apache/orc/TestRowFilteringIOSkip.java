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
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.impl.OrcFilterContextImpl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRowFilteringIOSkip {
  private static final Logger LOG = LoggerFactory.getLogger(TestRowFilteringIOSkip.class);
  private static final Path workDir = new Path(System.getProperty("test.tmp.dir",
                                                                  "target" + File.separator + "test"
                                                                  + File.separator + "tmp"));
  private static final Path filePath = new Path(workDir, "skip_file.orc");
  private static Configuration conf;
  private static FileSystem fs;

  private static final TypeDescription schema = TypeDescription.createStruct()
    .addField("f1", TypeDescription.createLong())
    .addField("f2", TypeDescription.createDecimal().withPrecision(20).withScale(6))
    .addField("f3", TypeDescription.createLong())
    .addField("f4", TypeDescription.createString())
    .addField("ridx", TypeDescription.createLong());
  private static final boolean[] FirstColumnOnly = new boolean[] {true, true, false, false, false
    , false};
  private static final long RowCount = 4000000L;
  private static final String[] FilterColumns = new String[] {"f1", "ridx"};
  private static final int scale = 3;

  @BeforeAll
  public static void setup() throws IOException {
    conf = new Configuration();
    fs = FileSystem.get(conf);

    LOG.info("Creating file {} with schema {}", filePath, schema);
    try (Writer writer = OrcFile.createWriter(filePath,
                                              OrcFile.writerOptions(conf)
                                                .fileSystem(fs)
                                                .overwrite(true)
                                                .rowIndexStride(8192)
                                                .setSchema(schema))) {
      Random rnd = new Random(1024);
      VectorizedRowBatch b = schema.createRowBatch();
      for (int rowIdx = 0; rowIdx < RowCount; rowIdx++) {
        long v = rnd.nextLong();
        for (int colIdx = 0; colIdx < schema.getChildren().size() - 1; colIdx++) {
          switch (schema.getChildren().get(colIdx).getCategory()) {
            case LONG:
              ((LongColumnVector) b.cols[colIdx]).vector[b.size] = v;
              break;
            case DECIMAL:
              HiveDecimalWritable d = new HiveDecimalWritable();
              d.setFromLongAndScale(v, scale);
              ((DecimalColumnVector) b.cols[colIdx]).vector[b.size] = d;
              break;
            case STRING:
              ((BytesColumnVector) b.cols[colIdx]).setVal(b.size,
                                                          String.valueOf(v)
                                                            .getBytes(StandardCharsets.UTF_8));
              break;
            default:
              throw new IllegalArgumentException();
          }
        }
        // Populate the rowIdx
        ((LongColumnVector) b.cols[4]).vector[b.size] = rowIdx;

        b.size += 1;
        if (b.size == b.getMaxSize()) {
          writer.addRowBatch(b);
          b.reset();
        }
      }
      if (b.size > 0) {
        writer.addRowBatch(b);
        b.reset();
      }
    }
    LOG.info("Created file {}", filePath);
  }

  @Test
  public void writeIsSuccessful() throws IOException {
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(RowCount, r.getNumberOfRows());
    assertTrue(r.getStripes().size() > 1);
  }

  @Test
  public void readFirstColumn() throws IOException {
    readStart();
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    VectorizedRowBatch b = schema.createRowBatch();
    long rowCount = 0;
    try (RecordReader rr = r.rows(r.options().include(FirstColumnOnly))) {
      while (rr.nextBatch(b)) {
        assertTrue(((LongColumnVector) b.cols[0]).vector[0] != 0);
        rowCount += b.size;
      }
    }
    FileSystem.Statistics stats = readEnd();
    assertEquals(RowCount, rowCount);
    // We should read less than half the length of the file
    assertTrue(stats.getBytesRead() < r.getContentLength() / 2,
        String.format("Bytes read %d is not half of file size %d",
            stats.getBytesRead(),
            r.getContentLength()));
  }

  @Test
  public void readSingleRowWithFilter() throws IOException {
    int cnt = 100;
    Random r = new Random(cnt);
    long ridx;

    while (cnt > 0) {
      ridx = r.nextInt((int) RowCount);
      readSingleRowWithFilter(ridx);
      readSingleRowWithPluginFilter(ridx);
      cnt--;
    }
  }

  private void readSingleRowWithFilter(long idx) throws IOException {
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
      .in("ridx", PredicateLeaf.Type.LONG, idx)
      .build();
    Reader.Options options = r.options()
      .searchArgument(sarg, new String[] {"ridx"})
      .useSelected(true)
      .allowSARGToFilter(true);
    VectorizedRowBatch b = schema.createRowBatch();
    long rowCount = 0;
    try (RecordReader rr = r.rows(options)) {
      assertTrue(rr.nextBatch(b));
      validateBatch(b, idx);
      rowCount += b.size;
      assertFalse(rr.nextBatch(b));
    }
    assertEquals(1, rowCount);
  }

  private void readSingleRowWithPluginFilter(long idx) throws IOException {
    Configuration localConf = new Configuration(conf);
    OrcConf.ALLOW_PLUGIN_FILTER.setBoolean(localConf, true);
    localConf.set("my.filter.name", "my_long_abs_eq");
    localConf.set("my.filter.col.name", "ridx");
    localConf.set("my.filter.col.value", String.valueOf(-idx));
    localConf.set("my.filter.scope", fs.makeQualified(filePath.getParent()) + "/.*");

    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(localConf).filesystem(fs));
    Reader.Options options = r.options()
      .useSelected(true)
      .allowSARGToFilter(true);
    VectorizedRowBatch b = schema.createRowBatch();
    long rowCount = 0;
    try (RecordReader rr = r.rows(options)) {
      assertTrue(rr.nextBatch(b));
      validateBatch(b, idx);
      rowCount += b.size;
      assertFalse(rr.nextBatch(b));
    }
    assertEquals(1, rowCount);
  }

  @Test
  public void readWithoutSelectedSupport() throws IOException {
    // When selected vector is not supported we will read more rows than just the filtered rows.
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    long rowIdx = 12345;
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
      .in("ridx", PredicateLeaf.Type.LONG, rowIdx)
      .build();
    Reader.Options options = r.options()
      .searchArgument(sarg, new String[] {"ridx"})
      .useSelected(false)
      .allowSARGToFilter(true);
    VectorizedRowBatch b = schema.createRowBatch();
    long rowCount = 0;
    HiveDecimalWritable d = new HiveDecimalWritable();
    readStart();
    try (RecordReader rr = r.rows(options)) {
      while (rr.nextBatch(b)) {
        rowCount += b.size;
        for (int i = 0; i < b.size; i++) {
          if (i == b.selected[0]) {
            // All the values are expected to match only for the selected row
            long expValue = ((LongColumnVector) b.cols[0]).vector[i];
            d.setFromLongAndScale(expValue, scale);
            assertEquals(d, ((DecimalColumnVector) b.cols[1]).vector[i]);
            assertEquals(expValue, ((LongColumnVector) b.cols[2]).vector[i]);
            BytesColumnVector sv = (BytesColumnVector) b.cols[3];
            assertEquals(String.valueOf(expValue),
                                sv.toString(i));
            assertEquals(rowIdx, ((LongColumnVector) b.cols[4]).vector[i]);
          }
        }
      }
    }
    double p = readPercentage(readEnd(), fs.getFileStatus(filePath).getLen());
    assertTrue(rowCount > 0 && rowCount <= b.getMaxSize(),
               String.format("RowCount: %s should be between 1 and 1024", rowCount));
    assertTrue(p <= 3, String.format("Read p: %s should be less than 3", p));
  }

  @Test
  public void readWithSArg() throws IOException {
    readStart();
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
      .in("f1", PredicateLeaf.Type.LONG, 0L)
      .build();
    Reader.Options options = r.options()
      .allowSARGToFilter(false)
      .useSelected(true)
      .searchArgument(sarg, new String[] {"f1"});
    VectorizedRowBatch b = schema.createRowBatch();
    long rowCount;
    try (RecordReader rr = r.rows(options)) {
      rowCount = validateFilteredRecordReader(rr, b);
    }
    double p = readPercentage(readEnd(), fs.getFileStatus(filePath).getLen());
    assertEquals(RowCount, rowCount);
    assertTrue(p >= 100);
  }

  @Test
  public void readWithSArgAsFilter() throws IOException {
    readStart();
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
      .in("f1", PredicateLeaf.Type.LONG, 0L)
      .build();
    Reader.Options options = r.options()
      .searchArgument(sarg, new String[] {"f1"})
      .useSelected(true)
      .allowSARGToFilter(true);
    VectorizedRowBatch b = schema.createRowBatch();
    long rowCount;
    try (RecordReader rr = r.rows(options)) {
      rowCount = validateFilteredRecordReader(rr, b);
    }
    double p = readPercentage(readEnd(), fs.getFileStatus(filePath).getLen());
    assertEquals(0, rowCount);
    assertTrue(p < 30);
  }

  @Test
  public void readWithInvalidSArgAs() throws IOException {
    readStart();
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
      .startNot()
      .isNull("f1", PredicateLeaf.Type.LONG)
      .end()
      .build();
    Reader.Options options = r.options()
      .searchArgument(sarg, new String[] {"f1"})
      .useSelected(true)
      .allowSARGToFilter(true);
    VectorizedRowBatch b = schema.createRowBatch();
    long rowCount;
    try (RecordReader rr = r.rows(options)) {
      rowCount = validateFilteredRecordReader(rr, b);
    }
    double p = readPercentage(readEnd(), fs.getFileStatus(filePath).getLen());
    assertEquals(RowCount, rowCount);
    assertTrue(p > 100);
  }

  private long validateFilteredRecordReader(RecordReader rr, VectorizedRowBatch b)
    throws IOException {
    long rowCount = 0;
    while (rr.nextBatch(b)) {
      validateBatch(b, -1);
      rowCount += b.size;
    }
    return rowCount;
  }

  private void validateBatch(VectorizedRowBatch b, long expRowNum) {
    HiveDecimalWritable d = new HiveDecimalWritable();

    for (int i = 0; i < b.size; i++) {
      int rowIdx;
      if (b.selectedInUse) {
        rowIdx = b.selected[i];
      } else {
        rowIdx = i;
      }
      long expValue = ((LongColumnVector) b.cols[0]).vector[rowIdx];
      d.setFromLongAndScale(expValue, scale);
      assertEquals(d, ((DecimalColumnVector) b.cols[1]).vector[rowIdx]);
      assertEquals(expValue, ((LongColumnVector) b.cols[2]).vector[rowIdx]);
      BytesColumnVector sv = (BytesColumnVector) b.cols[3];
      assertEquals(String.valueOf(expValue),
                          sv.toString(rowIdx));
      if (expRowNum != -1) {
        assertEquals(expRowNum + i, ((LongColumnVector) b.cols[4]).vector[rowIdx]);
      }
    }
  }

  @Test
  public void filterAllRowsWithFilter() throws IOException {
    readStart();
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    r.options();
    filterAllRows(r,
                  r.options()
                    .useSelected(true)
                    .setRowFilter(FilterColumns,
                                  new InFilter(new HashSet<>(0), 0)));
  }

  @Test
  public void filterAllRowsWPluginFilter() throws IOException {
    readStart();
    Configuration localConf = new Configuration(conf);
    OrcConf.ALLOW_PLUGIN_FILTER.setBoolean(localConf, true);
    localConf.set("my.filter.name", "my_long_abs_eq");
    localConf.set("my.filter.col.name", "f1");
    localConf.set("my.filter.col.value", String.valueOf(Long.MIN_VALUE));
    localConf.set("my.filter.scope", fs.makeQualified(filePath.getParent()) + "/.*");
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(localConf).filesystem(fs));
    filterAllRows(r, r.options());
  }

  private void filterAllRows(Reader r, Reader.Options options) throws IOException {
    VectorizedRowBatch b = schema.createRowBatch();
    long rowCount = 0;
    try (RecordReader rr = r.rows(options)) {
      while (rr.nextBatch(b)) {
        assertTrue(((LongColumnVector) b.cols[0]).vector[0] != 0);
        assertTrue(((LongColumnVector) b.cols[0]).vector[0] != 0);
        rowCount += b.size;
      }
    }
    FileSystem.Statistics stats = readEnd();
    assertEquals(0, rowCount);
    // We should read less than half the length of the file
    double readPercentage = readPercentage(stats, fs.getFileStatus(filePath).getLen());
    assertTrue(readPercentage < 50,
        String.format("Bytes read %.2f%% should be less than 50%%", readPercentage));
  }

  @Test
  public void readEverything() throws IOException {
    readStart();
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    VectorizedRowBatch b = schema.createRowBatch();
    long rowCount;
    try (RecordReader rr = r.rows(r.options().useSelected(true))) {
      rowCount = validateFilteredRecordReader(rr, b);
    }
    double p = readPercentage(readEnd(), fs.getFileStatus(filePath).getLen());
    assertEquals(RowCount, rowCount);
    assertTrue(p >= 100);
  }

  private double readPercentage(FileSystem.Statistics stats, long fileSize) {
    double p = stats.getBytesRead() * 100.0 / fileSize;
    LOG.info(String.format("%nFileSize: %d%nReadSize: %d%nRead %%: %.2f",
                           fileSize,
                           stats.getBytesRead(),
                           p));
    return p;
  }

  @Test
  public void readEverythingWithFilter() throws IOException {
    readStart();
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    VectorizedRowBatch b = schema.createRowBatch();
    long rowCount;
    try (RecordReader rr = r.rows(r.options()
                                    .useSelected(true)
                                    .setRowFilter(FilterColumns, new AllowAllFilter()))) {
      rowCount = validateFilteredRecordReader(rr, b);
    }
    double p = readPercentage(readEnd(), fs.getFileStatus(filePath).getLen());
    assertEquals(RowCount, rowCount);
    assertTrue(p >= 100);
  }

  @Test
  public void filterAlternateBatches() throws IOException {
    readStart();
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    VectorizedRowBatch b = schema.createRowBatch();
    Reader.Options options = r.options()
      .useSelected(true)
      .setRowFilter(FilterColumns, new AlternateFilter());
    long rowCount;
    try (RecordReader rr = r.rows(options)) {
      rowCount = validateFilteredRecordReader(rr, b);
    }
    FileSystem.Statistics stats = readEnd();
    double readPercentage = readPercentage(stats, fs.getFileStatus(filePath).getLen());
    assertTrue(readPercentage > 100);
    assertTrue(RowCount > rowCount);
  }

  @Test
  public void filterWithSeek() throws IOException {
    readStart();
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    VectorizedRowBatch b = schema.createRowBatch();
    Reader.Options options = r.options()
      .useSelected(true)
      .setRowFilter(FilterColumns, new AlternateFilter());
    long seekRow;
    try (RecordReader rr = r.rows(options)) {
      // Validate the first batch
      assertTrue(rr.nextBatch(b));
      validateBatch(b, 0);
      assertEquals(b.size, rr.getRowNumber());

      // Read the next batch, will skip a batch that is filtered
      assertTrue(rr.nextBatch(b));
      validateBatch(b, 2048);
      assertEquals(2048 + 1024, rr.getRowNumber());

      // Seek forward
      seekToRow(rr, b, 4096);

      // Seek back to the filtered batch
      long bytesRead = readEnd().getBytesRead();
      seekToRow(rr, b, 1024);
      // No IO should have taken place
      assertEquals(bytesRead, readEnd().getBytesRead());

      // Seek forward to next row group, where the first batch is not filtered
      seekToRow(rr, b, 8192);

      // Seek forward to next row group but position on filtered batch
      seekToRow(rr, b, (8192 * 2) + 1024);

      // Seek forward to next stripe
      seekRow = r.getStripes().get(0).getNumberOfRows();
      seekToRow(rr, b, seekRow);

      // Seek back to previous stripe, filtered row, it should require more IO as a result of
      // stripe change
      bytesRead = readEnd().getBytesRead();
      seekToRow(rr, b, 1024);
      assertTrue(readEnd().getBytesRead() > bytesRead,
          "Change of stripe should require more IO");
    }
    FileSystem.Statistics stats = readEnd();
    double readPercentage = readPercentage(stats, fs.getFileStatus(filePath).getLen());
    assertTrue(readPercentage > 130);
  }

  @Test
  public void readFewRGWithSArg() throws IOException {
    readStart();
    Reader r = OrcFile.createReader(filePath,
                                    OrcFile.readerOptions(conf).filesystem(fs));
    VectorizedRowBatch b = schema.createRowBatch();
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
      .in("ridx", PredicateLeaf.Type.LONG, 0L, 1000000L, 2000000L, 3000000L)
      .build();
    Reader.Options options = r.options()
      .allowSARGToFilter(false)
      .useSelected(true)
      .searchArgument(sarg, new String[] {"ridx"});

    long rowCount;
    try (RecordReader rr = r.rows(options)) {
      rowCount = validateFilteredRecordReader(rr, b);
    }
    assertEquals(8192 * 4, rowCount);
    FileSystem.Statistics stats = readEnd();
    double readPercentage = readPercentage(stats, fs.getFileStatus(filePath).getLen());
    assertTrue(readPercentage < 10);
  }

  @Test
  public void readFewRGWithSArgAndFilter() throws IOException {
    readStart();
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    VectorizedRowBatch b = schema.createRowBatch();
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
      .in("ridx", PredicateLeaf.Type.LONG, 0L, 1000000L, 2000000L, 3000000L)
      .build();
    Reader.Options options = r.options()
      .searchArgument(sarg, new String[] {"ridx"})
      .useSelected(true)
      .allowSARGToFilter(true);

    long rowCount;
    try (RecordReader rr = r.rows(options)) {
      rowCount = validateFilteredRecordReader(rr, b);
    }
    assertEquals(4, rowCount);
    FileSystem.Statistics stats = readEnd();
    double readPercentage = readPercentage(stats, fs.getFileStatus(filePath).getLen());
    assertTrue(readPercentage < 10);
  }

  @Test
  public void schemaEvolutionMissingFilterColumn() throws IOException {
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    TypeDescription readSchema = schema
      .clone()
      .addField("missing", TypeDescription.createLong());
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
      .startNot()
      .isNull("missing", PredicateLeaf.Type.LONG)
      .end()
      .build();
    Reader.Options options = r.options()
      .schema(readSchema)
      .searchArgument(sarg, new String[] {"missing"})
      .useSelected(true)
      .allowSARGToFilter(true);
    VectorizedRowBatch b = readSchema.createRowBatch();
    long rowCount = 0;
    try (RecordReader rr = r.rows(options)) {
      assertFalse(rr.nextBatch(b));
    }
    assertEquals(0, rowCount);
  }

  @Test
  public void schemaEvolutionLong2StringColumn() throws IOException {
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    // Change ridx column from long to string and swap the positions of ridx and f4 columns
    TypeDescription readSchema = TypeDescription.createStruct()
      .addField("f1", TypeDescription.createLong())
      .addField("f2", TypeDescription.createDecimal().withPrecision(20).withScale(6))
      .addField("f3", TypeDescription.createLong())
      .addField("ridx", TypeDescription.createString())
      .addField("f4", TypeDescription.createString());
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
      .in("ridx", PredicateLeaf.Type.STRING, "1")
      .build();
    Reader.Options options = r.options()
      .schema(readSchema)
      .searchArgument(sarg, new String[] {"ridx"})
      .useSelected(true)
      .allowSARGToFilter(true);
    VectorizedRowBatch b = readSchema.createRowBatch();
    long rowCount = 0;
    try (RecordReader rr = r.rows(options)) {
      assertTrue(rr.nextBatch(b));
      assertEquals(1, b.size);
      rowCount += b.size;
      HiveDecimalWritable d = new HiveDecimalWritable();
      int rowIdx = 1;
      long expValue = ((LongColumnVector) b.cols[0]).vector[rowIdx];
      d.setFromLongAndScale(expValue, scale);
      assertEquals(d, ((DecimalColumnVector) b.cols[1]).vector[rowIdx]);
      assertEquals(expValue, ((LongColumnVector) b.cols[2]).vector[rowIdx]);
      // The columns ridx and f4 are swapped, which is reflected in the updated index value
      BytesColumnVector sv = (BytesColumnVector) b.cols[4];
      assertEquals(String.valueOf(expValue),
                          sv.toString(rowIdx));
      sv = (BytesColumnVector) b.cols[3];
      assertEquals(String.valueOf(rowIdx),
                          sv.toString(rowIdx));

      assertFalse(rr.nextBatch(b));
    }

    assertEquals(1, rowCount);
  }

  @Test
  public void readWithCaseSensitivityOff() throws IOException {
    // Use the ridx column input in UpperCase and flag case-sensitivity off
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
      .in("RIDX", PredicateLeaf.Type.LONG, 1L)
      .build();
    Reader.Options options = r.options()
      .searchArgument(sarg, new String[] {"RIDX"})
      .useSelected(true)
      .allowSARGToFilter(true)
      .isSchemaEvolutionCaseAware(false);
    VectorizedRowBatch b = schema.createRowBatch();
    long rowCount = 0;
    try (RecordReader rr = r.rows(options)) {
      assertTrue(rr.nextBatch(b));
      validateBatch(b, 1L);
      rowCount += b.size;
      assertFalse(rr.nextBatch(b));
    }
    assertEquals(1, rowCount);
  }

  @Test
  public void readFailureWithCaseSensitivityOn() throws IOException {
    // Use the ridx column input in UpperCase and flag case-sensitivity off
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    SearchArgument sarg = SearchArgumentFactory.newBuilder()
      .in("RIDX", PredicateLeaf.Type.LONG, 1L)
      .build();
    Reader.Options options = r.options()
      .searchArgument(sarg, new String[] {"RIDX"})
      .useSelected(true)
      .allowSARGToFilter(true)
      .isSchemaEvolutionCaseAware(true);
    assertThrows(IllegalArgumentException.class,
                 () -> r.rows(options),
                 "Field RIDX not found in struct<f1:bigint,f2:decimal(20,6),f3:bigint,"
                 + "f4:string,ridx:bigint>");

  }

  private void seekToRow(RecordReader rr, VectorizedRowBatch b, long row) throws IOException {
    rr.seekToRow(row);
    assertTrue(rr.nextBatch(b));
    long expRowNum;
    if ((row / b.getMaxSize()) % 2 == 0) {
      expRowNum = row;
    } else {
      // As the seek batch gets filtered
      expRowNum = row + b.getMaxSize();
    }
    validateBatch(b, expRowNum);
    assertEquals(expRowNum + b.getMaxSize(), rr.getRowNumber());
  }

  private static class InFilter implements Consumer<OrcFilterContext> {
    private final Set<Long> ids;
    private final int colIdx;

    private InFilter(Set<Long> ids, int colIdx) {
      this.ids = ids;
      this.colIdx = colIdx;
    }

    @Override
    public void accept(OrcFilterContext b) {
      int newSize = 0;
      for (int i = 0; i < b.getSelectedSize(); i++) {
        if (ids.contains(getValue(b, i))) {
          b.getSelected()[newSize] = i;
          newSize += 1;
        }
      }
      b.setSelectedInUse(true);
      b.setSelectedSize(newSize);
    }

    private Long getValue(OrcFilterContext b, int rowIdx) {
      LongColumnVector v = ((LongColumnVector) ((OrcFilterContextImpl) b).getCols()[colIdx]);
      int valIdx = rowIdx;
      if (v.isRepeating) {
        valIdx = 0;
      }
      if (!v.noNulls && v.isNull[valIdx]) {
        return null;
      } else {
        return v.vector[valIdx];
      }
    }
  }

  /**
   * Fill odd batches values in a default read
   * if ridx(rowIdx) / 1024 is even then allow otherwise fail
   */
  private static class AlternateFilter implements Consumer<OrcFilterContext> {
    @Override
    public void accept(OrcFilterContext b) {
      LongColumnVector v = (LongColumnVector) ((OrcFilterContextImpl) b).getCols()[4];
      if ((v.vector[0] / 1024) % 2 == 1) {
        b.setSelectedInUse(true);
        b.setSelectedSize(0);
      }
    }
  }

  private static class AllowAllFilter implements Consumer<OrcFilterContext> {
    @Override
    public void accept(OrcFilterContext batch) {
      // do nothing every row is allowed
    }
  }

  private static void readStart() {
    FileSystem.clearStatistics();
  }

  private static FileSystem.Statistics readEnd() {
    return FileSystem.getAllStatistics().get(0);
  }
}
