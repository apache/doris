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
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentImpl;
import org.apache.orc.impl.RecordReaderImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestOrcTimestampPPD {
  Path workDir =
      new Path(System.getProperty("test.tmp.dir", "target" + File.separator + "test" + File.separator + "tmp"));
  Configuration conf;
  FileSystem fs;
  Path testFilePath;
  static TimeZone defaultTimeZone = TimeZone.getDefault();

  public TestOrcTimestampPPD() {
  }

  @BeforeEach
  public void openFileSystem(TestInfo testInfo) throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir,
        "TestOrcTimestampPPD." + testInfo.getTestMethod().get().getName() + ".orc");
    fs.delete(testFilePath, false);
  }

  @AfterEach
  public void restoreTimeZone() {
    TimeZone.setDefault(defaultTimeZone);
  }

  public static PredicateLeaf createPredicateLeaf(PredicateLeaf.Operator operator,
      PredicateLeaf.Type type,
      String columnName,
      Object literal,
      List<Object> literalList) {
    return new SearchArgumentImpl.PredicateLeafImpl(operator, type, columnName,
        literal, literalList);
  }

  @Test
  // ORC-611 : PPD evaluation with min-max stats for sub-millisecond timestamps
  public void testSubMsTimestampWriterStats() throws Exception {
    TypeDescription schema = TypeDescription.createTimestamp();
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000).bufferSize(10000)
            .version(OrcFile.Version.CURRENT));

    List<Timestamp> tslist = Lists.newArrayList();
    tslist.add(Timestamp.valueOf("1970-01-01 00:00:00.0005"));

    VectorizedRowBatch batch = schema.createRowBatch();
    TimestampColumnVector times = (TimestampColumnVector) batch.cols[0];
    for (Timestamp t : tslist) {
      times.set(batch.size++, t);
    }
    times.isRepeating = true;
    writer.addRowBatch(batch);
    // Done writing to file
    writer.close();

    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
    // Now reading
    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf).filesystem(fs));

    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    times = (TimestampColumnVector) batch.cols[0];
    while (rows.nextBatch(batch)) {
      for (int r = 0; r < batch.size; ++r) {
        assertEquals(tslist.get(0), times.asScratchTimestamp(r));
        assertEquals(tslist.get(0).getNanos(), times.asScratchTimestamp(r).getNanos());
      }
    }
    rows.close();
    ColumnStatistics[] colStats = reader.getStatistics();
    Timestamp gotMin = ((TimestampColumnStatistics) colStats[0]).getMinimum();
    assertEquals("1970-01-01 00:00:00.0005", gotMin.toString());

    Timestamp gotMax = ((TimestampColumnStatistics) colStats[0]).getMaximum();
    assertEquals("1970-01-01 00:00:00.0005", gotMax.toString());

    PredicateLeaf pred = createPredicateLeaf(PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.TIMESTAMP, "c",
        Timestamp.valueOf("1970-01-01 00:00:00.0005"), null);
    // Make sure PPD is now passing
    assertEquals(SearchArgument.TruthValue.YES, RecordReaderImpl.evaluatePredicate(colStats[0], pred, null));

    pred = createPredicateLeaf(PredicateLeaf.Operator.LESS_THAN_EQUALS, PredicateLeaf.Type.TIMESTAMP, "c",
        Timestamp.valueOf("1970-01-01 00:00:00.0005"), null);
    assertEquals(SearchArgument.TruthValue.YES, RecordReaderImpl.evaluatePredicate(colStats[0], pred, null));

    pred = createPredicateLeaf(PredicateLeaf.Operator.LESS_THAN, PredicateLeaf.Type.TIMESTAMP, "c",
        Timestamp.valueOf("1970-01-01 00:00:00.0005"), null);
    assertEquals(SearchArgument.TruthValue.NO, RecordReaderImpl.evaluatePredicate(colStats[0], pred, null));
  }

  @Test
  public void testSubMsComplexStats() throws IOException {
    TypeDescription schema = TypeDescription.createTimestamp();
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000).bufferSize(10000)
            .version(OrcFile.Version.CURRENT));

    List<Timestamp> tslist = Lists.newArrayList();
    tslist.add(Timestamp.valueOf("2037-01-01 00:00:00.001109"));
    tslist.add(Timestamp.valueOf("2037-01-01 00:00:00.001279"));
    tslist.add(Timestamp.valueOf("2037-01-01 00:00:00.001499"));
    tslist.add(Timestamp.valueOf("2037-01-01 00:00:00.0067891"));
    tslist.add(Timestamp.valueOf("2037-01-01 00:00:00.005199"));
    tslist.add(Timestamp.valueOf("2037-01-01 00:00:00.006789"));

    VectorizedRowBatch batch = schema.createRowBatch();
    TimestampColumnVector times = (TimestampColumnVector) batch.cols[0];
    for (Timestamp ts: tslist) {
      times.set(batch.size++, ts);
    }
    times.isRepeating = false;
    writer.addRowBatch(batch);
    // Done writing to file
    writer.close();

    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));
    // Now reading
    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf).filesystem(fs));

    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    times = (TimestampColumnVector) batch.cols[0];
    while (rows.nextBatch(batch)) {
      for (int r = 0; r < batch.size; ++r) {
        assertEquals(tslist.get(r), times.asScratchTimestamp(r));
        assertEquals(tslist.get(r).getNanos(), times.asScratchTimestamp(r).getNanos());
      }
    }
    rows.close();
    ColumnStatistics[] colStats = reader.getStatistics();
    Timestamp gotMin = ((TimestampColumnStatistics) colStats[0]).getMinimum();
    assertEquals("2037-01-01 00:00:00.001109", gotMin.toString());

    Timestamp gotMax = ((TimestampColumnStatistics) colStats[0]).getMaximum();
    assertEquals("2037-01-01 00:00:00.0067891", gotMax.toString());

    // PPD EQUALS with nano precision passing
    PredicateLeaf pred = createPredicateLeaf(PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.TIMESTAMP, "c",
        Timestamp.valueOf("2037-01-01 00:00:00.001109"), null);
    assertEquals(SearchArgument.TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(colStats[0], pred, null));

    // PPD EQUALS with ms precision NOT passing
    pred = createPredicateLeaf(PredicateLeaf.Operator.EQUALS, PredicateLeaf.Type.TIMESTAMP, "c",
        Timestamp.valueOf("2037-01-01 00:00:001"), null);
    assertEquals(SearchArgument.TruthValue.NO, RecordReaderImpl.evaluatePredicate(colStats[0], pred, null));

    // PPD LESS_THAN with ns precision passing
    pred = createPredicateLeaf(PredicateLeaf.Operator.LESS_THAN, PredicateLeaf.Type.TIMESTAMP, "c",
        Timestamp.valueOf("2037-01-01 00:00:00.006789"), null);
    assertEquals(SearchArgument.TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(colStats[0], pred, null));

    // PPD LESS_THAN with ms precision passing
    pred = createPredicateLeaf(PredicateLeaf.Operator.LESS_THAN, PredicateLeaf.Type.TIMESTAMP, "c",
        Timestamp.valueOf("2037-01-01 00:00:00.002"), null);
    assertEquals(SearchArgument.TruthValue.YES_NO, RecordReaderImpl.evaluatePredicate(colStats[0], pred, null));
  }
}

