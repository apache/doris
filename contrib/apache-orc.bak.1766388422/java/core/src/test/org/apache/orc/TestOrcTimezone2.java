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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.sql.Timestamp;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 *
 */
public class TestOrcTimezone2 {
  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));
  Configuration conf;
  FileSystem fs;
  Path testFilePath;
  static TimeZone defaultTimeZone = TimeZone.getDefault();

  private static Stream<Arguments> data() {
    String[] allTimeZones = TimeZone.getAvailableIDs();
    Random rand = new Random(123);
    int len = allTimeZones.length;
    int n = 500;
    Arguments[] data = new Arguments[n];
    for (int i = 0; i < n; i++) {
      int wIdx = rand.nextInt(len);
      int rIdx = rand.nextInt(len);
      data[i] = Arguments.of(allTimeZones[wIdx], allTimeZones[rIdx]);
    }
    return Stream.of(data);
  }

  @BeforeEach
  public void openFileSystem(TestInfo testInfo) throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestOrcFile." +
        testInfo.getTestMethod().get().getName() + ".orc");
    fs.delete(testFilePath, false);
  }

  @AfterEach
  public void restoreTimeZone() {
    TimeZone.setDefault(defaultTimeZone);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testTimestampWriter(String writerTimeZone, String readerTimeZone) throws Exception {
    TypeDescription schema = TypeDescription.createTimestamp();

    TimeZone.setDefault(TimeZone.getTimeZone(writerTimeZone));
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema)
            .stripeSize(100000).bufferSize(10000));
    assertEquals(writerTimeZone, TimeZone.getDefault().getID());
    List<String> ts = Lists.newArrayList();
    ts.add("2003-01-01 01:00:00.000000222");
    ts.add("1999-01-01 02:00:00.999999999");
    ts.add("1995-01-02 03:00:00.688888888");
    ts.add("2002-01-01 04:00:00.1");
    ts.add("2010-03-02 05:00:00.000009001");
    ts.add("2005-01-01 06:00:00.000002229");
    ts.add("2006-01-01 07:00:00.900203003");
    ts.add("2003-01-01 08:00:00.800000007");
    ts.add("1996-08-02 09:00:00.723100809");
    ts.add("1998-11-02 10:00:00.857340643");
    ts.add("2008-10-02 11:00:00.0");
    ts.add("2037-01-01 00:00:00.000999");
    VectorizedRowBatch batch = schema.createRowBatch();
    TimestampColumnVector tsc = (TimestampColumnVector) batch.cols[0];
    for (String t : ts) {
      tsc.set(batch.size++, Timestamp.valueOf(t));
    }
    writer.addRowBatch(batch);
    writer.close();

    TimeZone.setDefault(TimeZone.getTimeZone(readerTimeZone));
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(readerTimeZone, TimeZone.getDefault().getID());
    RecordReader rows = reader.rows();
    int idx = 0;
    batch = reader.getSchema().createRowBatch();
    tsc = (TimestampColumnVector) batch.cols[0];
    while (rows.nextBatch(batch)) {
      for (int r=0; r < batch.size; ++r) {
        assertEquals(ts.get(idx++), tsc.asScratchTimestamp(r).toString());
      }
    }
    rows.close();
  }
}
