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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 *
 */
public class TestOrcTimezone4 {
  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));
  Configuration conf;
  FileSystem fs;
  Path testFilePath;
  SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
  static TimeZone defaultTimeZone = TimeZone.getDefault();

  public TestOrcTimezone4() {
  }

  @BeforeEach
  public void openFileSystem(TestInfo testInfo) throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestOrcTimezone4." +
        testInfo.getTestMethod().get().getName() + ".orc");
    fs.delete(testFilePath, false);
  }

  @AfterEach
  public void restoreTimeZone() {
    TimeZone.setDefault(defaultTimeZone);
  }

  @Test
  public void testTimestampWriter() throws Exception {
    TypeDescription schema = TypeDescription.createTimestamp();

    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));

    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
            .bufferSize(10000));
    List<String> ts = Lists.newArrayList();
    ts.add("1969-12-31 15:59:56.007");
    ts.add("1969-12-31 16:00:14.007");
    ts.add("1969-12-31 16:00:06.021");
    VectorizedRowBatch batch = schema.createRowBatch();
    TimestampColumnVector times = (TimestampColumnVector) batch.cols[0];
    for (String t : ts) {
      long time = formatter.parse(t).getTime();
      times.set(batch.size++, new Timestamp(time));
    }
    writer.addRowBatch(batch);
    writer.close();

    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs).useUTCTimestamp(true));
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    times = (TimestampColumnVector) batch.cols[0];
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        Timestamp timestamp = times.asScratchTimestamp(r);
        assertEquals(ts.get(idx++), formatter.format(timestamp));
      }
    }
    rows.close();
  }
}
