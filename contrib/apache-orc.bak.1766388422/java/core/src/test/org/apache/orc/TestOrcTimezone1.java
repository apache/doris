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
import java.util.TimeZone;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
public class TestOrcTimezone1 {
  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));
  Configuration conf;
  FileSystem fs;
  Path testFilePath;
  static TimeZone defaultTimeZone = TimeZone.getDefault();

  private static Stream<Arguments> data() {
    return Stream.of(/* Extreme timezones */
        Arguments.of("GMT-12:00", "GMT+14:00"),
        /* No difference in DST */
        Arguments.of("America/Los_Angeles", "America/Los_Angeles"), /* same timezone both with DST */
        Arguments.of("Europe/Berlin", "Europe/Berlin"), /* same as above but europe */
        Arguments.of("America/Phoenix", "Asia/Kolkata") /* Writer no DST, Reader no DST */,
        Arguments.of("Europe/Berlin", "America/Los_Angeles") /* Writer DST, Reader DST */,
        Arguments.of("Europe/Berlin", "America/Chicago") /* Writer DST, Reader DST */,
        /* With DST difference */
        Arguments.of("Europe/Berlin", "UTC"),
        Arguments.of("UTC", "Europe/Berlin") /* Writer no DST, Reader DST */,
        Arguments.of("America/Los_Angeles", "Asia/Kolkata") /* Writer DST, Reader no DST */,
        Arguments.of("Europe/Berlin", "Asia/Kolkata") /* Writer DST, Reader no DST */,
        /* Timezone offsets for the reader has changed historically */
        Arguments.of("Asia/Saigon", "Pacific/Enderbury"),
        Arguments.of("UTC", "Asia/Jerusalem")

        // NOTE:
        // "1995-01-01 03:00:00.688888888" this is not a valid time in Pacific/Enderbury timezone.
        // On 1995-01-01 00:00:00 GMT offset moved from -11:00 hr to +13:00 which makes all values
        // on 1995-01-01 invalid. Try this with joda time
        // new MutableDateTime("1995-01-01", DateTimeZone.forTimeZone(readerTimeZone));
    );
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
        OrcFile.writerOptions(conf).setSchema(schema).stripeSize(100000)
            .bufferSize(10000));
    assertEquals(writerTimeZone, TimeZone.getDefault().getID());
    List<String> ts = Lists.newArrayList();
    ts.add("2003-01-01 01:00:00.000000222");
    ts.add("1996-08-02 09:00:00.723100809");
    ts.add("1999-01-01 02:00:00.999999999");
    ts.add("1995-01-02 03:00:00.688888888");
    ts.add("2002-01-01 04:00:00.1");
    ts.add("2010-03-02 05:00:00.000009001");
    ts.add("2005-01-01 06:00:00.000002229");
    ts.add("2006-01-01 07:00:00.900203003");
    ts.add("2003-01-01 08:00:00.800000007");
    ts.add("1998-11-02 10:00:00.857340643");
    ts.add("2008-10-02 11:00:00.0");
    ts.add("2037-01-01 00:00:00.000999");
    ts.add("2014-03-28 00:00:00.0");
    VectorizedRowBatch batch = schema.createRowBatch();
    TimestampColumnVector times = (TimestampColumnVector) batch.cols[0];
    for (String t : ts) {
      times.set(batch.size++, Timestamp.valueOf(t));
    }
    writer.addRowBatch(batch);
    writer.close();

    TimeZone.setDefault(TimeZone.getTimeZone(readerTimeZone));
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(readerTimeZone, TimeZone.getDefault().getID());
    RecordReader rows = reader.rows();
    batch = reader.getSchema().createRowBatch();
    times = (TimestampColumnVector) batch.cols[0];
    int idx = 0;
    while (rows.nextBatch(batch)) {
      for(int r=0; r < batch.size; ++r) {
        assertEquals(ts.get(idx++), times.asScratchTimestamp(r).toString());
      }
    }
    rows.close();
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testReadTimestampFormat_0_11(String writerTimeZone, String readerTimeZone) throws Exception {
    TimeZone.setDefault(TimeZone.getTimeZone(readerTimeZone));
    Path oldFilePath = new Path(getClass().getClassLoader().
        getSystemResource("orc-file-11-format.orc").getPath());
    Reader reader = OrcFile.createReader(oldFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));
    TypeDescription schema = reader.getSchema();
    int col = schema.getFieldNames().indexOf("ts");
    VectorizedRowBatch batch = schema.createRowBatch(10);
    TimestampColumnVector ts = (TimestampColumnVector) batch.cols[col];

    boolean[] include = new boolean[schema.getMaximumId() + 1];
    include[schema.getChildren().get(col).getId()] = true;
    RecordReader rows = reader.rows
        (reader.options().include(include));
    assertTrue(rows.nextBatch(batch));
    assertEquals(Timestamp.valueOf("2000-03-12 15:00:00"),
        ts.asScratchTimestamp(0));

    // check the contents of second row
    rows.seekToRow(7499);
    assertTrue(rows.nextBatch(batch));
    assertEquals(1, batch.size);
    assertEquals(Timestamp.valueOf("2000-03-12 15:00:01"),
        ts.asScratchTimestamp(0));

    // handle the close up
    assertFalse(rows.nextBatch(batch));
    rows.close();
  }
}
