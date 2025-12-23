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
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test over an orc file that does not store time zone information in the footer
 * and it was written from a time zone that does not observe DST.
 */
public class TestOrcNoTimezone {
  Configuration conf;
  FileSystem fs;
  SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
  static TimeZone defaultTimeZone = TimeZone.getDefault();

  @BeforeEach
  public void openFileSystem() throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
  }

  @AfterEach
  public void restoreTimeZone() {
    TimeZone.setDefault(defaultTimeZone);
  }

  @ParameterizedTest
  @ValueSource(strings = {"GMT-12:00", "UTC", "GMT+8:00"})
  public void testReadOldTimestampFormat(String readerTimeZone) throws Exception {
    TimeZone.setDefault(TimeZone.getTimeZone(readerTimeZone));
    Path oldFilePath = new Path(getClass().getClassLoader().
        getSystemResource("orc-file-no-timezone.orc").getPath());
    Reader reader = OrcFile.createReader(oldFilePath,
        OrcFile.readerOptions(conf).filesystem(fs).useUTCTimestamp(true));
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    TypeDescription schema = reader.getSchema();
    VectorizedRowBatch batch = schema.createRowBatch(10);
    TimestampColumnVector ts = (TimestampColumnVector) batch.cols[0];

    boolean[] include = new boolean[schema.getMaximumId() + 1];
    include[schema.getChildren().get(0).getId()] = true;
    RecordReader rows = reader.rows
        (reader.options().include(include));
    assertTrue(rows.nextBatch(batch));
    Timestamp timestamp = ts.asScratchTimestamp(0);
    assertEquals(
        Timestamp.valueOf("2014-01-01 12:34:56.0").toString(),
        formatter.format(timestamp),
        "For timezone : " + TimeZone.getTimeZone(readerTimeZone));

    // check the contents of second row
    rows.seekToRow(1);
    assertTrue(rows.nextBatch(batch));
    assertEquals(1, batch.size);
    timestamp = ts.asScratchTimestamp(0);
    assertEquals(
        Timestamp.valueOf("2014-06-06 12:34:56.0").toString(),
        formatter.format(timestamp),
        "For timezone : " + TimeZone.getTimeZone(readerTimeZone));

    // handle the close up
    assertFalse(rows.nextBatch(batch));
    rows.close();
  }
}
