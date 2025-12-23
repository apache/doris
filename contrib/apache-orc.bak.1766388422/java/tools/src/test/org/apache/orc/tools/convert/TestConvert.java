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

package org.apache.orc.tools.convert;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestConvert {

  public static final TimeZone DEFAULT_TIME_ZONE = TimeZone.getDefault();

  Path workDir = new Path(System.getProperty("test.tmp.dir"));
  Configuration conf;
  FileSystem fs;
  Path testFilePath;

  @BeforeEach
  public void openFileSystem () throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    fs.setWorkingDirectory(workDir);
    testFilePath = new Path("TestConvert.testConvert.orc");
    fs.delete(testFilePath, false);
  }

  @BeforeAll
  public static void changeDefaultTimeZone() {
    TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));
  }

  @AfterAll
  public static void resetDefaultTimeZone() {
    TimeZone.setDefault(DEFAULT_TIME_ZONE);
  }

  @Test
  public void testConvertCustomTimestampFromCsv() throws IOException, ParseException {
    Path csvFile = new Path("test.csv");
    FSDataOutputStream stream = fs.create(csvFile, true);
    String[] timeValues = new String[] {"0001-01-01 00:00:00.000", "2021-12-01 18:36:00.800"};
    stream.writeBytes(String.join("\n", timeValues));
    stream.close();
    String schema = "struct<d:timestamp>";
    String timestampFormat = "yyyy-MM-dd HH:mm:ss.SSS";
    TypeDescription readSchema = TypeDescription.fromString(schema);

    ConvertTool.main(conf, new String[]{"--schema", schema, "-o", testFilePath.toString(),
        "-t", timestampFormat, csvFile.toString()});

    assertTrue(fs.exists(testFilePath));

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
    VectorizedRowBatch batch = readSchema.createRowBatch();
    RecordReader rowIterator = reader.rows(reader.options().schema(readSchema));
    TimestampColumnVector tcv = (TimestampColumnVector) batch.cols[0];

    while (rowIterator.nextBatch(batch)) {
      for (int row = 0; row < batch.size; ++row) {
        Timestamp timestamp = Timestamp.valueOf(timeValues[row]);
        assertEquals(timestamp.getTime(), tcv.time[row]);
        assertEquals(timestamp.getNanos(), tcv.nanos[row]);
      }
    }
    rowIterator.close();
  }

}
