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

package org.apache.orc.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.orc.OrcConf;
import org.apache.orc.mapred.OrcStruct;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import static org.apache.orc.mapreduce.FilterTestUtil.RowCount;
import static org.apache.orc.mapreduce.FilterTestUtil.validateLimitedRow;
import static org.apache.orc.mapreduce.FilterTestUtil.validateRow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class TestMapReduceFiltering {
  private static final Path workDir = new Path(System.getProperty("test.tmp.dir",
                                                                  "target" + File.separator + "test"
                                                                  + File.separator + "tmp"));

  private static Configuration conf;
  private static FileSystem fs;
  private static final Path filePath = new Path(workDir, "mapreduce_skip_file.orc");

  @BeforeAll
  public static void setup() throws IOException {
    conf = new Configuration();
    fs = FileSystem.get(conf);
    FilterTestUtil.createFile(conf, fs, filePath);
  }

  @Test
  public void readWithSArg() throws IOException, InterruptedException {
    TaskAttemptID id = new TaskAttemptID("jt", 0, TaskType.MAP, 0, 0);
    OrcConf.ALLOW_SARG_TO_FILTER.setBoolean(conf, false);
    OrcConf.INCLUDE_COLUMNS.setString(conf, "0,1,2,3,4");
    OrcInputFormat.setSearchArgument(conf,
                                     SearchArgumentFactory.newBuilder()
                                       .in("f1", PredicateLeaf.Type.LONG, 0L)
                                       .build(),
                                     new String[] {"f1"});
    FileSplit split = new FileSplit(filePath,
                                    0, fs.getFileStatus(filePath).getLen(),
                                    new String[0]);
    TaskAttemptContext attemptContext = new TaskAttemptContextImpl(conf, id);
    FilterTestUtil.readStart();
    org.apache.hadoop.mapreduce.RecordReader<NullWritable, OrcStruct> r =
      new OrcInputFormat<OrcStruct>().createRecordReader(split,
                                                         attemptContext);
    long rowCount = validateFilteredRecordReader(r);
    double p = FilterTestUtil.readPercentage(FilterTestUtil.readEnd(),
                                             fs.getFileStatus(filePath).getLen());
    assertEquals(FilterTestUtil.RowCount, rowCount);
    assertTrue(p >= 100);
  }

  @Test
  public void readWithSArgAsFilter() throws IOException, InterruptedException {
    TaskAttemptID id = new TaskAttemptID("jt", 1, TaskType.MAP, 0, 0);
    OrcConf.ALLOW_SARG_TO_FILTER.setBoolean(conf, true);
    OrcConf.INCLUDE_COLUMNS.setString(conf, "0,1,2,3,4");
    OrcInputFormat.setSearchArgument(conf,
                                     SearchArgumentFactory.newBuilder()
                                       .in("f1", PredicateLeaf.Type.LONG, 0L)
                                       .build(),
                                     new String[] {"f1"});
    FileSplit split = new FileSplit(filePath,
                                    0, fs.getFileStatus(filePath).getLen(),
                                    new String[0]);
    TaskAttemptContext attemptContext = new TaskAttemptContextImpl(conf, id);
    FilterTestUtil.readStart();
    org.apache.hadoop.mapreduce.RecordReader<NullWritable, OrcStruct> r =
      new OrcInputFormat<OrcStruct>().createRecordReader(split,
                                                         attemptContext);
    long rowCount = validateFilteredRecordReader(r);
    double p = FilterTestUtil.readPercentage(FilterTestUtil.readEnd(),
                                             fs.getFileStatus(filePath).getLen());
    assertEquals(0, rowCount);
    assertTrue(p < 30);
  }

  @Test
  public void readSingleRowWFilter() throws IOException, InterruptedException {
    int cnt = 100;
    Random r = new Random(cnt);
    long ridx = 0;

    while (cnt > 0) {
      ridx = r.nextInt((int) RowCount);
      testSingleRowWfilter(ridx);
      cnt -= 1;
    }

  }

  private void testSingleRowWfilter(long idx) throws IOException, InterruptedException {
    TaskAttemptID id = new TaskAttemptID("jt", 1, TaskType.MAP, 0, 0);
    OrcConf.ALLOW_SARG_TO_FILTER.setBoolean(conf, true);
    OrcConf.INCLUDE_COLUMNS.setString(conf, "0,1,2,4");
    OrcInputFormat.setSearchArgument(conf,
                                     SearchArgumentFactory.newBuilder()
                                       .in("ridx", PredicateLeaf.Type.LONG, idx)
                                       .build(),
                                     new String[] {"ridx"});
    FileSplit split = new FileSplit(filePath,
                                    0, fs.getFileStatus(filePath).getLen(),
                                    new String[0]);
    TaskAttemptContext attemptContext = new TaskAttemptContextImpl(conf, id);
    FilterTestUtil.readStart();
    org.apache.hadoop.mapreduce.RecordReader<NullWritable, OrcStruct> r =
      new OrcInputFormat<OrcStruct>().createRecordReader(split,
                                                         attemptContext);
    long rowCount = 0;
    while (r.nextKeyValue()) {
      validateLimitedRow(r.getCurrentValue(), idx);
      rowCount += 1;
    }
    r.close();
    assertEquals(1, rowCount);
  }

  private static long validateFilteredRecordReader(org.apache.hadoop.mapreduce.RecordReader<NullWritable
    , OrcStruct> rr)
    throws IOException, InterruptedException {
    long rowCount = 0;
    while (rr.nextKeyValue()) {
      validateRow(rr.getCurrentValue());
      rowCount += 1;
    }
    return rowCount;
  }
}
