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

package org.apache.orc.mapred;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestOrcOutputFormat {

  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));
  JobConf conf = new JobConf();
  FileSystem fs;

  {
    try {
      fs =  FileSystem.getLocal(conf).getRaw();
      fs.delete(workDir, true);
      fs.mkdirs(workDir);
    } catch (IOException e) {
      throw new IllegalStateException("bad fs init", e);
    }
  }

  static class NullOutputCommitter extends OutputCommitter {

    @Override
    public void setupJob(JobContext jobContext) {
      // PASS
    }

    @Override
    public void setupTask(TaskAttemptContext taskAttemptContext) {

    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) {
      return false;
    }

    @Override
    public void commitTask(TaskAttemptContext taskAttemptContext) {
      // PASS
    }

    @Override
    public void abortTask(TaskAttemptContext taskAttemptContext) {
      // PASS
    }
  }

  @Test
  public void testAllTypes() throws Exception {
    conf.set("mapreduce.task.attempt.id", "attempt_20160101_0001_m_000001_0");
    conf.setOutputCommitter(NullOutputCommitter.class);
    final String typeStr = "struct<b1:binary,b2:boolean,b3:tinyint," +
        "c:char(10),d1:date,d2:decimal(20,5),d3:double,fff:float,int:int," +
        "l:array<bigint>,map:map<smallint,string>," +
        "str:struct<u:uniontype<timestamp,varchar(100)>>,ts:timestamp>";
    OrcConf.MAPRED_OUTPUT_SCHEMA.setString(conf, typeStr);
    FileOutputFormat.setOutputPath(conf, workDir);
    TypeDescription type = TypeDescription.fromString(typeStr);

    // build a row object
    OrcStruct row = (OrcStruct) OrcStruct.createValue(type);
    ((BytesWritable) row.getFieldValue(0)).set(new byte[]{1,2,3,4}, 0, 4);
    ((BooleanWritable) row.getFieldValue(1)).set(true);
    ((ByteWritable) row.getFieldValue(2)).set((byte) 23);
    ((Text) row.getFieldValue(3)).set("aaabbbcccddd");
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    ((DateWritable) row.getFieldValue(4)).set(DateWritable.millisToDays
        (format.parse("2016-04-01").getTime()));
    ((HiveDecimalWritable) row.getFieldValue(5)).set(new HiveDecimalWritable("1.23"));
    ((DoubleWritable) row.getFieldValue(6)).set(1.5);
    ((FloatWritable) row.getFieldValue(7)).set(4.5f);
    ((IntWritable) row.getFieldValue(8)).set(31415);
    OrcList<LongWritable> longList = (OrcList<LongWritable>) row.getFieldValue(9);
    longList.add(new LongWritable(123));
    longList.add(new LongWritable(456));
    OrcMap<ShortWritable,Text> map = (OrcMap<ShortWritable,Text>) row.getFieldValue(10);
    map.put(new ShortWritable((short) 1000), new Text("aaaa"));
    map.put(new ShortWritable((short) 123), new Text("bbbb"));
    OrcStruct struct = (OrcStruct) row.getFieldValue(11);
    OrcUnion union = (OrcUnion) struct.getFieldValue(0);
    union.set((byte) 1, new Text("abcde"));
    ((OrcTimestamp) row.getFieldValue(12)).set("1996-12-11 15:00:00");
    NullWritable nada = NullWritable.get();
    RecordWriter<NullWritable, OrcStruct> writer =
        new OrcOutputFormat<OrcStruct>().getRecordWriter(fs, conf, "all.orc",
            Reporter.NULL);
    for(int r=0; r < 10; ++r) {
      row.setFieldValue(8, new IntWritable(r * 10));
      writer.write(nada, row);
    }
    union.set((byte) 0, new OrcTimestamp("2011-12-25 12:34:56"));
    for(int r=0; r < 10; ++r) {
      row.setFieldValue(8, new IntWritable(r * 10 + 100));
      writer.write(nada, row);
    }
    OrcStruct row2 = new OrcStruct(type);
    writer.write(nada, row2);
    row.setFieldValue(8, new IntWritable(210));
    writer.write(nada, row);
    writer.close(Reporter.NULL);

    FileSplit split = new FileSplit(new Path(workDir, "all.orc"), 0, 100000,
        new String[0]);
    RecordReader<NullWritable, OrcStruct> reader =
        new OrcInputFormat<OrcStruct>().getRecordReader(split, conf,
            Reporter.NULL);
    nada = reader.createKey();
    row = reader.createValue();
    for(int r=0; r < 22; ++r) {
      assertTrue(reader.next(nada, row));
      if (r == 20) {
        for(int c=0; c < 12; ++c) {
          assertNull(row.getFieldValue(c));
        }
      } else {
        assertEquals(new BytesWritable(new byte[]{1, 2, 3, 4}), row.getFieldValue(0));
        assertEquals(new BooleanWritable(true), row.getFieldValue(1));
        assertEquals(new ByteWritable((byte) 23), row.getFieldValue(2));
        assertEquals(new Text("aaabbbcccd"), row.getFieldValue(3));
        assertEquals(new DateWritable(DateWritable.millisToDays
            (format.parse("2016-04-01").getTime())), row.getFieldValue(4));
        assertEquals(new HiveDecimalWritable("1.23"), row.getFieldValue(5));
        assertEquals(new DoubleWritable(1.5), row.getFieldValue(6));
        assertEquals(new FloatWritable(4.5f), row.getFieldValue(7));
        assertEquals(new IntWritable(r * 10), row.getFieldValue(8));
        assertEquals(longList, row.getFieldValue(9));
        assertEquals(map, row.getFieldValue(10));
        if (r < 10) {
          union.set((byte) 1, new Text("abcde"));
        } else {
          union.set((byte) 0, new OrcTimestamp("2011-12-25 12:34:56"));
        }
        assertEquals(struct, row.getFieldValue(11), "row " + r);
        assertEquals(new OrcTimestamp("1996-12-11 15:00:00"),
            row.getFieldValue(12), "row " + r);
      }
    }
    assertFalse(reader.next(nada, row));
  }

  /**
   * Test the case where the top level isn't a struct, but a long.
   */
  @Test
  public void testLongRoot() throws Exception {
    conf.set("mapreduce.task.attempt.id", "attempt_20160101_0001_m_000001_0");
    conf.setOutputCommitter(NullOutputCommitter.class);
    conf.set(OrcConf.COMPRESS.getAttribute(), "SNAPPY");
    conf.setInt(OrcConf.ROW_INDEX_STRIDE.getAttribute(), 1000);
    conf.setInt(OrcConf.BUFFER_SIZE.getAttribute(), 64 * 1024);
    conf.set(OrcConf.WRITE_FORMAT.getAttribute(), "0.11");
    final String typeStr = "bigint";
    OrcConf.MAPRED_OUTPUT_SCHEMA.setString(conf, typeStr);
    FileOutputFormat.setOutputPath(conf, workDir);
    LongWritable value = new LongWritable();
    NullWritable nada = NullWritable.get();
    RecordWriter<NullWritable, LongWritable> writer =
        new OrcOutputFormat<LongWritable>().getRecordWriter(fs, conf,
            "long.orc", Reporter.NULL);
    for(long lo=0; lo < 2000; ++lo) {
      value.set(lo);
      writer.write(nada, value);
    }
    writer.close(Reporter.NULL);

    Path path = new Path(workDir, "long.orc");
    Reader file = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    assertEquals(CompressionKind.SNAPPY, file.getCompressionKind());
    assertEquals(2000, file.getNumberOfRows());
    assertEquals(1000, file.getRowIndexStride());
    assertEquals(64 * 1024, file.getCompressionSize());
    assertEquals(OrcFile.Version.V_0_11, file.getFileVersion());
    FileSplit split = new FileSplit(path, 0, 100000,
        new String[0]);
    RecordReader<NullWritable, LongWritable> reader =
        new OrcInputFormat<LongWritable>().getRecordReader(split, conf,
            Reporter.NULL);
    nada = reader.createKey();
    value = reader.createValue();
    for(long lo=0; lo < 2000; ++lo) {
      assertTrue(reader.next(nada, value));
      assertEquals(lo, value.get());
    }
    assertFalse(reader.next(nada, value));
  }

  /**
   * Make sure that the writer ignores the OrcKey
   * @throws Exception
   */
  @Test
  public void testOrcKey() throws Exception {
    conf.set("mapreduce.output.fileoutputformat.outputdir", workDir.toString());
    conf.set("mapreduce.task.attempt.id", "attempt_jt0_0_m_0_0");
    String TYPE_STRING = "struct<i:int,s:string>";
    OrcConf.MAPRED_OUTPUT_SCHEMA.setString(conf, TYPE_STRING);
    conf.setOutputCommitter(NullOutputCommitter.class);
    TypeDescription schema = TypeDescription.fromString(TYPE_STRING);
    OrcKey key = new OrcKey(new OrcStruct(schema));
    RecordWriter<NullWritable, Writable> writer =
        new OrcOutputFormat<>().getRecordWriter(fs, conf, "key.orc",
            Reporter.NULL);
    NullWritable nada = NullWritable.get();
    for(int r=0; r < 2000; ++r) {
      ((OrcStruct) key.key).setAllFields(new IntWritable(r),
          new Text(Integer.toString(r)));
      writer.write(nada, key);
    }
    writer.close(Reporter.NULL);
    Path path = new Path(workDir, "key.orc");
    Reader file = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    assertEquals(2000, file.getNumberOfRows());
    assertEquals(TYPE_STRING, file.getSchema().toString());
  }

  /**
   * Make sure that the writer ignores the OrcValue
   * @throws Exception
   */
  @Test
  public void testOrcValue() throws Exception {
    conf.set("mapreduce.output.fileoutputformat.outputdir", workDir.toString());
    conf.set("mapreduce.task.attempt.id", "attempt_jt0_0_m_0_0");
    String TYPE_STRING = "struct<i:int>";
    OrcConf.MAPRED_OUTPUT_SCHEMA.setString(conf, TYPE_STRING);
    conf.setOutputCommitter(NullOutputCommitter.class);
    TypeDescription schema = TypeDescription.fromString(TYPE_STRING);
    OrcValue value = new OrcValue(new OrcStruct(schema));
    RecordWriter<NullWritable, Writable> writer =
        new OrcOutputFormat<>().getRecordWriter(fs, conf, "value.orc",
            Reporter.NULL);
    NullWritable nada = NullWritable.get();
    for(int r=0; r < 3000; ++r) {
      ((OrcStruct) value.value).setAllFields(new IntWritable(r));
      writer.write(nada, value);
    }
    writer.close(Reporter.NULL);
    Path path = new Path(workDir, "value.orc");
    Reader file = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    assertEquals(OrcConf.ROW_BATCH_SIZE.getDefaultValue(), file.options().getRowBatchSize());
    assertEquals(3000, file.getNumberOfRows());
    assertEquals(TYPE_STRING, file.getSchema().toString());
  }

  /**
   * Make sure that the ORC writer is initialized with a configured row batch size
   * @throws Exception
   */
  @Test
  public void testOrcOutputFormatWithRowBatchSize() throws Exception {
    conf.set("mapreduce.output.fileoutputformat.outputdir", workDir.toString());
    conf.set("mapreduce.task.attempt.id", "attempt_jt0_0_m_0_0");
    OrcConf.ROW_BATCH_SIZE.setInt(conf, 128);
    String TYPE_STRING = "struct<i:int,s:string>";
    OrcConf.MAPRED_OUTPUT_SCHEMA.setString(conf, TYPE_STRING);
    conf.setOutputCommitter(NullOutputCommitter.class);
    TypeDescription schema = TypeDescription.fromString(TYPE_STRING);
    OrcKey key = new OrcKey(new OrcStruct(schema));
    RecordWriter<NullWritable, Writable> writer =
        new OrcOutputFormat<>().getRecordWriter(fs, conf, "key.orc",
            Reporter.NULL);
    NullWritable nada = NullWritable.get();
    for(int r=0; r < 2000; ++r) {
      ((OrcStruct) key.key).setAllFields(new IntWritable(r),
          new Text(Integer.toString(r)));
      writer.write(nada, key);
    }
    writer.close(Reporter.NULL);
    Path path = new Path(workDir, "key.orc");
    Reader file = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    assertEquals(128, file.options().getRowBatchSize());
    assertEquals(2000, file.getNumberOfRows());
    assertEquals(TYPE_STRING, file.getSchema().toString());
  }
}
