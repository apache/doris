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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.mapred.OrcKey;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMapreduceOrcOutputFormat {

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

  @Test
  public void testPredicatePushdown() throws Exception {
    TaskAttemptID id = new TaskAttemptID("jt", 0, TaskType.MAP, 0, 0);
    TaskAttemptContext attemptContext = new TaskAttemptContextImpl(conf, id);
    final String typeStr = "struct<i:int,s:string>";
    OrcConf.MAPRED_OUTPUT_SCHEMA.setString(conf, typeStr);
    conf.set("mapreduce.output.fileoutputformat.outputdir", workDir.toString());
    conf.setInt(OrcConf.ROW_INDEX_STRIDE.getAttribute(), 1000);
    conf.setBoolean(OrcConf.ALLOW_SARG_TO_FILTER.getAttribute(), false);
    conf.setBoolean(OrcOutputFormat.SKIP_TEMP_DIRECTORY, true);
    OutputFormat<NullWritable, OrcStruct> outputFormat =
        new OrcOutputFormat<OrcStruct>();
    RecordWriter<NullWritable, OrcStruct> writer =
        outputFormat.getRecordWriter(attemptContext);

    // write 4000 rows with the integer and the binary string
    TypeDescription type = TypeDescription.fromString(typeStr);
    OrcStruct row = (OrcStruct) OrcStruct.createValue(type);
    NullWritable nada = NullWritable.get();
    for(int r=0; r < 4000; ++r) {
      row.setFieldValue(0, new IntWritable(r));
      row.setFieldValue(1, new Text(Integer.toBinaryString(r)));
      writer.write(nada, row);
    }
    writer.close(attemptContext);

    OrcInputFormat.setSearchArgument(conf,
        SearchArgumentFactory.newBuilder()
            .between("i", PredicateLeaf.Type.LONG, new Long(1500), new Long(1999))
            .build(), new String[]{null, "i", "s"});
    FileSplit split = new FileSplit(new Path(workDir, "part-m-00000.orc"),
        0, 1000000, new String[0]);
    RecordReader<NullWritable, OrcStruct> reader =
        new OrcInputFormat<OrcStruct>().createRecordReader(split,
            attemptContext);
    // the sarg should cause it to skip over the rows except 1000 to 2000
    for(int r=1000; r < 2000; ++r) {
      assertTrue(reader.nextKeyValue());
      row = reader.getCurrentValue();
      assertEquals(r, ((IntWritable) row.getFieldValue(0)).get());
      assertEquals(Integer.toBinaryString(r), row.getFieldValue(1).toString());
    }
    assertFalse(reader.nextKeyValue());
  }

  @Test
  public void testColumnSelection() throws Exception {
    String typeStr = "struct<i:int,j:int,k:int>";
    OrcConf.MAPRED_OUTPUT_SCHEMA.setString(conf, typeStr);
    conf.set("mapreduce.output.fileoutputformat.outputdir", workDir.toString());
    conf.setInt(OrcConf.ROW_INDEX_STRIDE.getAttribute(), 1000);
    conf.setBoolean(OrcOutputFormat.SKIP_TEMP_DIRECTORY, true);
    TaskAttemptID id = new TaskAttemptID("jt", 0, TaskType.MAP, 0, 1);
    TaskAttemptContext attemptContext = new TaskAttemptContextImpl(conf, id);
    OutputFormat<NullWritable, OrcStruct> outputFormat =
        new OrcOutputFormat<OrcStruct>();
    RecordWriter<NullWritable, OrcStruct> writer =
        outputFormat.getRecordWriter(attemptContext);

    // write 4000 rows with the integer and the binary string
    TypeDescription type = TypeDescription.fromString(typeStr);
    OrcStruct row = (OrcStruct) OrcStruct.createValue(type);
    NullWritable nada = NullWritable.get();
    for(int r=0; r < 3000; ++r) {
      row.setFieldValue(0, new IntWritable(r));
      row.setFieldValue(1, new IntWritable(r * 2));
      row.setFieldValue(2, new IntWritable(r * 3));
      writer.write(nada, row);
    }
    writer.close(attemptContext);

    conf.set(OrcConf.INCLUDE_COLUMNS.getAttribute(), "0,2");
    FileSplit split = new FileSplit(new Path(workDir, "part-m-00000.orc"),
        0, 1000000, new String[0]);
    RecordReader<NullWritable, OrcStruct> reader =
        new OrcInputFormat<OrcStruct>().createRecordReader(split,
            attemptContext);
    // the sarg should cause it to skip over the rows except 1000 to 2000
    for(int r=0; r < 3000; ++r) {
      assertTrue(reader.nextKeyValue());
      row = reader.getCurrentValue();
      assertEquals(r, ((IntWritable) row.getFieldValue(0)).get());
      assertNull(row.getFieldValue(1));
      assertEquals(r * 3, ((IntWritable) row.getFieldValue(2)).get());
    }
    assertFalse(reader.nextKeyValue());
  }

  @Test
  public void testAcidSelectionNoSchema() throws IOException, InterruptedException {
    TaskAttemptID id = new TaskAttemptID("jt", 0, TaskType.MAP, 0, 1);
    TaskAttemptContext attemptContext = new TaskAttemptContextImpl(conf, id);
    // struct<operation:int,originalTransaction:bigint,bucket:int,rowId:bigint,currentTransaction:bigint,
    // row:struct<i:int,j:int,k:int>>
    conf.set(OrcConf.INCLUDE_COLUMNS.getAttribute(), "5");
    // Do not set OrcConf.MAPRED_INPUT_SCHEMA (reader should use file schema instead)
    FileSplit split = new FileSplit(new Path(getClass().getClassLoader().
        getSystemResource("acid5k.orc").getPath()),
        0, 1000000, new String[0]);
    RecordReader<NullWritable, OrcStruct> reader =
        new OrcInputFormat<OrcStruct>().createRecordReader(split,
            attemptContext);
    // Make sure we can read all rows
    OrcStruct row;
    for (int r=0; r < 5000; ++r) {
      assertTrue(reader.nextKeyValue());
      row = reader.getCurrentValue();
      assertEquals(6, row.getNumFields());
      OrcStruct innerRow = (OrcStruct) row.getFieldValue(5);
      assertEquals(3,innerRow.getNumFields());
      assertTrue(((IntWritable)innerRow.getFieldValue(0)).get() >= 0);
      assertTrue(((IntWritable)innerRow.getFieldValue(1)).get() >= 0);
      assertTrue(((IntWritable)innerRow.getFieldValue(2)).get() >= 0);
    }
  }

  @Test
  public void testColumnSelectionBlank() throws Exception {
    String typeStr = "struct<i:int,j:int,k:int>";
    OrcConf.MAPRED_OUTPUT_SCHEMA.setString(conf, typeStr);
    conf.set("mapreduce.output.fileoutputformat.outputdir", workDir.toString());
    conf.setInt(OrcConf.ROW_INDEX_STRIDE.getAttribute(), 1000);
    conf.setBoolean(OrcOutputFormat.SKIP_TEMP_DIRECTORY, true);
    TaskAttemptID id = new TaskAttemptID("jt", 0, TaskType.MAP, 0, 1);
    TaskAttemptContext attemptContext = new TaskAttemptContextImpl(conf, id);
    OutputFormat<NullWritable, OrcStruct> outputFormat =
        new OrcOutputFormat<OrcStruct>();
    RecordWriter<NullWritable, OrcStruct> writer =
        outputFormat.getRecordWriter(attemptContext);

    // write 3000 rows with the integer and the binary string
    TypeDescription type = TypeDescription.fromString(typeStr);
    OrcStruct row = (OrcStruct) OrcStruct.createValue(type);
    NullWritable nada = NullWritable.get();
    for (int r = 0; r < 3000; ++r) {
      row.setFieldValue(0, new IntWritable(r));
      row.setFieldValue(1, new IntWritable(r * 2));
      row.setFieldValue(2, new IntWritable(r * 3));
      writer.write(nada, row);
    }
    writer.close(attemptContext);

    conf.set(OrcConf.INCLUDE_COLUMNS.getAttribute(), "");
    FileSplit split = new FileSplit(new Path(workDir, "part-m-00000.orc"),
        0, 1000000, new String[0]);
    RecordReader<NullWritable, OrcStruct> reader =
        new OrcInputFormat<OrcStruct>().createRecordReader(split,
            attemptContext);
    // the sarg should cause it to skip over the rows except 1000 to 2000
    for (int r = 0; r < 3000; ++r) {
      assertTrue(reader.nextKeyValue());
      row = reader.getCurrentValue();
      assertNull(row.getFieldValue(0));
      assertNull(row.getFieldValue(1));
      assertNull(row.getFieldValue(2));
    }
    assertFalse(reader.nextKeyValue());
  }

  /**
   * Make sure that the writer ignores the OrcKey
   * @throws Exception
   */
  @Test
  public void testOrcKey() throws Exception {
    conf.set("mapreduce.output.fileoutputformat.outputdir", workDir.toString());
    String TYPE_STRING = "struct<i:int,s:string>";
    OrcConf.MAPRED_OUTPUT_SCHEMA.setString(conf, TYPE_STRING);
    conf.setBoolean(OrcOutputFormat.SKIP_TEMP_DIRECTORY, true);
    TaskAttemptID id = new TaskAttemptID("jt", 0, TaskType.MAP, 0, 1);
    TaskAttemptContext attemptContext = new TaskAttemptContextImpl(conf, id);
    TypeDescription schema = TypeDescription.fromString(TYPE_STRING);
    OrcKey key = new OrcKey(new OrcStruct(schema));
    RecordWriter<NullWritable, Writable> writer =
        new OrcOutputFormat<>().getRecordWriter(attemptContext);
    NullWritable nada = NullWritable.get();
    for(int r=0; r < 2000; ++r) {
      ((OrcStruct) key.key).setAllFields(new IntWritable(r),
          new Text(Integer.toString(r)));
      writer.write(nada, key);
    }
    writer.close(attemptContext);
    Path path = new Path(workDir, "part-m-00000.orc");
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
    String TYPE_STRING = "struct<i:int>";
    OrcConf.MAPRED_OUTPUT_SCHEMA.setString(conf, TYPE_STRING);
    conf.setBoolean(OrcOutputFormat.SKIP_TEMP_DIRECTORY, true);
    TaskAttemptID id = new TaskAttemptID("jt", 0, TaskType.MAP, 0, 1);
    TaskAttemptContext attemptContext = new TaskAttemptContextImpl(conf, id);

    TypeDescription schema = TypeDescription.fromString(TYPE_STRING);
    OrcValue value = new OrcValue(new OrcStruct(schema));
    RecordWriter<NullWritable, Writable> writer =
        new OrcOutputFormat<>().getRecordWriter(attemptContext);
    NullWritable nada = NullWritable.get();
    for(int r=0; r < 3000; ++r) {
      ((OrcStruct) value.value).setAllFields(new IntWritable(r));
      writer.write(nada, value);
    }
    writer.close(attemptContext);
    Path path = new Path(workDir, "part-m-00000.orc");
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
    OrcConf.ROW_BATCH_SIZE.setInt(conf, 128);
    String TYPE_STRING = "struct<i:int,s:string>";
    OrcConf.MAPRED_OUTPUT_SCHEMA.setString(conf, TYPE_STRING);
    conf.setBoolean(OrcOutputFormat.SKIP_TEMP_DIRECTORY, true);
    TaskAttemptID id = new TaskAttemptID("jt", 0, TaskType.MAP, 0, 1);
    TaskAttemptContext attemptContext = new TaskAttemptContextImpl(conf, id);
    TypeDescription schema = TypeDescription.fromString(TYPE_STRING);
    OrcKey key = new OrcKey(new OrcStruct(schema));
    RecordWriter<NullWritable, Writable> writer =
        new OrcOutputFormat<>().getRecordWriter(attemptContext);
    NullWritable nada = NullWritable.get();
    for(int r=0; r < 2000; ++r) {
      ((OrcStruct) key.key).setAllFields(new IntWritable(r),
          new Text(Integer.toString(r)));
      writer.write(nada, key);
    }
    writer.close(attemptContext);
    Path path = new Path(workDir, "part-m-00000.orc");
    Reader file = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    assertEquals(128, file.options().getRowBatchSize());
    assertEquals(2000, file.getNumberOfRows());
    assertEquals(TYPE_STRING, file.getSchema().toString());
  }
}
