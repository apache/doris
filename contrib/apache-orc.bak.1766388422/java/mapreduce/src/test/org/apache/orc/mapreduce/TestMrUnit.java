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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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

public class TestMrUnit {

  private static final File TEST_DIR = new File(
      System.getProperty("test.build.data",
          System.getProperty("java.io.tmpdir")), "TestMapReduce-mapreduce");
  private static FileSystem FS;

  private static final JobConf CONF = new JobConf();

  private static final TypeDescription INPUT_SCHEMA = TypeDescription
      .fromString("struct<one:struct<x:int,y:int>,two:struct<z:string>>");

  private static final TypeDescription OUT_SCHEMA = TypeDescription
      .fromString("struct<first:struct<x:int,y:int>,second:struct<z:string>>");

  static {
    OrcConf.MAPRED_SHUFFLE_KEY_SCHEMA.setString(CONF, "struct<x:int,y:int>");
    OrcConf.MAPRED_SHUFFLE_VALUE_SCHEMA.setString(CONF, "struct<z:string>");
    OrcConf.MAPRED_OUTPUT_SCHEMA.setString(CONF, OUT_SCHEMA.toString());
    // This is required due to ORC-964
    CONF.set("mapreduce.job.output.key.comparator.class", OrcKeyComparator.class.getName());
    try {
      FS = FileSystem.getLocal(CONF);
    } catch (IOException ioe) {
      FS = null;
    }
  }

  public static class OrcKeyComparator implements RawComparator<OrcKey>, JobConfigurable {

    private JobConf jobConf;

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      DataInputBuffer buffer1 = new DataInputBuffer();
      DataInputBuffer buffer2 = new DataInputBuffer();

      try {
        buffer1.reset(b1, s1, l1);
        buffer2.reset(b2, s2, l2);
        OrcKey orcKey1 = new OrcKey();
        orcKey1.configure(this.jobConf);
        orcKey1.readFields(buffer1);
        OrcKey orcKey2 = new OrcKey();
        orcKey2.configure(this.jobConf);
        orcKey2.readFields(buffer2);
        return orcKey1.compareTo(orcKey2);
      } catch (IOException e) {
        throw new RuntimeException("compare orcKey fail", e);
      }
    }

    @Override
    public int compare(OrcKey o1, OrcKey o2) {
      return o1.compareTo(o2);
    }

    @Override
    public void configure(JobConf jobConf) {
      this.jobConf = jobConf;
    }
  }


  /**
   * Split the input struct into its two parts.
   */
  public static class MyMapper
      extends Mapper<NullWritable, OrcStruct, OrcKey, OrcValue> {
    private final OrcKey keyWrapper = new OrcKey();
    private final OrcValue valueWrapper = new OrcValue();

    @Override
    protected void map(NullWritable key,
                       OrcStruct value,
                       Context context
    ) throws IOException, InterruptedException {
      keyWrapper.key = value.getFieldValue(0);
      valueWrapper.value = value.getFieldValue(1);
      context.write(keyWrapper, valueWrapper);
    }
  }

  /**
   * Glue the key and values back together.
   */
  public static class MyReducer
      extends Reducer<OrcKey, OrcValue, NullWritable, OrcStruct> {
    private final OrcStruct output = new OrcStruct(OUT_SCHEMA);
    private final NullWritable nada = NullWritable.get();

    @Override
    protected void reduce(OrcKey key,
                          Iterable<OrcValue> values,
                          Context context
    ) throws IOException, InterruptedException {
      output.setFieldValue(0, key.key);
      for(OrcValue value: values) {
        output.setFieldValue(1, value.value);
        context.write(nada, output);
      }
    }
  }

  public void writeInputFile(Path inputPath) throws IOException {
    Writer writer = OrcFile.createWriter(inputPath,
        OrcFile.writerOptions(CONF).setSchema(INPUT_SCHEMA).overwrite(true));
    OrcMapreduceRecordWriter<OrcStruct> recordWriter = new OrcMapreduceRecordWriter<>(writer);
    NullWritable nada = NullWritable.get();

    OrcStruct input = (OrcStruct) OrcStruct.createValue(INPUT_SCHEMA);
    IntWritable x =
        (IntWritable) ((OrcStruct) input.getFieldValue(0)).getFieldValue(0);
    IntWritable y =
        (IntWritable) ((OrcStruct) input.getFieldValue(0)).getFieldValue(1);
    Text z = (Text) ((OrcStruct) input.getFieldValue(1)).getFieldValue(0);

    for(int r = 0; r < 20; ++r) {
      x.set(100 -  (r / 4));
      y.set(r * 2);
      z.set(Integer.toHexString(r));
      recordWriter.write(nada, input);
    }
    recordWriter.close(null);
  }

  private void readOutputFile(Path output) throws IOException, InterruptedException {
    Reader reader = OrcFile.createReader(output, OrcFile.readerOptions(CONF));
    OrcMapreduceRecordReader<OrcStruct> recordReader = new OrcMapreduceRecordReader<>(reader,
            org.apache.orc.mapred.OrcInputFormat.buildOptions(CONF, reader, 0, 20));

    int[] expectedX = new int[20];
    int[] expectedY = new int[20];
    String[] expectedZ = new String[20];
    int count = 0;
    for(int g = 4; g >= 0; --g) {
      for(int i = 0; i < 4; ++i) {
        expectedX[count] = 100 - g;
        int r = g * 4 + i;
        expectedY[count] = r * 2;
        expectedZ[count ++] = Integer.toHexString(r);
      }
    }

    int row = 0;
    while (recordReader.nextKeyValue()) {
      OrcStruct value = recordReader.getCurrentValue();
      IntWritable x =
          (IntWritable) ((OrcStruct) value.getFieldValue(0)).getFieldValue(0);
      IntWritable y =
          (IntWritable) ((OrcStruct) value.getFieldValue(0)).getFieldValue(1);
      Text z = (Text) ((OrcStruct) value.getFieldValue(1)).getFieldValue(0);
      assertEquals(expectedX[row], x.get());
      assertEquals(expectedY[row], y.get());
      assertEquals(expectedZ[row], z.toString());
      row ++;
    }
    recordReader.close();
  }

  @Test
  public void testMapReduce() throws IOException, InterruptedException, ClassNotFoundException {
    Path testDir = new Path(TEST_DIR.getAbsolutePath());
    Path input = new Path(testDir, "input");
    Path output = new Path(testDir, "output");
    FS.delete(input, true);
    FS.delete(output, true);

    writeInputFile(new Path(input, "input.orc"));

    Job job = Job.getInstance(CONF);

    job.setMapperClass(MyMapper.class);
    job.setInputFormatClass(OrcInputFormat.class);
    FileInputFormat.setInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, output);
    job.setOutputKeyClass(OrcKey.class);
    job.setOutputValueClass(OrcValue.class);
    job.setOutputFormatClass(OrcOutputFormat.class);
    job.setReducerClass(MyReducer.class);
    job.setNumReduceTasks(1);

    job.waitForCompletion(true);

    FileStatus[] fileStatuses = output.getFileSystem(CONF)
        .listStatus(output, path -> path.getName().endsWith(".orc"));

    assertEquals(fileStatuses.length, 1);

    Path path = fileStatuses[0].getPath();
    readOutputFile(path);
  }

}
