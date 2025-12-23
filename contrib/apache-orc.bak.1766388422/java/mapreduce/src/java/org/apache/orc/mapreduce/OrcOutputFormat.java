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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Writer;

import java.io.IOException;

/**
 * An ORC output format that satisfies the org.apache.hadoop.mapreduce API.
 */
public class OrcOutputFormat<V extends Writable>
    extends FileOutputFormat<NullWritable, V> {
  private static final String EXTENSION = ".orc";
  // This is useful for unit tests or local runs where you don't need the
  // output committer.
  public static final String SKIP_TEMP_DIRECTORY =
      "orc.mapreduce.output.skip-temporary-directory";

  @Override
  public RecordWriter<NullWritable, V>
       getRecordWriter(TaskAttemptContext taskAttemptContext
                       ) throws IOException {
    Configuration conf = taskAttemptContext.getConfiguration();
    Path filename = getDefaultWorkFile(taskAttemptContext, EXTENSION);
    Writer writer = OrcFile.createWriter(filename,
        org.apache.orc.mapred.OrcOutputFormat.buildOptions(conf));
    return new OrcMapreduceRecordWriter<V>(writer,
        OrcConf.ROW_BATCH_SIZE.getInt(conf),
        OrcConf.ROW_BATCH_CHILD_LIMIT.getInt(conf));
  }

  @Override
  public Path getDefaultWorkFile(TaskAttemptContext context,
                                 String extension) throws IOException {
    if (context.getConfiguration().getBoolean(SKIP_TEMP_DIRECTORY, false)) {
      return new Path(getOutputPath(context),
          getUniqueFile(context, getOutputName(context), extension));
    } else {
      return super.getDefaultWorkFile(context, extension);
    }
  }
}
