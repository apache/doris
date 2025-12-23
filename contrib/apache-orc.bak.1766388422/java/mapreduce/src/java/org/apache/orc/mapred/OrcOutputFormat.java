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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;

/**
 * An ORC output format that satisfies the org.apache.hadoop.mapred API.
 */
public class OrcOutputFormat<V extends Writable>
    extends FileOutputFormat<NullWritable, V> {

  /**
   * This function builds the options for the ORC Writer based on the JobConf.
   * @param conf the job configuration
   * @return a new options object
   */
  public static OrcFile.WriterOptions buildOptions(Configuration conf) {
    return OrcFile.writerOptions(conf)
        .version(OrcFile.Version.byName(OrcConf.WRITE_FORMAT.getString(conf)))
        .setSchema(TypeDescription.fromString(OrcConf.MAPRED_OUTPUT_SCHEMA
            .getString(conf)))
        .compress(CompressionKind.valueOf(OrcConf.COMPRESS.getString(conf)))
        .encodingStrategy(OrcFile.EncodingStrategy.valueOf
            (OrcConf.ENCODING_STRATEGY.getString(conf)))
        .bloomFilterColumns(OrcConf.BLOOM_FILTER_COLUMNS.getString(conf))
        .bloomFilterFpp(OrcConf.BLOOM_FILTER_FPP.getDouble(conf))
        .blockSize(OrcConf.BLOCK_SIZE.getLong(conf))
        .blockPadding(OrcConf.BLOCK_PADDING.getBoolean(conf))
        .stripeSize(OrcConf.STRIPE_SIZE.getLong(conf))
        .rowIndexStride((int) OrcConf.ROW_INDEX_STRIDE.getLong(conf))
        .bufferSize((int) OrcConf.BUFFER_SIZE.getLong(conf))
        .paddingTolerance(OrcConf.BLOCK_PADDING_TOLERANCE.getDouble(conf))
        .encrypt(OrcConf.ENCRYPTION.getString(conf))
        .masks(OrcConf.DATA_MASK.getString(conf));
  }

  @Override
  public RecordWriter<NullWritable, V> getRecordWriter(FileSystem fileSystem,
                                                       JobConf conf,
                                                       String name,
                                                       Progressable progressable
                                                       ) throws IOException {
    Path path = getTaskOutputPath(conf, name);
    Writer writer = OrcFile.createWriter(path,
        buildOptions(conf).fileSystem(fileSystem));
    return new OrcMapredRecordWriter<>(writer,
        OrcConf.ROW_BATCH_SIZE.getInt(conf),
        OrcConf.ROW_BATCH_CHILD_LIMIT.getInt(conf));
  }
}
