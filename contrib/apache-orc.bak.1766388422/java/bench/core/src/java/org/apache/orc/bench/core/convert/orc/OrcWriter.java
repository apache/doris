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

package org.apache.orc.bench.core.convert.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.bench.core.CompressionKind;
import org.apache.orc.bench.core.Utilities;
import org.apache.orc.bench.core.convert.BatchWriter;

import java.io.IOException;

public class OrcWriter implements BatchWriter {
  private final Writer writer;

  public OrcWriter(Path path,
                   TypeDescription schema,
                   Configuration conf,
                   CompressionKind compression
                   ) throws IOException {
    writer = OrcFile.createWriter(path,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .compress(Utilities.getCodec(compression)));
  }

  public void writeBatch(VectorizedRowBatch batch) throws IOException {
    writer.addRowBatch(batch);
  }

  public void close() throws IOException {
    writer.close();
  }
}
