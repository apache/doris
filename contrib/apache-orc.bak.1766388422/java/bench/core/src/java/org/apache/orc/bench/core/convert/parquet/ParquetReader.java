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

package org.apache.orc.bench.core.convert.parquet;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.apache.orc.bench.core.convert.BatchReader;
import org.apache.orc.bench.core.convert.avro.AvroReader;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;

public class ParquetReader implements BatchReader {
  private final org.apache.parquet.hadoop.ParquetReader<GenericData.Record>
      reader;
  private final AvroReader.AvroConverter[] converters;

  public ParquetReader(Path path,
                       TypeDescription schema,
                       Configuration conf) throws IOException {
    HadoopInputFile inputFile = HadoopInputFile.fromPath(path, conf);
    reader = AvroParquetReader.<GenericData.Record>builder(inputFile)
        .withCompatibility(true).build();
    converters = AvroReader.buildConverters(schema);
  }

  @Override
  public boolean nextBatch(VectorizedRowBatch batch) throws IOException {
    batch.reset();
    int maxSize = batch.getMaxSize();
    while (batch.size < maxSize) {
      GenericData.Record value = reader.read();
      if (value == null) {
        break;
      }
      int row = batch.size++;
      for(int c=0; c < converters.length; ++c) {
        converters[c].convert(batch.cols[c], row, value.get(c));
      }
    }
    return batch.size != 0;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
