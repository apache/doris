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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.apache.orc.bench.core.CompressionKind;
import org.apache.orc.bench.core.convert.BatchWriter;
import org.apache.orc.bench.core.convert.avro.AvroSchemaUtils;
import org.apache.orc.bench.core.convert.avro.AvroWriter;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;

import java.io.IOException;

public class ParquetWriter implements BatchWriter {
  private final org.apache.parquet.hadoop.ParquetWriter<GenericData.Record>
      writer;
  private final AvroWriter.AvroConverter[] converters;
  private final GenericData.Record record;


  static CompressionCodecName getParquetCompression(CompressionKind kind) {
    switch (kind) {
      case NONE:
        return CompressionCodecName.UNCOMPRESSED;
      case ZLIB:
        return CompressionCodecName.GZIP;
      case SNAPPY:
        return CompressionCodecName.SNAPPY;
      case ZSTD:
        return CompressionCodecName.ZSTD;
      default:
        throw new IllegalArgumentException("Unhandled compression type " + kind);
    }
  }
  public ParquetWriter(Path path,
                       TypeDescription schema,
                       Configuration conf,
                       CompressionKind compression
                       ) throws IOException {
    Schema avroSchema = AvroSchemaUtils.createAvroSchema(schema);
    HadoopOutputFile outputFile = HadoopOutputFile.fromPath(path, conf);
    writer = AvroParquetWriter
        .<GenericData.Record>builder(outputFile)
        .withSchema(avroSchema)
        .withConf(conf)
        .withCompressionCodec(getParquetCompression(compression))
        .build();
    converters = AvroWriter.buildConverters(schema, avroSchema);
    record = new GenericData.Record(avroSchema);
  }

  public void writeBatch(VectorizedRowBatch batch) throws IOException {
    for(int r=0; r < batch.size; ++r) {
      for(int f=0; f < batch.cols.length; ++f) {
        record.put(f, converters[f].convert(batch.cols[f], r));
      }
      writer.write(record);
    }
  }

  public void close() throws IOException {
    writer.close();
  }
}
