/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for operations on Orc file with very large stripe statistics.
 * <p>
 * The test is disabled by default cause it is rather slow (approx 14 minutes) and memory greedy
 * (it requires about 4g heap space when creating the files). If you want to run it remove the
 * {@code Disabled} annotation and ensure that max heap (Xmx) is at least 4g.
 * </p>
 */
@Disabled("ORC-1361")
public class TestOrcWithLargeStripeStatistics {

  @ParameterizedTest
  @EnumSource(value = OrcFile.Version.class, mode = EnumSource.Mode.EXCLUDE, names = "FUTURE")
  public void testGetStripeStatisticsNoProtocolBufferExceptions(OrcFile.Version version)
      throws Exception {
    // Use a size that exceeds the protobuf limit (e.g., 1GB) to trigger protobuf exception
    Path p = createOrcFile(1024L << 20, version);
    try (Reader reader = OrcFile.createReader(p, OrcFile.readerOptions(new Configuration()))) {
      assertTrue(reader.getStripeStatistics().isEmpty());
    }
  }

  /**
   * Creates an Orc file with a metadata section of the specified size and return its path in the
   * filesystem.
   * 
   * The file has a fixed schema (500 string columns) and content (every column contains 200
   * characters, which is roughly 200 bytes). Each row is roughly 100KB uncompressed and each stripe
   * holds exactly one row thus stripe metadata (column statistics) per row is 200KB (100KB for min,
   * 100KB for max, few bytes for sum).
   * 
   * @param metadataSize the desired size of the resulting metadata section in bytes
   * @param version the desired version to create the file
   * @return the path to filesystem where the file was created.
   * @throws IOException if an IO problem occurs while creating the file
   */
  private static Path createOrcFile(long metadataSize, OrcFile.Version version) throws IOException {
    // Calculate the number of rows/stripes to create based on the size of one row (200KB).
    final long ROW_STRIPE_NUM = metadataSize / 200_000L;
    Path p = new Path(System.getProperty("test.tmp.dir"),
        TestOrcWithLargeStripeStatistics.class.getSimpleName()
            + "_" + ROW_STRIPE_NUM + "_" + version + ".orc");
    // Modify defaults to force one row per stripe.
    Configuration conf = new Configuration();
    conf.set(OrcConf.ROWS_BETWEEN_CHECKS.getAttribute(), "0");
    TypeDescription schema = createTypeDescription();
    OrcFile.WriterOptions writerOptions =
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .stripeSize(1)
            .encodingStrategy(OrcFile.EncodingStrategy.SPEED)
            .version(version);
    try (Writer writer = OrcFile.createWriter(p, writerOptions)) {
      VectorizedRowBatch batch = createSingleRowBatch(schema);
      for (long i = 0; i < ROW_STRIPE_NUM; i++) {
        writer.addRowBatch(batch);
      }
    }
    return p;
  }

  private static VectorizedRowBatch createSingleRowBatch(TypeDescription schema) {
    VectorizedRowBatch batch = schema.createRowBatch();
    batch.size = 1;
    byte[] bigString = new byte[200];
    Arrays.fill(bigString, (byte) 'A');
    for (int i = 0; i < batch.numCols; i++) {
      BytesColumnVector col = (BytesColumnVector) batch.cols[i];
      col.setVal(0, bigString);
    }
    return batch;
  }

  private static TypeDescription createTypeDescription() {
    String strCols = IntStream.range(0, 500)
        .mapToObj(i -> "col" + i + ":string")
        .collect(Collectors.joining(","));
    return TypeDescription.fromString("struct<" + strCols + ">");
  }

}
