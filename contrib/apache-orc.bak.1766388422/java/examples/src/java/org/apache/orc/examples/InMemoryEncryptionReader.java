/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.InMemoryKeystore;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcFile.ReaderOptions;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.apache.orc.EncryptionAlgorithm.AES_CTR_128;

public class InMemoryEncryptionReader {
  public static void main(Configuration conf, String[] args) throws IOException {
    byte[] kmsKey = "secret123".getBytes(StandardCharsets.UTF_8);
    // InMemoryKeystore is used to get key to read encryption data.
    InMemoryKeystore keyProvider = new InMemoryKeystore().addKey("pii", AES_CTR_128, kmsKey);
    ReaderOptions readerOptions = OrcFile.readerOptions(conf).setKeyProvider(keyProvider);
    Reader reader = OrcFile.createReader(new Path("encrypted.orc"), readerOptions);

    System.out.println("File schema: " + reader.getSchema());
    System.out.println("Row count: " + reader.getNumberOfRows());

    // Pick the schema we want to read using schema evolution
    TypeDescription schema = TypeDescription.fromString("struct<x:int,y:string>");
    // Read the encryption data
    VectorizedRowBatch batch = schema.createRowBatch();
    RecordReader rowIterator = reader.rows(reader.options().schema(schema));
    LongColumnVector x = (LongColumnVector) batch.cols[0];
    BytesColumnVector y = (BytesColumnVector) batch.cols[1];
    while (rowIterator.nextBatch(batch)) {
      for (int row = 0; row < batch.size; ++row) {
        System.out.println("x: " + x.vector[row]);
        System.out.println("y: " + y.toString(row));
      }
    }
    rowIterator.close();
  }

  public static void main(String[] args) throws IOException {
    main(new Configuration(), args);
  }
}
