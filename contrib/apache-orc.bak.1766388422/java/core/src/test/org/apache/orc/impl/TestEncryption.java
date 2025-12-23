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
package org.apache.orc.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.EncryptionAlgorithm;
import org.apache.orc.InMemoryKeystore;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestEncryption {

  Path workDir = new Path(System.getProperty("test.tmp.dir"));
  Configuration conf;
  FileSystem fs;
  Path testFilePath;
  TypeDescription schema;
  KeyProvider keyProvider;
  String encryption;
  String mask;

  @BeforeEach
  public void openFileSystem() throws Exception {
    conf = new Configuration();
    conf.setInt(OrcConf.ROW_INDEX_STRIDE.getAttribute(), VectorizedRowBatch.DEFAULT_SIZE);
    fs = FileSystem.getLocal(conf);
    fs.setWorkingDirectory(workDir);
    testFilePath = new Path("testWriterImpl.orc");
    fs.create(testFilePath, true);
    schema = TypeDescription.fromString("struct<id:int,name:string>");
    byte[] kmsKey = "secret123".getBytes(StandardCharsets.UTF_8);
    keyProvider = new InMemoryKeystore()
        .addKey("pii", EncryptionAlgorithm.AES_CTR_128, kmsKey);
    encryption = "pii:id,name";
    mask = "sha256:id,name";
  }

  @AfterEach
  public void deleteTestFile() throws Exception {
    fs.delete(testFilePath, false);
  }

  private void write() throws IOException {
    Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .overwrite(true)
            .setKeyProvider(keyProvider)
            .encrypt(encryption)
            .masks(mask));
    VectorizedRowBatch batch = schema.createRowBatch();
    LongColumnVector id = (LongColumnVector) batch.cols[0];
    BytesColumnVector name = (BytesColumnVector) batch.cols[1];
    for (int r = 0; r < VectorizedRowBatch.DEFAULT_SIZE * 2; ++r) {
      int row = batch.size++;
      id.vector[row] = r;
      byte[] buffer = ("name-" + (r * 3)).getBytes(StandardCharsets.UTF_8);
      name.setRef(row, buffer, 0, buffer.length);
      if (batch.size == batch.getMaxSize()) {
        writer.addRowBatch(batch);
        batch.reset();
      }
    }
    if (batch.size != 0) {
      writer.addRowBatch(batch);
    }
    writer.close();
  }

  private void read(boolean pushDown) throws IOException {
    Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).setKeyProvider(keyProvider));
    SearchArgument searchArgument = pushDown ? SearchArgumentFactory.newBuilder()
        .equals("id", PredicateLeaf.Type.LONG, (long) VectorizedRowBatch.DEFAULT_SIZE)
        .build() : null;
    VectorizedRowBatch batch = schema.createRowBatch();
    Reader.Options options = reader.options().schema(this.schema);
    if (pushDown) {
      options = options.searchArgument(searchArgument, new String[]{"id"});
    }
    RecordReader rowIterator = reader.rows(options);
    LongColumnVector idColumn = (LongColumnVector) batch.cols[0];
    BytesColumnVector nameColumn = (BytesColumnVector) batch.cols[1];
    int batchNum = pushDown ? 1 : 0;
    while (rowIterator.nextBatch(batch)) {
      for (int row = 0; row < batch.size; ++row) {
        long value = row + ((long) batchNum * VectorizedRowBatch.DEFAULT_SIZE);
        assertEquals(value, idColumn.vector[row]);
        assertEquals("name-" + (value * 3), nameColumn.toString(row));
      }
      batchNum ++;
    }
    rowIterator.close();
  }

  @Test
  public void testReadEncryption() throws IOException {
    write();
    read(false);
  }

  @Test
  public void testPushDownReadEncryption() throws IOException {
    write();
    read(true);
  }

}
