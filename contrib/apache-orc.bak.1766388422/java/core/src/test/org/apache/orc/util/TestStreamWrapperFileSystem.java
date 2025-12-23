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

package org.apache.orc.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TestVectorOrcFile;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for StreamWrapperFileSystem.
 */
public class TestStreamWrapperFileSystem {

  @Test
  public void testWrapper() throws IOException {
    Configuration conf = new Configuration();
    Path realFilename = new Path(TestVectorOrcFile.getFileFromClasspath(
        "orc-file-11-format.orc"));
    FileSystem local = FileSystem.getLocal(conf);
    FSDataInputStream stream = local.open(realFilename);
    long fileSize = local.getFileStatus(realFilename).getLen();
    FileSystem fs = new StreamWrapperFileSystem(stream, new Path("foo"),
        fileSize, conf);
    assertSame(stream, fs.open(new Path("foo")));
    TypeDescription readerSchema =
        TypeDescription.fromString("struct<boolean1:boolean>");
    try (Reader reader = OrcFile.createReader(new Path("foo"),
            OrcFile.readerOptions(conf).filesystem(fs));
        RecordReader rows = reader.rows(reader.options().schema(readerSchema))) {

      // make sure that the metadata is what we expect
      assertEquals(7500, reader.getNumberOfRows());
      assertEquals("struct<boolean1:boolean,byte1:tinyint,short1:smallint," +
              "int1:int,long1:bigint," +"float1:float,double1:double," +
              "bytes1:binary,string1:string,middle:struct<list:array<struct<" +
              "int1:int,string1:string>>>,list:array<struct<int1:int," +
              "string1:string>>,map:map<string,struct<int1:int," +
              "string1:string>>,ts:timestamp,decimal1:decimal(38,10)>",
          reader.getSchema().toString());

      // read the boolean1 column and check the data
      VectorizedRowBatch batch = readerSchema.createRowBatchV2();
      LongColumnVector boolean1 = (LongColumnVector) batch.cols[0];
      int current = 0;
      for(int r=0; r < 7500; ++r) {
        if (current >= batch.size) {
          assertTrue(rows.nextBatch(batch), "row " + r);
          current = 0;
        }
        assertEquals(r % 2, boolean1.vector[current++], "row " + r);
      }
    }
  }
}
