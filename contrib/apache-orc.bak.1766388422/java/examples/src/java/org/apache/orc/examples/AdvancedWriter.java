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

package org.apache.orc.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * This example shows how to write compound data types in ORC.
 *
 */
public class AdvancedWriter {
  public static void main(Configuration conf, String[] args) throws IOException {
    Path testFilePath = new Path("advanced-example.orc");

    TypeDescription schema =
        TypeDescription.fromString("struct<first:int," +
            "second:int,third:map<string,int>>");

    Writer writer =
        OrcFile.createWriter(testFilePath,
            OrcFile.writerOptions(conf).setSchema(schema));

    VectorizedRowBatch batch = schema.createRowBatch();
    LongColumnVector first = (LongColumnVector) batch.cols[0];
    LongColumnVector second = (LongColumnVector) batch.cols[1];

    //Define map. You need also to cast the key and value vectors
    MapColumnVector map = (MapColumnVector) batch.cols[2];
    BytesColumnVector mapKey = (BytesColumnVector) map.keys;
    LongColumnVector mapValue = (LongColumnVector) map.values;

    // Each map has 5 elements
    final int MAP_SIZE = 5;
    final int BATCH_SIZE = batch.getMaxSize();

    // Ensure the map is big enough
    mapKey.ensureSize(BATCH_SIZE * MAP_SIZE, false);
    mapValue.ensureSize(BATCH_SIZE * MAP_SIZE, false);

    // add 1500 rows to file
    for(int r=0; r < 1500; ++r) {
      int row = batch.size++;

      first.vector[row] = r;
      second.vector[row] = r * 3;

      map.offsets[row] = map.childCount;
      map.lengths[row] = MAP_SIZE;
      map.childCount += MAP_SIZE;

      for (int mapElem = (int) map.offsets[row];
           mapElem < map.offsets[row] + MAP_SIZE; ++mapElem) {
        String key = "row " + r + "." + (mapElem - map.offsets[row]);
        mapKey.setVal(mapElem, key.getBytes(StandardCharsets.UTF_8));
        mapValue.vector[mapElem] = mapElem;
      }
      if (row == BATCH_SIZE - 1) {
        writer.addRowBatch(batch);
        batch.reset();
      }
    }
    if (batch.size != 0) {
      writer.addRowBatch(batch);
      batch.reset();
    }
    writer.close();  }

  public static void main(String[] args) throws IOException {
    main(new Configuration(), args);
  }
}
