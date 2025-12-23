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
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import java.io.IOException;

public class CoreReader {
  public static void main(Configuration conf, String[] args) throws IOException {
    // Get the information from the file footer
    Reader reader = OrcFile.createReader(new Path("my-file.orc"),
                                         OrcFile.readerOptions(conf));
    System.out.println("File schema: " + reader.getSchema());
    System.out.println("Row count: " + reader.getNumberOfRows());

    // Pick the schema we want to read using schema evolution
    TypeDescription readSchema =
        TypeDescription.fromString("struct<z:int,y:string,x:bigint>");
    // Read the row data
    VectorizedRowBatch batch = readSchema.createRowBatch();
    RecordReader rowIterator = reader.rows(reader.options()
                                             .schema(readSchema));
    LongColumnVector z = (LongColumnVector) batch.cols[0];
    BytesColumnVector y = (BytesColumnVector) batch.cols[1];
    LongColumnVector x = (LongColumnVector) batch.cols[2];
    while (rowIterator.nextBatch(batch)) {
      for(int row=0; row < batch.size; ++row) {
        int zRow = z.isRepeating ? 0: row;
        int xRow = x.isRepeating ? 0: row;
        System.out.println("z: " +
            (z.noNulls || !z.isNull[zRow] ? z.vector[zRow] : null));
        System.out.println("y: " + y.toString(row));
        System.out.println("x: " +
            (x.noNulls || !x.isNull[xRow] ? x.vector[xRow] : null));
      }
    }
    rowIterator.close();
  }

  public static void main(String[] args) throws IOException {
    main(new Configuration(), args);
  }
}
