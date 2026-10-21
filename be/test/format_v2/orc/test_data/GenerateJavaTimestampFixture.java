// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import java.sql.Timestamp;
import java.util.TimeZone;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

// Generate with ORC 1.9.0, hadoop-client-{api,runtime} 3.3.5, hive-storage-api 2.8.1,
// and protobuf-java 4.34.1 on Java 17.
// The hidden NANOS sibling exercises the matching-writer-zone decode path.
public class GenerateJavaTimestampFixture {
    public static void main(String[] args) throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("America/New_York"));
        TypeDescription schema = TypeDescription.fromString("struct<plain:timestamp,ns:timestamp>");
        schema.getChildren().get(1).setAttribute("iceberg.timestamp-unit", "NANOS");
        Configuration conf = new Configuration();
        conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
        String[] values = {"1800-01-01 00:00:00.123456789", "1969-12-31 23:59:59.500000123",
                "1970-01-01 00:00:00.000000001"};
        try (Writer writer = OrcFile.createWriter(new Path(args[0]),
                OrcFile.writerOptions(conf).setSchema(schema).overwrite(true))) {
            VectorizedRowBatch batch = schema.createRowBatch();
            batch.size = values.length;
            for (int row = 0; row < values.length; row++) {
                for (int col = 0; col < 2; col++) {
                    ((TimestampColumnVector) batch.cols[col]).set(row, Timestamp.valueOf(values[row]));
                }
            }
            writer.addRowBatch(batch);
        }
    }
}
