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

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.StripeStatistics;
import org.apache.orc.StringColumnStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

// Reproduce the stripe/row-group statistics mismatch from Doris #50185.
// Run with java -ea; the output must not already exist.
public class GenerateOrcMergeFixture {
    private static final int ROWS_PER_GROUP = 10000;
    private static final int STRIPES = 3;
    private static final String TARGET = "1210647803";
    private static final TypeDescription SCHEMA = TypeDescription.fromString(
            "struct<dt:string,type:string,risk_flow_id:string,trace_id:string,"
                    + "created_time:string,created_ts:bigint,subflow_decision:string>");

    private static void setString(VectorizedRowBatch batch, int column, int row, String value) {
        ((BytesColumnVector) batch.cols[column]).setVal(row, value.getBytes(StandardCharsets.UTF_8));
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
        OrcConf.DICTIONARY_KEY_SIZE_THRESHOLD.setDouble(conf, 0.0);
        Path path = new Path(args[0]);
        try (Writer writer = OrcFile.createWriter(path, OrcFile.writerOptions(conf)
                .setSchema(SCHEMA).compress(CompressionKind.ZLIB).rowIndexStride(ROWS_PER_GROUP)
                .stripeSize(64L * 1024 * 1024).overwrite(false))) {
            VectorizedRowBatch batch = SCHEMA.createRowBatch(1000);
            for (int stripe = 0; stripe < STRIPES; stripe++) {
                for (int group = 0; group < 2; group++) {
                    for (int offset = 0; offset < ROWS_PER_GROUP; offset += batch.getMaxSize()) {
                        batch.reset();
                        batch.size = batch.getMaxSize();
                        for (int row = 0; row < batch.size; row++) {
                            long id = stripe * 2L * ROWS_PER_GROUP
                                    + group * ROWS_PER_GROUP + offset + row;
                            setString(batch, 0, row, "20250415");
                            setString(batch, 1, row, "type_" + stripe);
                            setString(batch, 2, row, Long.toHexString(id * 2654435761L));
                            // Stripe min/max encloses TARGET; neither row group contains it.
                            setString(batch, 3, row, group == 0 ? "1210647802" : "1210647804");
                            setString(batch, 4, row, "2025-04-15 00:00:00");
                            ((LongColumnVector) batch.cols[5]).vector[row] = 1744675200000L + id;
                            setString(batch, 6, row, "decision_" + (id % (stripe + 2)));
                        }
                        writer.addRowBatch(batch);
                    }
                }
                // Force exactly two whole row groups per stripe, independently of compression.
                writer.writeIntermediateFooter();
            }
        }
        try (Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf))) {
            assert reader.getNumberOfRows() == STRIPES * 2L * ROWS_PER_GROUP;
            assert reader.getStripes().size() == STRIPES;
            assert reader.getRowIndexStride() == ROWS_PER_GROUP;
            assert reader.getCompressionKind() == CompressionKind.ZLIB;
            List<StripeStatistics> statistics = reader.getStripeStatistics();
            int traceId = SCHEMA.findSubtype("trace_id").getId();
            for (int stripe = 0; stripe < STRIPES; stripe++) {
                assert reader.getStripes().get(stripe).getNumberOfRows() == 2L * ROWS_PER_GROUP;
                StringColumnStatistics trace = (StringColumnStatistics)
                        statistics.get(stripe).getColumnStatistics()[traceId];
                assert trace.getMinimum().compareTo(TARGET) < 0;
                assert trace.getMaximum().compareTo(TARGET) > 0;
            }
            System.out.println("Generated ZLIB ORC: 3 stripes, 6 row groups, 60000 rows; "
                    + "stripe trace_id bounds enclose the absent target " + TARGET);
        }
    }
}
