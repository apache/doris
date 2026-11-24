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

#include <memory>
#include <orc/OrcFile.hh>
#include <orc/Writer.hh>

// Link against Doris's ORC library. Pass the row-group fixture path followed by
// the multi-stripe fixture path. Signed zeros must remain in separate Bloom filters.
int main(int argc, char** argv) {
    if (argc != 3) {
        return 1;
    }
    constexpr int64_t rows = 1000;
    for (int fixture = 1; fixture <= 2; ++fixture) {
        auto output = orc::writeLocalFile(argv[fixture]);
        auto type = orc::Type::buildTypeFromString("struct<id:int,f:float,d:double>");
        orc::WriterOptions options;
        options.setRowIndexStride(rows)
                .setStripeSize(fixture == 1 ? 1024 * 1024 : 1)
                .setColumnsUseBloomFilter({2, 3})
                .setBloomFilterFPP(0.000001)
                .setCompression(orc::CompressionKind_NONE);
        auto writer = orc::createWriter(*type, output.get(), options);
        auto batch = writer->createRowBatch(rows);
        auto& root = dynamic_cast<orc::StructVectorBatch&>(*batch);
        auto& ids = dynamic_cast<orc::LongVectorBatch&>(*root.fields[0]);
        auto& floats = dynamic_cast<orc::DoubleVectorBatch&>(*root.fields[1]);
        auto& doubles = dynamic_cast<orc::DoubleVectorBatch&>(*root.fields[2]);
        for (int64_t group = 0; group < 4; ++group) {
            root.numElements = ids.numElements = floats.numElements = doubles.numElements = rows;
            floats.hasNulls = doubles.hasNulls = group == 3;
            for (int64_t row = 0; row < rows; ++row) {
                ids.data[row] = group * rows + row;
                floats.notNull[row] = doubles.notNull[row] = group != 3;
                // Nonzero values straddle 2, so rejecting x = 2 requires Bloom rather than bounds.
                double value = row % 2 ? 1.0 : 3.0;
                if (group == 0) {
                    value = -0.0;
                } else if (group == 1) {
                    value = 0.0;
                }
                floats.data[row] = doubles.data[row] = value;
            }
            writer->add(*batch);
        }
        writer->close();
        output->close();
    }
    return 0;
}
