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

// Link against the ORC library used by Doris and pass the output fixture path.
// Each stripe has 20,000 rows and two 10,000-row index groups. Only its first row
// matches id > 5000. The second match makes cache granule [18432, 20480) true,
// so a cache hit seeks into the first stripe's SARG-filtered suffix at row 18432.
int main(int argc, char** argv) {
    if (argc != 2) {
        return 1;
    }
    auto stream = orc::writeLocalFile(argv[1]);
    auto type = std::unique_ptr<orc::Type>(
            orc::Type::buildTypeFromString("struct<id:int,payload:bigint>"));
    orc::WriterOptions options;
    options.setStripeSize(1).setRowIndexStride(10000).setCompression(orc::CompressionKind_NONE);
    auto writer = orc::createWriter(*type, stream.get(), options);
    auto batch = writer->createRowBatch(20000);
    auto& root = dynamic_cast<orc::StructVectorBatch&>(*batch);
    auto& ids = dynamic_cast<orc::LongVectorBatch&>(*root.fields[0]);
    auto& payloads = dynamic_cast<orc::LongVectorBatch&>(*root.fields[1]);
    for (int64_t stripe = 0; stripe < 2; ++stripe) {
        for (int64_t row = 0; row < 20000; ++row) {
            ids.data[row] = row == 0 ? 10000 + stripe : 0;
            payloads.data[row] = stripe * 20000 + row;
        }
        root.numElements = ids.numElements = payloads.numElements = 20000;
        writer->add(*batch);
    }
    writer->close();
    stream->close();
    return 0;
}
