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

#pragma once

#include <vector>

#include "format_v2/parquet/reader/native/decoder.h"

namespace doris::format::parquet::native {
class ByteStreamSplitDecoder final : public Decoder {
public:
    ByteStreamSplitDecoder() = default;
    ~ByteStreamSplitDecoder() override = default;

    Status decode_fixed_values(size_t num_values, ParquetFixedValueConsumer& consumer) override;

    Status decode_selected_fixed_values(const ParquetSelection& selection,
                                        ParquetFixedValueConsumer& consumer) override;

    Status skip_values(size_t num_values) override;

private:
    std::vector<uint8_t> _decoded_values;
};

} // namespace doris::format::parquet::native
