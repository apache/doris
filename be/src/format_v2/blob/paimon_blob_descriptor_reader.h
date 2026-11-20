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

#include "format_v2/blob/paimon_blob_column_reader.h"

namespace doris::format::paimon {

class PaimonBlobPayloadReader;

// PK-table managed BLOBs: decode descriptors from scalar/array/map physical inputs.
// The split-wide payload cache is borrowed from the owning materializer.
class PaimonBlobDescriptorReader final : public PaimonBlobColumnReader {
public:
    PaimonBlobDescriptorReader(const ColumnDefinition& column_definition,
                               PaimonBlobOutputMode output_mode,
                               PaimonBlobPayloadReader& payload_reader)
            : PaimonBlobColumnReader(column_definition, output_mode),
              _payload_reader(payload_reader) {}
    Status prepare_split(const TPaimonFileDesc&) override { return Status::OK(); }
    void reset() override {}
    Status read(const ColumnPtr& input_column, const DataTypePtr& input_column_type,
                const ReadContext& read_context, ColumnPtr* result_column) override;

private:
    Status _convert(const ColumnPtr& input_column, const DataTypePtr& input_column_type,
                    const DataTypePtr& result_column_type, const ReadContext& read_context,
                    ColumnPtr* result_column);

    PaimonBlobPayloadReader& _payload_reader;
};

} // namespace doris::format::paimon
