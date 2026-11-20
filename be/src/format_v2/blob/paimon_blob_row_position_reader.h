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

#include <cstdint>
#include <memory>
#include <vector>

#include "core/column/column_vector.h"
#include "format_v2/blob/paimon_blob_column_reader.h"

namespace doris::format {
class PaimonBlobFileReader;
} // namespace doris::format

namespace doris::format::paimon {

// Append-table managed BLOBs: join original anchor row positions to companion
// .blob files. Only this implementation owns decoded indexes and validates record CRCs.
class PaimonBlobRowPositionReader final : public PaimonBlobColumnReader {
public:
    PaimonBlobRowPositionReader(
            const ColumnDefinition& column_definition,
            const std::vector<const TPaimonBlobFileDesc*>& blob_file_descriptors,
            PaimonBlobOutputMode output_mode);
    ~PaimonBlobRowPositionReader() override;

    // Sort one field's file descriptors and validate full anchor coverage.
    // The caller validates field IDs and the non-overflowing anchor range first.
    static Status validate_blob_files(
            std::vector<const TPaimonBlobFileDesc*>* blob_file_descriptors,
            int64_t anchor_first_row_id, int64_t anchor_row_count);

    Status prepare_split(const TPaimonFileDesc& split_params) override;
    void reset() override;
    Status read(const ColumnPtr& input_column, const DataTypePtr& input_column_type,
                const ReadContext& read_context, ColumnPtr* result_column) override;
    // Positions are zero-based in the complete anchor file and survive filtering.
    Status read_rows(const ColumnInt64& row_positions, int64_t anchor_first_row_id,
                     const ReadContext& read_context, MutableColumnPtr* result_column);

private:
    struct BlobFileState {
        TPaimonBlobFileDesc file_descriptor;
        std::unique_ptr<PaimonBlobFileReader> file_reader;
    };
    Status _open_blob_file(BlobFileState* blob_file, const ReadContext& read_context);

    int32_t _field_id;
    int64_t _anchor_first_row_id = -1;
    int64_t _anchor_row_count = 0;
    std::vector<BlobFileState> _blob_files;
};

} // namespace doris::format::paimon
