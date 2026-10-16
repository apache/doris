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

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "core/column/column_vector.h"
#include "format_v2/blob/paimon_blob_column_reader.h"
#include "format_v2/column_data.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris {
class Block;
} // namespace doris

namespace doris::format::paimon {

// Coordinates Default blob storage materialization across projected columns. Column readers
// own file selection and FILE construction; this class owns anchor row validation and output
// Block positions. PaimonReader supplies original file row positions after Parquet filtering.
class PaimonBlobMaterializer {
public:
    using ReadContext = PaimonBlobColumnReader::ReadContext;

    Status init(const std::map<std::string, std::string>& options);
    // Clears the previous split, validates row ranges and resolves projected FILE columns.
    // This does not open BLOB files. An absent blob_files field disables materialization;
    // a present but empty list still creates readers that fill missing nullable columns with NULL.
    Status prepare_split(const TPaimonFileDesc& params,
                         const std::vector<ColumnDefinition>& projected_columns);
    Status materialize(const ColumnInt64& anchor_row_positions, Block* table_block,
                       const ReadContext& context);
    // Releases per-split files and indexes while retaining query options.
    void reset();
    bool has_blob_columns() const { return !_blob_column_readers.empty(); }
    const std::set<std::string>& blob_column_names() const { return _blob_column_names; }

private:
    bool _blob_as_descriptor = false;
    // Logical range of the complete Parquet anchor. All byte splits use the same origin;
    // ROW_POSITION remains relative to the whole file, including rows skipped by filtering.
    int64_t _anchor_first_row_id = -1;
    int64_t _anchor_row_count = 0;
    std::map<GlobalIndex, std::unique_ptr<PaimonBlobColumnReader>> _blob_column_readers;
    std::set<std::string> _blob_column_names;
};

} // namespace doris::format::paimon
