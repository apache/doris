// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "format_v2/column_data.h"
#include "format_v2/parquet/parquet_profile.h"

namespace parquet {
class RowGroupReader;
namespace internal {
class RecordReader;
} // namespace internal
} // namespace parquet

namespace doris::format::parquet {
struct ParquetColumnSchema;

// Isolated compatibility reader for the existing COUNT(nullable_col) pushdown.
//
// Ordinary scans never instantiate this class. COUNT needs only Dremel definition/repetition
// levels, so this reader exposes exactly one shape operation and no value-materialization API.
// Arrow currently has no public levels-only page decoder; ReadRecords therefore advances its
// private RecordReader, after which binary builder chunks are immediately released and only the
// copied level vectors survive. Keeping this exception isolated prevents Arrow arrays/builders,
// ParquetLeafBatch, and decoded-value views from leaking back into the scan reader contract.
class CountColumnReader {
public:
    static Status create(std::shared_ptr<::parquet::RowGroupReader> row_group,
                         const ParquetColumnSchema& root_schema,
                         const format::LocalColumnIndex* projection,
                         ParquetColumnReaderProfile profile,
                         std::unique_ptr<CountColumnReader>* reader);

    Status skip(int64_t rows);
    Status read_levels(int64_t rows, int64_t* rows_read);

    const std::vector<int16_t>& definition_levels() const { return _definition_levels; }
    const std::vector<int16_t>& repetition_levels() const { return _repetition_levels; }
    int64_t levels_written() const { return _levels_written; }

private:
    CountColumnReader(const ParquetColumnSchema& leaf_schema,
                      std::shared_ptr<::parquet::internal::RecordReader> record_reader,
                      ParquetColumnReaderProfile profile);

    Status release_binary_builder();

    const ParquetColumnSchema& _leaf_schema;
    std::shared_ptr<::parquet::internal::RecordReader> _record_reader;
    ParquetColumnReaderProfile _profile;
    std::string _name;
    std::vector<int16_t> _definition_levels;
    std::vector<int16_t> _repetition_levels;
    int64_t _levels_written = 0;
};

} // namespace doris::format::parquet
