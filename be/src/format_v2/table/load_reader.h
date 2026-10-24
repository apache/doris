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

#include "format_v2/table_reader.h"

namespace doris::format {

// Adapts the existing file-load source/destination tuple protocol to a table reader.
// Source materialization uses TableReader; finalization returns the destination schema.
class LoadTableReader : public TableReader {
public:
    LoadTableReader(TupleDescriptor* source, const TupleDescriptor* destination);
    ~LoadTableReader() override;

    Status init(TableReadOptions&& options) override;
    Status prepare_split(const SplitReadOptions& options) override;
    Status refresh_conjuncts(VExprContextSPtrs conjuncts,
                             std::optional<uint64_t> digest = std::nullopt,
                             bool all_runtime_filters_applied = false) override;
    Status build_physical_splits(const FileScanSplit& source, std::vector<FileScanSplit>* splits,
                                 bool* was_split) override;
    bool current_split_uses_metadata_count() const override { return false; }
    Status abort_split() override;
    Status close() override;
    int64_t condition_cache_hit_count() const override { return 0; }

protected:
    Status finalize_chunk(Block* block, size_t rows) override;
    Status validate_file_mapping(const TableColumnMapper& mapper) const override;
    Status customize_file_scan_request(FileScanRequest* request) override;
    Status create_file_reader(std::unique_ptr<FileReader>* reader) override;
    TableColumnMappingMode mapping_mode() const override;

private:
    Status _init_source_columns();
    Status _materialize_missing_values();
    Status _convert(Block* output);
    Status _reject_row(size_t row, const std::string& reason);

    TupleDescriptor* _source_tuple;
    const TupleDescriptor* _destination_tuple;
    RowDescriptor _source_row;
    Block _source;
    std::vector<ColumnDefinition> _source_columns;
    std::vector<SlotDescriptor*> _source_file_slots;
    VExprContextSPtrs _pre_filters;
    VExprContextSPtrs _destination_exprs;
    std::map<size_t, VExprContextSPtr> _source_defaults;
    std::vector<int> _direct_source_positions;
    std::vector<int> _partition_positions;
    int _skip_bitmap_position = -1;
    int _sequence_map_uid = -1;
    int _sequence_uid = -1;
    bool _closed = false;
};

} // namespace doris::format
