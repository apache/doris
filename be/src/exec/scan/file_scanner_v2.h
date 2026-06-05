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
#include <string>
#include <unordered_map>
#include <vector>

#include "common/factory_creator.h"
#include "common/status.h"
#include "core/block/block.h"
#include "exec/operator/file_scan_operator.h"
#include "exec/scan/scanner.h"
#include "exec/scan/split_source_connector.h"
#include "exprs/vexpr_fwd.h"
#include "format/reader/column_mapper.h"
#include "format/reader/table_reader.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/io_common.h"
#include "runtime/runtime_profile.h"

namespace doris {

class RuntimeState;
class SlotDescriptor;
class TFileRangeDesc;
class TFileScanRangeParams;
class ShardedKVCache;

class FileScannerV2 final : public Scanner {
    ENABLE_FACTORY_CREATOR(FileScannerV2);

public:
    static constexpr const char* NAME = "FileScannerV2";

    static bool is_supported(const TFileScanRangeParams& params, const TFileRangeDesc& range);
#ifdef BE_TEST
    static Status TEST_build_nested_children_from_access_paths(
            reader::TableColumnDefinition* column,
            const std::vector<TColumnAccessPath>& access_paths);
    static Status TEST_build_nested_children_from_access_paths(
            reader::TableColumnDefinition* column,
            const std::vector<TColumnAccessPath>& access_paths,
            const reader::TableColumnDefinition* schema_column);
#endif

    FileScannerV2(RuntimeState* state, FileScanLocalState* parent, int64_t limit,
                  std::shared_ptr<SplitSourceConnector> split_source, RuntimeProfile* profile,
                  ShardedKVCache* kv_cache,
                  const std::unordered_map<std::string, int>* colname_to_slot_id);

    Status init(RuntimeState* state, const VExprContextSPtrs& conjuncts) override;
    Status _open_impl(RuntimeState* state) override;
    Status close(RuntimeState* state) override;
    void try_stop() override;
    std::string get_name() override { return FileScannerV2::NAME; }
    std::string get_current_scan_range_name() override { return _current_range_path; }
    void update_realtime_counters() override;

protected:
    Status _get_block_impl(RuntimeState* state, Block* block, bool* eof) override;

private:
    TFileFormatType::type _get_current_format_type() const;
    Status _init_io_ctx();
    Status _init_expr_ctxes();
    Status _prepare_next_split(bool* eos);
    Status _create_table_reader(const TFileRangeDesc& range);
    Status _create_table_reader_for_format(const TFileRangeDesc& range);
    Status _prepare_table_reader_split(const TFileRangeDesc& range);
    Status _generate_partition_values(const TFileRangeDesc& range,
                                      std::map<std::string, Field>* partition_values) const;
    Status _parse_partition_value(const SlotDescriptor* slot_desc, const std::string& value,
                                  bool is_null, Field* field) const;
    Status _build_projected_columns();
    Status _build_default_expr(const TFileScanSlotInfo& slot_info, VExprContextSPtr* ctx) const;
    static reader::TableColumnDefinition _build_table_column(const SlotDescriptor* slot_desc);
    Status _build_table_column_predicates(reader::TableColumnPredicates* predicates) const;
    static Status _to_file_format(TFileFormatType::type format_type, reader::FileFormat* format);

    const TFileScanRangeParams* _params = nullptr;
    std::shared_ptr<SplitSourceConnector> _split_source;
    bool _first_scan_range = false;
    TFileRangeDesc _current_range;
    std::string _current_range_path;

    std::unique_ptr<reader::TableReader> _table_reader;
    std::vector<reader::TableColumnDefinition> _projected_columns;
    std::vector<int32_t> _projected_column_unique_ids;
    std::unordered_map<int32_t, const SlotDescriptor*> _slot_id_to_desc;
    std::unordered_map<std::string, const SlotDescriptor*> _partition_slot_descs;

    std::unique_ptr<io::FileCacheStatistics> _file_cache_statistics;
    std::unique_ptr<io::FileReaderStats> _file_reader_stats;
    std::shared_ptr<io::IOContext> _io_ctx;
    ShardedKVCache* _kv_cache = nullptr;

    RuntimeProfile::Counter* _get_block_timer = nullptr;
    RuntimeProfile::Counter* _file_counter = nullptr;
    RuntimeProfile::Counter* _file_read_bytes_counter = nullptr;
    RuntimeProfile::Counter* _file_read_calls_counter = nullptr;
    RuntimeProfile::Counter* _file_read_time_counter = nullptr;
};

} // namespace doris
