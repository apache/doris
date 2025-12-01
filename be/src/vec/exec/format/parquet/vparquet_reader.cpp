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

#include "vparquet_reader.h"

#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/parquet_types.h>
#include <glog/logging.h>

#include <algorithm>
#include <functional>
#include <utility>

#include "common/status.h"
#include "exec/schema_scanner.h"
#include "io/file_factory.h"
#include "io/fs/buffered_reader.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/tracing_file_reader.h"
#include "parquet_predicate.h"
#include "parquet_thrift_util.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "util/slice.h"
#include "util/string_util.h"
#include "util/timezone_utils.h"
#include "vec/columns/column.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/exec/format/column_type_convert.h"
#include "vec/exec/format/parquet/parquet_block_split_bloom_filter.h"
#include "vec/exec/format/parquet/parquet_common.h"
#include "vec/exec/format/parquet/schema_desc.h"
#include "vec/exec/format/parquet/vparquet_file_metadata.h"
#include "vec/exec/format/parquet/vparquet_group_reader.h"
#include "vec/exec/format/parquet/vparquet_page_index.h"
#include "vec/exec/scan/file_scanner.h"
#include "vec/exprs/vbloom_predicate.h"
#include "vec/exprs/vdirect_in_predicate.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vin_predicate.h"
#include "vec/exprs/vruntimefilter_wrapper.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/exprs/vtopn_pred.h"

namespace cctz {
class time_zone;
} // namespace cctz
namespace doris {
class RowDescriptor;
class RuntimeState;
class SlotDescriptor;
class TupleDescriptor;
namespace io {
struct IOContext;
enum class FileCachePolicy : uint8_t;
} // namespace io
namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

#include "common/compile_check_begin.h"
ParquetReader::ParquetReader(RuntimeProfile* profile, const TFileScanRangeParams& params,
                             const TFileRangeDesc& range, size_t batch_size,
                             const cctz::time_zone* ctz, io::IOContext* io_ctx, RuntimeState* state,
                             FileMetaCache* meta_cache, bool enable_lazy_mat)
        : _profile(profile),
          _scan_params(params),
          _scan_range(range),
          _batch_size(std::max(batch_size, _MIN_BATCH_SIZE)),
          _range_start_offset(range.start_offset),
          _range_size(range.size),
          _ctz(ctz),
          _io_ctx(io_ctx),
          _state(state),
          _enable_lazy_mat(enable_lazy_mat),
          _enable_filter_by_min_max(
                  state == nullptr ? true
                                   : state->query_options().enable_parquet_filter_by_min_max),
          _enable_filter_by_bloom_filter(
                  state == nullptr ? true
                                   : state->query_options().enable_parquet_filter_by_bloom_filter) {
    _meta_cache = meta_cache;
    _init_profile();
    _init_system_properties();
    _init_file_description();
}

ParquetReader::ParquetReader(const TFileScanRangeParams& params, const TFileRangeDesc& range,
                             io::IOContext* io_ctx, RuntimeState* state, FileMetaCache* meta_cache,
                             bool enable_lazy_mat)
        : _profile(nullptr),
          _scan_params(params),
          _scan_range(range),
          _io_ctx(io_ctx),
          _state(state),
          _enable_lazy_mat(enable_lazy_mat),
          _enable_filter_by_min_max(
                  state == nullptr ? true
                                   : state->query_options().enable_parquet_filter_by_min_max),
          _enable_filter_by_bloom_filter(
                  state == nullptr ? true
                                   : state->query_options().enable_parquet_filter_by_bloom_filter) {
    _meta_cache = meta_cache;
    _init_system_properties();
    _init_file_description();
}

ParquetReader::~ParquetReader() {
    _close_internal();
}

#ifdef BE_TEST
// for unit test
void ParquetReader::set_file_reader(io::FileReaderSPtr file_reader) {
    _file_reader = file_reader;
    _tracing_file_reader = file_reader;
}
#endif

void ParquetReader::_init_profile() {
    if (_profile != nullptr) {
        static const char* parquet_profile = "ParquetReader";
        ADD_TIMER_WITH_LEVEL(_profile, parquet_profile, 1);

        _parquet_profile.filtered_row_groups = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "FilteredGroups", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.filtered_row_groups_by_min_max = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "FilteredGroupsByMinMax", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.filtered_row_groups_by_bloom_filter = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "FilteredGroupsByBloomFilter", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.to_read_row_groups = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "ReadGroups", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.filtered_group_rows = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "FilteredRowsByGroup", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.filtered_page_rows = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "FilteredRowsByPage", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.lazy_read_filtered_rows = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "FilteredRowsByLazyRead", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.filtered_bytes = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "FilteredBytes", TUnit::BYTES, parquet_profile, 1);
        _parquet_profile.raw_rows_read = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "RawRowsRead", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.to_read_bytes = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "ReadBytes", TUnit::BYTES, parquet_profile, 1);
        _parquet_profile.column_read_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "ColumnReadTime", parquet_profile, 1);
        _parquet_profile.parse_meta_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "ParseMetaTime", parquet_profile, 1);
        _parquet_profile.parse_footer_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "ParseFooterTime", parquet_profile, 1);
        _parquet_profile.open_file_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "FileOpenTime", parquet_profile, 1);
        _parquet_profile.open_file_num =
                ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "FileNum", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.page_index_read_calls =
                ADD_COUNTER_WITH_LEVEL(_profile, "PageIndexReadCalls", TUnit::UNIT, 1);
        _parquet_profile.page_index_filter_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "PageIndexFilterTime", parquet_profile, 1);
        _parquet_profile.read_page_index_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "PageIndexReadTime", parquet_profile, 1);
        _parquet_profile.parse_page_index_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "PageIndexParseTime", parquet_profile, 1);
        _parquet_profile.row_group_filter_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "RowGroupFilterTime", parquet_profile, 1);
        _parquet_profile.file_footer_read_calls =
                ADD_COUNTER_WITH_LEVEL(_profile, "FileFooterReadCalls", TUnit::UNIT, 1);
        _parquet_profile.file_footer_hit_cache =
                ADD_COUNTER_WITH_LEVEL(_profile, "FileFooterHitCache", TUnit::UNIT, 1);
        _parquet_profile.decompress_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "DecompressTime", parquet_profile, 1);
        _parquet_profile.decompress_cnt = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "DecompressCount", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.decode_header_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "DecodeHeaderTime", parquet_profile, 1);
        _parquet_profile.decode_value_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "DecodeValueTime", parquet_profile, 1);
        _parquet_profile.decode_dict_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "DecodeDictTime", parquet_profile, 1);
        _parquet_profile.decode_level_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "DecodeLevelTime", parquet_profile, 1);
        _parquet_profile.decode_null_map_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "DecodeNullMapTime", parquet_profile, 1);
        _parquet_profile.skip_page_header_num = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "SkipPageHeaderNum", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.parse_page_header_num = ADD_CHILD_COUNTER_WITH_LEVEL(
                _profile, "ParsePageHeaderNum", TUnit::UNIT, parquet_profile, 1);
        _parquet_profile.predicate_filter_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "PredicateFilterTime", parquet_profile, 1);
        _parquet_profile.dict_filter_rewrite_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "DictFilterRewriteTime", parquet_profile, 1);
        _parquet_profile.bloom_filter_read_time =
                ADD_CHILD_TIMER_WITH_LEVEL(_profile, "BloomFilterReadTime", parquet_profile, 1);
    }
}

Status ParquetReader::close() {
    _close_internal();
    return Status::OK();
}

void ParquetReader::_close_internal() {
    if (!_closed) {
        _closed = true;
    }
}

Status ParquetReader::_open_file() {
    if (UNLIKELY(_io_ctx && _io_ctx->should_stop)) {
        return Status::EndOfFile("stop");
    }
    if (_file_reader == nullptr) {
        SCOPED_RAW_TIMER(&_statistics.open_file_time);
        ++_statistics.open_file_num;
        _file_description.mtime =
                _scan_range.__isset.modification_time ? _scan_range.modification_time : 0;
        io::FileReaderOptions reader_options =
                FileFactory::get_reader_options(_state, _file_description);
        _file_reader = DORIS_TRY(io::DelegateReader::create_file_reader(
                _profile, _system_properties, _file_description, reader_options,
                io::DelegateReader::AccessMode::RANDOM, _io_ctx));
        _tracing_file_reader = _io_ctx ? std::make_shared<io::TracingFileReader>(
                                                 _file_reader, _io_ctx->file_reader_stats)
                                       : _file_reader;
    }

    if (_file_metadata == nullptr) {
        SCOPED_RAW_TIMER(&_statistics.parse_footer_time);
        if (_tracing_file_reader->size() <= sizeof(PARQUET_VERSION_NUMBER)) {
            // Some system may generate parquet file with only 4 bytes: PAR1
            // Should consider it as empty file.
            return Status::EndOfFile("open file failed, empty parquet file {} with size: {}",
                                     _scan_range.path, _tracing_file_reader->size());
        }
        size_t meta_size = 0;
        bool enable_mapping_varbinary = _scan_params.__isset.enable_mapping_varbinary
                                                ? _scan_params.enable_mapping_varbinary
                                                : false;
        if (_meta_cache == nullptr) {
            // wrap _file_metadata with unique ptr, so that it can be released finally.
            RETURN_IF_ERROR(parse_thrift_footer(_tracing_file_reader, &_file_metadata_ptr,
                                                &meta_size, _io_ctx, enable_mapping_varbinary));
            _file_metadata = _file_metadata_ptr.get();
            _column_statistics.read_bytes += meta_size;
            // parse magic number & parse meta data
            _statistics.file_footer_read_calls += 1;
        } else {
            const auto& file_meta_cache_key =
                    FileMetaCache::get_key(_tracing_file_reader, _file_description);
            if (!_meta_cache->lookup(file_meta_cache_key, &_meta_cache_handle)) {
                RETURN_IF_ERROR(parse_thrift_footer(_tracing_file_reader, &_file_metadata_ptr,
                                                    &meta_size, _io_ctx, enable_mapping_varbinary));
                // _file_metadata_ptr.release() : move control of _file_metadata to _meta_cache_handle
                _meta_cache->insert(file_meta_cache_key, _file_metadata_ptr.release(),
                                    &_meta_cache_handle);
                _file_metadata = _meta_cache_handle.data<FileMetaData>();
                _column_statistics.read_bytes += meta_size;
                _statistics.file_footer_read_calls += 1;
            } else {
                _statistics.file_footer_hit_cache++;
            }
            _file_metadata = _meta_cache_handle.data<FileMetaData>();
        }

        if (_file_metadata == nullptr) {
            return Status::InternalError("failed to get file meta data: {}",
                                         _file_description.path);
        }
        _column_statistics.read_bytes += meta_size;
        // parse magic number & parse meta data
        _column_statistics.read_calls += 1;
    }
    return Status::OK();
}

Status ParquetReader::get_file_metadata_schema(const FieldDescriptor** ptr) {
    RETURN_IF_ERROR(_open_file());
    DCHECK(_file_metadata != nullptr);
    *ptr = &_file_metadata->schema();
    return Status::OK();
}

void ParquetReader::_init_system_properties() {
    if (_scan_range.__isset.file_type) {
        // for compatibility
        _system_properties.system_type = _scan_range.file_type;
    } else {
        _system_properties.system_type = _scan_params.file_type;
    }
    _system_properties.properties = _scan_params.properties;
    _system_properties.hdfs_params = _scan_params.hdfs_params;
    if (_scan_params.__isset.broker_addresses) {
        _system_properties.broker_addresses.assign(_scan_params.broker_addresses.begin(),
                                                   _scan_params.broker_addresses.end());
    }
}

void ParquetReader::_init_file_description() {
    _file_description.path = _scan_range.path;
    _file_description.file_size = _scan_range.__isset.file_size ? _scan_range.file_size : -1;
    if (_scan_range.__isset.fs_name) {
        _file_description.fs_name = _scan_range.fs_name;
    }
}

Status ParquetReader::init_reader(
        const std::vector<std::string>& all_column_names, const VExprContextSPtrs& conjuncts,
        const TupleDescriptor* tuple_descriptor, const RowDescriptor* row_descriptor,
        const std::unordered_map<std::string, int>* colname_to_slot_id,
        const VExprContextSPtrs* not_single_slot_filter_conjuncts,
        const std::unordered_map<int, VExprContextSPtrs>* slot_id_to_filter_conjuncts,
        std::shared_ptr<TableSchemaChangeHelper::Node> table_info_node_ptr, bool filter_groups,
        const std::set<uint64_t>& column_ids, const std::set<uint64_t>& filter_column_ids) {
    _tuple_descriptor = tuple_descriptor;
    _row_descriptor = row_descriptor;
    _colname_to_slot_id = colname_to_slot_id;
    _not_single_slot_filter_conjuncts = not_single_slot_filter_conjuncts;
    _slot_id_to_filter_conjuncts = slot_id_to_filter_conjuncts;
    _table_info_node_ptr = table_info_node_ptr;
    _filter_groups = filter_groups;
    _column_ids = column_ids;
    _filter_column_ids = filter_column_ids;

    RETURN_IF_ERROR(_open_file());
    _t_metadata = &(_file_metadata->to_thrift());
    if (_file_metadata == nullptr) {
        return Status::InternalError("failed to init parquet reader, please open reader first");
    }

    SCOPED_RAW_TIMER(&_statistics.parse_meta_time);
    _total_groups = _t_metadata->row_groups.size();
    if (_total_groups == 0) {
        return Status::EndOfFile("init reader failed, empty parquet file: " + _scan_range.path);
    }
    _current_row_group_index = RowGroupReader::RowGroupIndex {-1, 0, 0};

    _table_column_names = &all_column_names;
    auto schema_desc = _file_metadata->schema();

    std::map<std::string, std::string> required_file_columns; //file column -> table column
    for (auto table_column_name : all_column_names) {
        if (_table_info_node_ptr->children_column_exists(table_column_name)) {
            required_file_columns.emplace(
                    _table_info_node_ptr->children_file_column_name(table_column_name),
                    table_column_name);
        } else {
            _missing_cols.emplace_back(table_column_name);
        }
    }
    for (int i = 0; i < schema_desc.size(); ++i) {
        const auto& name = schema_desc.get_column(i)->name;
        if (required_file_columns.contains(name)) {
            _read_file_columns.emplace_back(name);
            _read_table_columns.emplace_back(required_file_columns[name]);
            _read_table_columns_set.insert(required_file_columns[name]);
        }
    }
    // build column predicates for column lazy read
    _lazy_read_ctx.conjuncts = conjuncts;
    return Status::OK();
}

bool ParquetReader::_exists_in_file(const VSlotRef* slot_ref) const {
    // `_read_table_columns_set` is used to ensure that only columns actually read are subject to min-max filtering.
    // This primarily handles cases where partition columns also exist in a file. The reason it's not modified
    // in `_table_info_node_ptr` is that Iceberg、Hudi has inconsistent requirements for this node;
    // Iceberg partition evolution need read partition columns from a file.
    // hudi set `hoodie.datasource.write.drop.partition.columns=false` not need read partition columns from a file.
    return _table_info_node_ptr->children_column_exists(slot_ref->expr_name()) &&
           _read_table_columns_set.contains(slot_ref->expr_name());
}

bool ParquetReader::_type_matches(const VSlotRef* slot_ref) const {
    auto* slot = _tuple_descriptor->slots()[slot_ref->column_id()];
    auto table_col_type = remove_nullable(slot->type());

    const auto& file_col_name = _table_info_node_ptr->children_file_column_name(slot->col_name());
    const auto& file_col_type =
            remove_nullable(_file_metadata->schema().get_column(file_col_name)->data_type);

    return (table_col_type->get_primitive_type() == file_col_type->get_primitive_type()) &&
           !is_complex_type(table_col_type->get_primitive_type());
}

Status ParquetReader::set_fill_columns(
        const std::unordered_map<std::string, std::tuple<std::string, const SlotDescriptor*>>&
                partition_columns,
        const std::unordered_map<std::string, VExprContextSPtr>& missing_columns) {
    SCOPED_RAW_TIMER(&_statistics.parse_meta_time);
    // std::unordered_map<column_name, std::pair<col_id, slot_id>>
    std::unordered_map<std::string, std::pair<uint32_t, int>> predicate_columns;
    // visit_slot for lazy mat.
    std::function<void(VExpr * expr)> visit_slot = [&](VExpr* expr) {
        if (expr->is_slot_ref()) {
            VSlotRef* slot_ref = static_cast<VSlotRef*>(expr);
            auto expr_name = slot_ref->expr_name();
            predicate_columns.emplace(expr_name,
                                      std::make_pair(slot_ref->column_id(), slot_ref->slot_id()));
            if (slot_ref->column_id() == 0) {
                _lazy_read_ctx.resize_first_column = false;
            }
            return;
        }
        for (auto& child : expr->children()) {
            visit_slot(child.get());
        }
    };

    for (const auto& conjunct : _lazy_read_ctx.conjuncts) {
        auto expr = conjunct->root();

        if (expr->is_rf_wrapper()) {
            // REF: src/runtime_filter/runtime_filter_consumer.cpp
            VRuntimeFilterWrapper* runtime_filter = assert_cast<VRuntimeFilterWrapper*>(expr.get());

            auto filter_impl = runtime_filter->get_impl();
            visit_slot(filter_impl.get());

            // only support push down for filter row group : MAX_FILTER, MAX_FILTER, MINMAX_FILTER,  IN_FILTER
            if ((runtime_filter->node_type() == TExprNodeType::BINARY_PRED) &&
                (runtime_filter->op() == TExprOpcode::GE ||
                 runtime_filter->op() == TExprOpcode::LE)) {
                expr = filter_impl;
            } else if (runtime_filter->node_type() == TExprNodeType::IN_PRED &&
                       runtime_filter->op() == TExprOpcode::FILTER_IN) {
                VDirectInPredicate* direct_in_predicate =
                        assert_cast<VDirectInPredicate*>(filter_impl.get());

                int max_in_size =
                        _state->query_options().__isset.max_pushdown_conditions_per_column
                                ? _state->query_options().max_pushdown_conditions_per_column
                                : 1024;
                if (direct_in_predicate->get_set_func()->size() == 0 ||
                    direct_in_predicate->get_set_func()->size() > max_in_size) {
                    continue;
                }

                VExprSPtr new_in_slot = nullptr;
                if (direct_in_predicate->get_slot_in_expr(new_in_slot)) {
                    expr = new_in_slot;
                } else {
                    continue;
                }
            } else {
                continue;
            }
        } else if (VTopNPred* topn_pred = typeid_cast<VTopNPred*>(expr.get())) {
            // top runtime filter : only le && ge.
            DCHECK(topn_pred->children().size() > 0);
            visit_slot(topn_pred->children()[0].get());

            if (topn_pred->children()[0]->is_slot_ref()) {
                // can min-max filter row group and page index.
                // Since the filtering conditions for topn are dynamic, the filtering is
                // delayed until create next row group reader.
                _top_runtime_vexprs.emplace_back(expr);
            }
            continue;
        } else {
            visit_slot(expr.get());
        }

        if (check_expr_can_push_down(expr)) {
            _push_down_predicates.push_back(AndBlockColumnPredicate::create_unique());
            RETURN_IF_ERROR(convert_predicates({expr}, _useless_predicates,
                                               _push_down_predicates.back(), _arena));
        }
    }

    const FieldDescriptor& schema = _file_metadata->schema();

    for (auto& read_table_col : _read_table_columns) {
        _lazy_read_ctx.all_read_columns.emplace_back(read_table_col);

        auto file_column_name = _table_info_node_ptr->children_file_column_name(read_table_col);
        PrimitiveType column_type =
                schema.get_column(file_column_name)->data_type->get_primitive_type();
        if (is_complex_type(column_type)) {
            _lazy_read_ctx.has_complex_type = true;
        }
        if (predicate_columns.size() > 0) {
            auto iter = predicate_columns.find(read_table_col);
            if (iter == predicate_columns.end()) {
                _lazy_read_ctx.lazy_read_columns.emplace_back(read_table_col);
            } else {
                _lazy_read_ctx.predicate_columns.first.emplace_back(iter->first);
                _lazy_read_ctx.predicate_columns.second.emplace_back(iter->second.second);
                _lazy_read_ctx.all_predicate_col_ids.emplace_back(iter->second.first);
            }
        }
    }
    if (_row_id_column_iterator_pair.first != nullptr) {
        _lazy_read_ctx.all_predicate_col_ids.emplace_back(_row_id_column_iterator_pair.second);
    }

    for (auto& kv : partition_columns) {
        auto iter = predicate_columns.find(kv.first);
        if (iter == predicate_columns.end()) {
            _lazy_read_ctx.partition_columns.emplace(kv.first, kv.second);
        } else {
            _lazy_read_ctx.predicate_partition_columns.emplace(kv.first, kv.second);
            _lazy_read_ctx.all_predicate_col_ids.emplace_back(iter->second.first);
        }
    }

    for (auto& kv : missing_columns) {
        auto iter = predicate_columns.find(kv.first);
        if (iter == predicate_columns.end()) {
            _lazy_read_ctx.missing_columns.emplace(kv.first, kv.second);
        } else {
            //For check missing column :   missing column == xx, missing column is null,missing column is not null.
            if (_slot_id_to_filter_conjuncts->find(iter->second.second) !=
                _slot_id_to_filter_conjuncts->end()) {
                for (auto& ctx : _slot_id_to_filter_conjuncts->find(iter->second.second)->second) {
                    _lazy_read_ctx.missing_columns_conjuncts.emplace_back(ctx);
                }
            }

            _lazy_read_ctx.predicate_missing_columns.emplace(kv.first, kv.second);
            _lazy_read_ctx.all_predicate_col_ids.emplace_back(iter->second.first);
        }
    }

    if (_enable_lazy_mat && _lazy_read_ctx.predicate_columns.first.size() > 0 &&
        _lazy_read_ctx.lazy_read_columns.size() > 0) {
        _lazy_read_ctx.can_lazy_read = true;
    }

    if (!_lazy_read_ctx.can_lazy_read) {
        for (auto& kv : _lazy_read_ctx.predicate_partition_columns) {
            _lazy_read_ctx.partition_columns.emplace(kv.first, kv.second);
        }
        for (auto& kv : _lazy_read_ctx.predicate_missing_columns) {
            _lazy_read_ctx.missing_columns.emplace(kv.first, kv.second);
        }
    }

    if (_filter_groups && (_total_groups == 0 || _t_metadata->num_rows == 0 || _range_size < 0)) {
        return Status::EndOfFile("No row group to read");
    }
    _fill_all_columns = true;
    return Status::OK();
}

// init file reader and file metadata for parsing schema
Status ParquetReader::init_schema_reader() {
    RETURN_IF_ERROR(_open_file());
    _t_metadata = &(_file_metadata->to_thrift());
    return Status::OK();
}

Status ParquetReader::get_parsed_schema(std::vector<std::string>* col_names,
                                        std::vector<DataTypePtr>* col_types) {
    _total_groups = _t_metadata->row_groups.size();
    auto schema_desc = _file_metadata->schema();
    for (int i = 0; i < schema_desc.size(); ++i) {
        // Get the Column Reader for the boolean column
        col_names->emplace_back(schema_desc.get_column(i)->name);
        col_types->emplace_back(make_nullable(schema_desc.get_column(i)->data_type));
    }
    return Status::OK();
}

Status ParquetReader::get_columns(std::unordered_map<std::string, DataTypePtr>* name_to_type,
                                  std::unordered_set<std::string>* missing_cols) {
    const auto& schema_desc = _file_metadata->schema();
    std::unordered_set<std::string> column_names;
    schema_desc.get_column_names(&column_names);
    for (auto& name : column_names) {
        auto field = schema_desc.get_column(name);
        name_to_type->emplace(name, field->data_type);
    }
    for (auto& col : _missing_cols) {
        missing_cols->insert(col);
    }
    return Status::OK();
}

Status ParquetReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    if (_current_group_reader == nullptr || _row_group_eof) {
        Status st = _next_row_group_reader();
        if (!st.ok() && !st.is<ErrorCode::END_OF_FILE>()) {
            return st;
        }
        if (_current_group_reader == nullptr || _row_group_eof || st.is<ErrorCode::END_OF_FILE>()) {
            _current_group_reader.reset(nullptr);
            _row_group_eof = true;
            *read_rows = 0;
            *eof = true;
            return Status::OK();
        }
    }
    if (_push_down_agg_type == TPushAggOp::type::COUNT) {
        auto rows = std::min(_current_group_reader->get_remaining_rows(), (int64_t)_batch_size);

        _current_group_reader->set_remaining_rows(_current_group_reader->get_remaining_rows() -
                                                  rows);
        auto mutate_columns = block->mutate_columns();
        for (auto& col : mutate_columns) {
            col->resize(rows);
        }
        block->set_columns(std::move(mutate_columns));

        *read_rows = rows;
        if (_current_group_reader->get_remaining_rows() == 0) {
            _current_group_reader.reset(nullptr);
        }

        return Status::OK();
    }

    SCOPED_RAW_TIMER(&_statistics.column_read_time);
    Status batch_st =
            _current_group_reader->next_batch(block, _batch_size, read_rows, &_row_group_eof);
    if (batch_st.is<ErrorCode::END_OF_FILE>()) {
        block->clear_column_data();
        _current_group_reader.reset(nullptr);
        *read_rows = 0;
        *eof = true;
        return Status::OK();
    }

    if (!batch_st.ok()) {
        return Status::InternalError("Read parquet file {} failed, reason = {}", _scan_range.path,
                                     batch_st.to_string());
    }

    if (_row_group_eof) {
        auto column_st = _current_group_reader->statistics();
        _column_statistics.merge(column_st);
        _statistics.lazy_read_filtered_rows += _current_group_reader->lazy_read_filtered_rows();
        _statistics.predicate_filter_time += _current_group_reader->predicate_filter_time();
        _statistics.dict_filter_rewrite_time += _current_group_reader->dict_filter_rewrite_time();
        if (_current_row_group_index.row_group_id + 1 == _total_groups) {
            *eof = true;
        } else {
            *eof = false;
        }
    }
    return Status::OK();
}

RowGroupReader::PositionDeleteContext ParquetReader::_get_position_delete_ctx(
        const tparquet::RowGroup& row_group, const RowGroupReader::RowGroupIndex& row_group_index) {
    if (_delete_rows == nullptr) {
        return RowGroupReader::PositionDeleteContext(row_group.num_rows, row_group_index.first_row);
    }
    const int64_t* delete_rows = &(*_delete_rows)[0];
    const int64_t* delete_rows_end = delete_rows + _delete_rows->size();
    const int64_t* start_pos = std::lower_bound(delete_rows + _delete_rows_index, delete_rows_end,
                                                row_group_index.first_row);
    int64_t start_index = start_pos - delete_rows;
    const int64_t* end_pos = std::lower_bound(start_pos, delete_rows_end, row_group_index.last_row);
    int64_t end_index = end_pos - delete_rows;
    _delete_rows_index = end_index;
    return RowGroupReader::PositionDeleteContext(*_delete_rows, row_group.num_rows,
                                                 row_group_index.first_row, start_index, end_index);
}

Status ParquetReader::_next_row_group_reader() {
    if (_current_group_reader != nullptr) {
        _current_group_reader->collect_profile_before_close();
    }

    RowRanges candidate_row_ranges;
    while (++_current_row_group_index.row_group_id < _total_groups) {
        const auto& row_group = _t_metadata->row_groups[_current_row_group_index.row_group_id];
        _current_row_group_index.first_row = _current_row_group_index.last_row;
        _current_row_group_index.last_row = _current_row_group_index.last_row + row_group.num_rows;

        if (_filter_groups && _is_misaligned_range_group(row_group)) {
            continue;
        }

        size_t before_predicate_size = _push_down_predicates.size();
        _push_down_predicates.reserve(before_predicate_size + _top_runtime_vexprs.size());
        for (const auto& vexpr : _top_runtime_vexprs) {
            VTopNPred* topn_pred = assert_cast<VTopNPred*>(vexpr.get());
            VExprSPtr binary_expr;
            if (topn_pred->get_binary_expr(binary_expr)) {
                // for min-max filter.
                if (check_expr_can_push_down(binary_expr)) {
                    _push_down_predicates.push_back(AndBlockColumnPredicate::create_unique());
                    RETURN_IF_ERROR(convert_predicates({binary_expr}, _useless_predicates,
                                                       _push_down_predicates.back(), _arena));
                }
            }
        }

        candidate_row_ranges.clear();
        // The range of lines to be read is determined by the push down predicate.
        RETURN_IF_ERROR(_process_min_max_bloom_filter(
                _current_row_group_index, row_group, _push_down_predicates, &candidate_row_ranges));

        _push_down_predicates.resize(before_predicate_size);

        std::function<int64_t(const FieldSchema*)> column_compressed_size =
                [&row_group, &column_compressed_size](const FieldSchema* field) -> int64_t {
            if (field->physical_column_index >= 0) {
                int parquet_col_id = field->physical_column_index;
                if (row_group.columns[parquet_col_id].__isset.meta_data) {
                    return row_group.columns[parquet_col_id].meta_data.total_compressed_size;
                }
                return 0;
            }
            int64_t size = 0;
            for (const FieldSchema& child : field->children) {
                size += column_compressed_size(&child);
            }
            return size;
        };
        int64_t group_size = 0; // only calculate the needed columns
        for (auto& read_col : _read_file_columns) {
            const FieldSchema* field = _file_metadata->schema().get_column(read_col);
            group_size += column_compressed_size(field);
        }

        _statistics.read_rows += candidate_row_ranges.count();
        if (_io_ctx) {
            _io_ctx->file_reader_stats->read_rows += candidate_row_ranges.count();
        }

        if (candidate_row_ranges.count() != 0) {
            // need read this row group.
            _statistics.read_row_groups++;
            _statistics.read_bytes += group_size;

            _statistics.filtered_page_rows += row_group.num_rows - candidate_row_ranges.count();
            break;
        } else {
            // this row group be filtered.
            _statistics.filtered_row_groups++;
            _statistics.filtered_bytes += group_size;
            _statistics.filtered_group_rows += row_group.num_rows;
        }
    }

    if (_current_row_group_index.row_group_id == _total_groups) {
        _row_group_eof = true;
        _current_group_reader.reset(nullptr);
        return Status::EndOfFile("No next RowGroupReader");
    }

    // process page index and generate the ranges to read
    auto& row_group = _t_metadata->row_groups[_current_row_group_index.row_group_id];

    RowGroupReader::PositionDeleteContext position_delete_ctx =
            _get_position_delete_ctx(row_group, _current_row_group_index);
    io::FileReaderSPtr group_file_reader;
    if (typeid_cast<io::InMemoryFileReader*>(_file_reader.get())) {
        // InMemoryFileReader has the ability to merge small IO
        group_file_reader = _file_reader;
    } else {
        size_t avg_io_size = 0;
        const std::vector<io::PrefetchRange> io_ranges =
                _generate_random_access_ranges(_current_row_group_index, &avg_io_size);
        int64_t merged_read_slice_size = -1;
        if (_state != nullptr && _state->query_options().__isset.merge_read_slice_size) {
            merged_read_slice_size = _state->query_options().merge_read_slice_size;
        }
        // The underlying page reader will prefetch data in column.
        // Using both MergeRangeFileReader and BufferedStreamReader simultaneously would waste a lot of memory.
        group_file_reader =
                avg_io_size < io::MergeRangeFileReader::SMALL_IO
                        ? std::make_shared<io::MergeRangeFileReader>(
                                  _profile, _file_reader, io_ranges, merged_read_slice_size)
                        : _file_reader;
    }
    _current_group_reader.reset(new RowGroupReader(
            _io_ctx ? std::make_shared<io::TracingFileReader>(group_file_reader,
                                                              _io_ctx->file_reader_stats)
                    : group_file_reader,
            _read_table_columns, _current_row_group_index.row_group_id, row_group, _ctz, _io_ctx,
            position_delete_ctx, _lazy_read_ctx, _state, _column_ids, _filter_column_ids));
    _row_group_eof = false;

    _current_group_reader->set_current_row_group_idx(_current_row_group_index);
    _current_group_reader->set_row_id_column_iterator(_row_id_column_iterator_pair);

    _current_group_reader->_table_info_node_ptr = _table_info_node_ptr;
    return _current_group_reader->init(_file_metadata->schema(), candidate_row_ranges, _col_offsets,
                                       _tuple_descriptor, _row_descriptor, _colname_to_slot_id,
                                       _not_single_slot_filter_conjuncts,
                                       _slot_id_to_filter_conjuncts);
}

std::vector<io::PrefetchRange> ParquetReader::_generate_random_access_ranges(
        const RowGroupReader::RowGroupIndex& group, size_t* avg_io_size) {
    std::vector<io::PrefetchRange> result;
    int64_t last_chunk_end = -1;
    size_t total_io_size = 0;
    std::function<void(const FieldSchema*, const tparquet::RowGroup&)> scalar_range =
            [&](const FieldSchema* field, const tparquet::RowGroup& row_group) {
                if (_column_ids.empty() ||
                    _column_ids.find(field->get_column_id()) != _column_ids.end()) {
                    if (field->data_type->get_primitive_type() == TYPE_ARRAY) {
                        scalar_range(&field->children[0], row_group);
                    } else if (field->data_type->get_primitive_type() == TYPE_MAP) {
                        scalar_range(&field->children[0], row_group);
                        scalar_range(&field->children[1], row_group);
                    } else if (field->data_type->get_primitive_type() == TYPE_STRUCT) {
                        for (int i = 0; i < field->children.size(); ++i) {
                            scalar_range(&field->children[i], row_group);
                        }
                    } else {
                        const tparquet::ColumnChunk& chunk =
                                row_group.columns[field->physical_column_index];
                        auto& chunk_meta = chunk.meta_data;
                        int64_t chunk_start = has_dict_page(chunk_meta)
                                                      ? chunk_meta.dictionary_page_offset
                                                      : chunk_meta.data_page_offset;
                        int64_t chunk_end = chunk_start + chunk_meta.total_compressed_size;
                        DCHECK_GE(chunk_start, last_chunk_end);
                        result.emplace_back(chunk_start, chunk_end);
                        total_io_size += chunk_meta.total_compressed_size;
                        last_chunk_end = chunk_end;
                    }
                }
            };
    const tparquet::RowGroup& row_group = _t_metadata->row_groups[group.row_group_id];
    for (const auto& read_col : _read_file_columns) {
        const FieldSchema* field = _file_metadata->schema().get_column(read_col);
        scalar_range(field, row_group);
    }
    if (!result.empty()) {
        *avg_io_size = total_io_size / result.size();
    }
    return result;
}

bool ParquetReader::_is_misaligned_range_group(const tparquet::RowGroup& row_group) {
    int64_t start_offset = _get_column_start_offset(row_group.columns[0].meta_data);

    auto& last_column = row_group.columns[row_group.columns.size() - 1].meta_data;
    int64_t end_offset = _get_column_start_offset(last_column) + last_column.total_compressed_size;

    int64_t row_group_mid = start_offset + (end_offset - start_offset) / 2;
    if (!(row_group_mid >= _range_start_offset &&
          row_group_mid < _range_start_offset + _range_size)) {
        return true;
    }
    return false;
}

Status ParquetReader::_process_page_index_filter(
        const tparquet::RowGroup& row_group, const RowGroupReader::RowGroupIndex& row_group_index,
        const std::vector<std::unique_ptr<MutilColumnBlockPredicate>>& push_down_pred,
        RowRanges* candidate_row_ranges) {
    if (UNLIKELY(_io_ctx && _io_ctx->should_stop)) {
        return Status::EndOfFile("stop");
    }

    std::function<void()> read_whole_row_group = [&]() {
        candidate_row_ranges->add(RowRange {0, row_group.num_rows});
    };

    // Check if the page index is available and if it exists.
    PageIndex page_index;
    if (!config::enable_parquet_page_index || _colname_to_slot_id == nullptr ||
        !page_index.check_and_get_page_index_ranges(row_group.columns)) {
        read_whole_row_group();
        return Status::OK();
    }

    auto parse_offset_index = [&]() -> Status {
        std::vector<uint8_t> off_index_buff(page_index._offset_index_size);
        Slice res(off_index_buff.data(), page_index._offset_index_size);
        size_t bytes_read = 0;
        {
            SCOPED_RAW_TIMER(&_statistics.read_page_index_time);
            RETURN_IF_ERROR(_tracing_file_reader->read_at(page_index._offset_index_start, res,
                                                          &bytes_read, _io_ctx));
        }
        _column_statistics.read_bytes += bytes_read;
        _column_statistics.page_index_read_calls++;
        _col_offsets.clear();
        for (size_t idx = 0; idx < _read_table_columns.size(); idx++) {
            const auto& read_table_col = _read_table_columns[idx];
            const auto& read_file_col = _read_file_columns[idx];
            if (!_colname_to_slot_id->contains(read_table_col)) {
                // equal delete may add column to read_table_col, but this column no slot_id.
                continue;
            }

            int parquet_col_id =
                    _file_metadata->schema().get_column(read_file_col)->physical_column_index;
            if (parquet_col_id < 0) {
                // complex type, not support page index yet.
                continue;
            }
            auto& chunk = row_group.columns[parquet_col_id];
            if (chunk.offset_index_length == 0) [[unlikely]] {
                continue;
            }
            tparquet::OffsetIndex offset_index;
            SCOPED_RAW_TIMER(&_statistics.parse_page_index_time);
            RETURN_IF_ERROR(
                    page_index.parse_offset_index(chunk, off_index_buff.data(), &offset_index));
            _col_offsets[parquet_col_id] = offset_index;
        }
        return Status::OK();
    };

    // from https://github.com/apache/doris/pull/55795
    RETURN_IF_ERROR(parse_offset_index());

    // Check if page index is needed for min-max filter.
    if (!_enable_filter_by_min_max || _lazy_read_ctx.has_complex_type || push_down_pred.empty()) {
        read_whole_row_group();
        return Status::OK();
    }

    // read column index.
    std::vector<uint8_t> col_index_buff(page_index._column_index_size);
    size_t bytes_read = 0;
    Slice result(col_index_buff.data(), page_index._column_index_size);
    {
        SCOPED_RAW_TIMER(&_statistics.read_page_index_time);
        RETURN_IF_ERROR(_tracing_file_reader->read_at(page_index._column_index_start, result,
                                                      &bytes_read, _io_ctx));
    }
    _column_statistics.read_bytes += bytes_read;
    _column_statistics.page_index_read_calls++;

    SCOPED_RAW_TIMER(&_statistics.page_index_filter_time);

    // Construct a cacheable page index structure to avoid repeatedly reading the page index of the same column.
    ParquetPredicate::CachedPageIndexStat cached_page_index;
    cached_page_index.ctz = _ctz;
    std::function<bool(ParquetPredicate::PageIndexStat**, int)> get_stat_func =
            [&](ParquetPredicate::PageIndexStat** ans, const int cid) -> bool {
        if (cached_page_index.stats.contains(cid)) {
            *ans = &cached_page_index.stats[cid];
            return (*ans)->available;
        }
        cached_page_index.stats.emplace(cid, ParquetPredicate::PageIndexStat {});
        auto& sig_stat = cached_page_index.stats[cid];

        auto* slot = _tuple_descriptor->slots()[cid];
        if (!_table_info_node_ptr->children_column_exists(slot->col_name())) {
            // table column not exist in file, may be schema change.
            return false;
        }

        const auto& file_col_name =
                _table_info_node_ptr->children_file_column_name(slot->col_name());
        const FieldSchema* col_schema = _file_metadata->schema().get_column(file_col_name);
        int parquet_col_id = col_schema->physical_column_index;

        if (parquet_col_id < 0) {
            // complex type, not support page index yet.
            return false;
        }
        if (!_col_offsets.contains(parquet_col_id)) {
            // If the file contains partition columns and the query applies filters on those
            // partition columns, then reading the page index is unnecessary.
            return false;
        }

        auto& column_chunk = row_group.columns[parquet_col_id];
        if (column_chunk.column_index_length == 0 || column_chunk.offset_index_length == 0) {
            // column no page index.
            return false;
        }

        tparquet::ColumnIndex column_index;
        {
            SCOPED_RAW_TIMER(&_statistics.parse_page_index_time);
            RETURN_IF_ERROR(page_index.parse_column_index(column_chunk, col_index_buff.data(),
                                                          &column_index));
        }
        const int64_t num_of_pages = column_index.null_pages.size();
        if (num_of_pages <= 0) [[unlikely]] {
            // no page. (maybe this row group no data.)
            return false;
        }
        DCHECK_EQ(column_index.min_values.size(), column_index.max_values.size());
        if (!column_index.__isset.null_counts) {
            // not set null or null counts;
            return false;
        }

        auto& offset_index = _col_offsets[parquet_col_id];
        const auto& page_locations = offset_index.page_locations;

        sig_stat.col_schema = col_schema;
        sig_stat.num_of_pages = num_of_pages;
        sig_stat.encoded_min_value = column_index.min_values;
        sig_stat.encoded_max_value = column_index.max_values;
        sig_stat.is_all_null.resize(num_of_pages);
        sig_stat.has_null.resize(num_of_pages);
        sig_stat.ranges.resize(num_of_pages);

        for (int page_id = 0; page_id < num_of_pages; page_id++) {
            sig_stat.is_all_null[page_id] = column_index.null_pages[page_id];
            sig_stat.has_null[page_id] = column_index.null_counts[page_id] > 0;

            int64_t from = page_locations[page_id].first_row_index;
            int64_t to = 0;
            if (page_id == page_locations.size() - 1) {
                to = row_group_index.last_row;
            } else {
                to = page_locations[page_id + 1].first_row_index;
            }
            sig_stat.ranges[page_id] = RowRange {from, to};
        }

        sig_stat.available = true;
        *ans = &sig_stat;
        return true;
    };
    cached_page_index.get_stat_func = get_stat_func;

    candidate_row_ranges->add({0, row_group.num_rows});
    for (const auto& predicate : push_down_pred) {
        RowRanges tmp_row_range;
        if (!predicate->evaluate_and(&cached_page_index, &tmp_row_range)) {
            // no need read this row group.
            candidate_row_ranges->clear();
            return Status::OK();
        }
        RowRanges::ranges_intersection(*candidate_row_ranges, tmp_row_range, candidate_row_ranges);
    }
    return Status::OK();
}

Status ParquetReader::_process_min_max_bloom_filter(
        const RowGroupReader::RowGroupIndex& row_group_index, const tparquet::RowGroup& row_group,
        const std::vector<std::unique_ptr<MutilColumnBlockPredicate>>& push_down_pred,
        RowRanges* row_ranges) {
    SCOPED_RAW_TIMER(&_statistics.row_group_filter_time);
    if (!_filter_groups) {
        // No row group filtering is needed;
        // for example, Iceberg reads position delete files.
        row_ranges->add({0, row_group.num_rows});
        return Status::OK();
    }

    if (_read_by_rows) {
        auto group_start = row_group_index.first_row;
        auto group_end = row_group_index.last_row;

        while (!_row_ids.empty()) {
            auto v = _row_ids.front();
            if (v < group_start) {
                continue;
            } else if (v < group_end) {
                row_ranges->add(RowRange {v - group_start, v - group_start + 1});
                _row_ids.pop_front();
            } else {
                break;
            }
        }
    } else {
        bool filter_this_row_group = false;
        bool filtered_by_min_max = false;
        bool filtered_by_bloom_filter = false;
        RETURN_IF_ERROR(_process_column_stat_filter(row_group, push_down_pred,
                                                    &filter_this_row_group, &filtered_by_min_max,
                                                    &filtered_by_bloom_filter));
        // Update statistics based on filter type
        if (filter_this_row_group) {
            if (filtered_by_min_max) {
                _statistics.filtered_row_groups_by_min_max++;
            }
            if (filtered_by_bloom_filter) {
                _statistics.filtered_row_groups_by_bloom_filter++;
            }
        }

        if (!filter_this_row_group) {
            RETURN_IF_ERROR(_process_page_index_filter(row_group, row_group_index, push_down_pred,
                                                       row_ranges));
        }
    }

    return Status::OK();
}

Status ParquetReader::_process_column_stat_filter(
        const tparquet::RowGroup& row_group,
        const std::vector<std::unique_ptr<MutilColumnBlockPredicate>>& push_down_pred,
        bool* filter_group, bool* filtered_by_min_max, bool* filtered_by_bloom_filter) {
    // If both filters are disabled, skip filtering
    if (!_enable_filter_by_min_max && !_enable_filter_by_bloom_filter) {
        return Status::OK();
    }

    // Cache bloom filters for each column to avoid reading the same bloom filter multiple times
    // when there are multiple predicates on the same column
    std::unordered_map<int, std::unique_ptr<vectorized::ParquetBlockSplitBloomFilter>>
            bloom_filter_cache;

    // Initialize output parameters
    *filtered_by_min_max = false;
    *filtered_by_bloom_filter = false;

    for (const auto& predicate : _push_down_predicates) {
        std::function<bool(ParquetPredicate::ColumnStat*, int)> get_stat_func =
                [&](ParquetPredicate::ColumnStat* stat, const int cid) {
                    // Check if min-max filter is enabled
                    if (!_enable_filter_by_min_max) {
                        return false;
                    }
                    auto* slot = _tuple_descriptor->slots()[cid];
                    if (!_table_info_node_ptr->children_column_exists(slot->col_name())) {
                        return false;
                    }
                    const auto& file_col_name =
                            _table_info_node_ptr->children_file_column_name(slot->col_name());
                    const FieldSchema* col_schema =
                            _file_metadata->schema().get_column(file_col_name);
                    int parquet_col_id = col_schema->physical_column_index;
                    auto meta_data = row_group.columns[parquet_col_id].meta_data;
                    stat->col_schema = col_schema;
                    return ParquetPredicate::read_column_stats(col_schema, meta_data,
                                                               &_ignored_stats,
                                                               _t_metadata->created_by, stat)
                            .ok();
                };
        std::function<bool(ParquetPredicate::ColumnStat*, int)> get_bloom_filter_func =
                [&](ParquetPredicate::ColumnStat* stat, const int cid) {
                    auto* slot = _tuple_descriptor->slots()[cid];
                    if (!_table_info_node_ptr->children_column_exists(slot->col_name())) {
                        return false;
                    }
                    const auto& file_col_name =
                            _table_info_node_ptr->children_file_column_name(slot->col_name());
                    const FieldSchema* col_schema =
                            _file_metadata->schema().get_column(file_col_name);
                    int parquet_col_id = col_schema->physical_column_index;
                    auto meta_data = row_group.columns[parquet_col_id].meta_data;
                    if (!meta_data.__isset.bloom_filter_offset) {
                        return false;
                    }
                    auto primitive_type =
                            remove_nullable(col_schema->data_type)->get_primitive_type();
                    if (!ParquetPredicate::bloom_filter_supported(primitive_type)) {
                        return false;
                    }

                    // Check if bloom filter is enabled
                    if (!_enable_filter_by_bloom_filter) {
                        return false;
                    }

                    // Check cache first
                    auto cache_iter = bloom_filter_cache.find(parquet_col_id);
                    if (cache_iter != bloom_filter_cache.end()) {
                        // Bloom filter already loaded for this column, reuse it
                        stat->bloom_filter = std::move(cache_iter->second);
                        bloom_filter_cache.erase(cache_iter);
                        return stat->bloom_filter != nullptr;
                    }

                    if (!stat->bloom_filter) {
                        SCOPED_RAW_TIMER(&_statistics.bloom_filter_read_time);
                        auto st = ParquetPredicate::read_bloom_filter(
                                meta_data, _tracing_file_reader, _io_ctx, stat);
                        if (!st.ok()) {
                            LOG(WARNING) << "Failed to read bloom filter for column "
                                         << col_schema->name << " in file " << _scan_range.path
                                         << ", status: " << st.to_string();
                            stat->bloom_filter.reset();
                            return false;
                        }
                    }
                    return stat->bloom_filter != nullptr;
                };
        ParquetPredicate::ColumnStat stat;
        stat.ctz = _ctz;
        stat.get_stat_func = &get_stat_func;
        stat.get_bloom_filter_func = &get_bloom_filter_func;

        if (!predicate->evaluate_and(&stat)) {
            *filter_group = true;

            // Track which filter was used for filtering
            // If bloom filter was loaded, it means bloom filter was used
            if (stat.bloom_filter) {
                *filtered_by_bloom_filter = true;
            }
            // If col_schema was set but no bloom filter, it means min-max stats were used
            if (stat.col_schema && !stat.bloom_filter) {
                *filtered_by_min_max = true;
            }

            return Status::OK();
        }

        // After evaluating, if the bloom filter was used, cache it for subsequent predicates
        if (stat.bloom_filter) {
            // Find the column id for caching
            for (auto* slot : _tuple_descriptor->slots()) {
                if (_table_info_node_ptr->children_column_exists(slot->col_name())) {
                    const auto& file_col_name =
                            _table_info_node_ptr->children_file_column_name(slot->col_name());
                    const FieldSchema* col_schema =
                            _file_metadata->schema().get_column(file_col_name);
                    int parquet_col_id = col_schema->physical_column_index;
                    if (stat.col_schema == col_schema) {
                        bloom_filter_cache[parquet_col_id] = std::move(stat.bloom_filter);
                        break;
                    }
                }
            }
        }
    }

    // Update filter statistics if this row group was not filtered
    // The statistics will be updated in _init_row_groups when filter_group is true
    return Status::OK();
}

int64_t ParquetReader::_get_column_start_offset(const tparquet::ColumnMetaData& column) {
    return has_dict_page(column) ? column.dictionary_page_offset : column.data_page_offset;
}

void ParquetReader::_collect_profile() {
    if (_profile == nullptr) {
        return;
    }

    if (_current_group_reader != nullptr) {
        _current_group_reader->collect_profile_before_close();
    }
    COUNTER_UPDATE(_parquet_profile.filtered_row_groups, _statistics.filtered_row_groups);
    COUNTER_UPDATE(_parquet_profile.filtered_row_groups_by_min_max,
                   _statistics.filtered_row_groups_by_min_max);
    COUNTER_UPDATE(_parquet_profile.filtered_row_groups_by_bloom_filter,
                   _statistics.filtered_row_groups_by_bloom_filter);
    COUNTER_UPDATE(_parquet_profile.to_read_row_groups, _statistics.read_row_groups);
    COUNTER_UPDATE(_parquet_profile.filtered_group_rows, _statistics.filtered_group_rows);
    COUNTER_UPDATE(_parquet_profile.filtered_page_rows, _statistics.filtered_page_rows);
    COUNTER_UPDATE(_parquet_profile.lazy_read_filtered_rows, _statistics.lazy_read_filtered_rows);
    COUNTER_UPDATE(_parquet_profile.filtered_bytes, _statistics.filtered_bytes);
    COUNTER_UPDATE(_parquet_profile.raw_rows_read, _statistics.read_rows);
    COUNTER_UPDATE(_parquet_profile.to_read_bytes, _statistics.read_bytes);
    COUNTER_UPDATE(_parquet_profile.column_read_time, _statistics.column_read_time);
    COUNTER_UPDATE(_parquet_profile.parse_meta_time, _statistics.parse_meta_time);
    COUNTER_UPDATE(_parquet_profile.parse_footer_time, _statistics.parse_footer_time);
    COUNTER_UPDATE(_parquet_profile.open_file_time, _statistics.open_file_time);
    COUNTER_UPDATE(_parquet_profile.open_file_num, _statistics.open_file_num);
    COUNTER_UPDATE(_parquet_profile.page_index_filter_time, _statistics.page_index_filter_time);
    COUNTER_UPDATE(_parquet_profile.read_page_index_time, _statistics.read_page_index_time);
    COUNTER_UPDATE(_parquet_profile.parse_page_index_time, _statistics.parse_page_index_time);
    COUNTER_UPDATE(_parquet_profile.row_group_filter_time, _statistics.row_group_filter_time);
    COUNTER_UPDATE(_parquet_profile.file_footer_read_calls, _statistics.file_footer_read_calls);
    COUNTER_UPDATE(_parquet_profile.file_footer_hit_cache, _statistics.file_footer_hit_cache);

    COUNTER_UPDATE(_parquet_profile.skip_page_header_num, _column_statistics.skip_page_header_num);
    COUNTER_UPDATE(_parquet_profile.parse_page_header_num,
                   _column_statistics.parse_page_header_num);
    COUNTER_UPDATE(_parquet_profile.predicate_filter_time, _statistics.predicate_filter_time);
    COUNTER_UPDATE(_parquet_profile.dict_filter_rewrite_time, _statistics.dict_filter_rewrite_time);
    COUNTER_UPDATE(_parquet_profile.bloom_filter_read_time, _statistics.bloom_filter_read_time);
    COUNTER_UPDATE(_parquet_profile.page_index_read_calls,
                   _column_statistics.page_index_read_calls);
    COUNTER_UPDATE(_parquet_profile.decompress_time, _column_statistics.decompress_time);
    COUNTER_UPDATE(_parquet_profile.decompress_cnt, _column_statistics.decompress_cnt);
    COUNTER_UPDATE(_parquet_profile.decode_header_time, _column_statistics.decode_header_time);
    COUNTER_UPDATE(_parquet_profile.decode_value_time, _column_statistics.decode_value_time);
    COUNTER_UPDATE(_parquet_profile.decode_dict_time, _column_statistics.decode_dict_time);
    COUNTER_UPDATE(_parquet_profile.decode_level_time, _column_statistics.decode_level_time);
    COUNTER_UPDATE(_parquet_profile.decode_null_map_time, _column_statistics.decode_null_map_time);
}

void ParquetReader::_collect_profile_before_close() {
    _collect_profile();
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
