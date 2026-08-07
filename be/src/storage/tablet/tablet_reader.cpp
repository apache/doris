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

#include "storage/tablet/tablet_reader.h"

#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/segment_v2.pb.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <memory>
#include <ostream>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "core/block/block.h"
#include "exec/common/variant_util.h"
#include "exprs/bloom_filter_func.h"
#include "exprs/create_predicate_function.h"
#include "exprs/hybrid_set.h"
#include "runtime/query_context.h"
#include "runtime/runtime_predicate.h"
#include "runtime/runtime_state.h"
#include "storage/delete/delete_handler.h"
#include "storage/index/bloom_filter/bloom_filter.h"
#include "storage/itoken_extractor.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/predicate/block_column_predicate.h"
#include "storage/predicate/column_predicate.h"
#include "storage/predicate/like_column_predicate.h"
#include "storage/predicate/predicate_creator.h"
#include "storage/row_cursor.h"
#include "storage/schema.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {
using namespace ErrorCode;

void TabletReader::ReaderParams::check_validation() const {
    if (UNLIKELY(version.first == -1 && is_segcompaction == false)) {
        throw Exception(Status::FatalError("version is not set. tablet={}", tablet->tablet_id()));
    }
}

Status TabletReader::init(const ReaderParams& read_params) {
    Status res = _init_params(read_params);
    if (!res.ok()) {
        LOG(WARNING) << "fail to init reader when init params. res:" << res
                     << ", tablet_id:" << read_params.tablet->tablet_id()
                     << ", schema_hash:" << read_params.tablet->schema_hash()
                     << ", reader type:" << int(read_params.reader_type)
                     << ", version:" << read_params.version;
    }
    return res;
}

void TabletReader::remove_delete_columns_from_access_paths(
        const DeleteHandler& delete_handler, const ReadSchema& read_schema,
        std::map<int32_t, TColumnAccessPaths>& all_access_paths) {
    auto delete_predicates = AndBlockColumnPredicate::create_shared();
    std::unordered_map<int32_t, std::vector<std::shared_ptr<const ColumnPredicate>>>
            del_predicates_for_zone_map;
    delete_handler.get_delete_conditions_after_version(0, delete_predicates.get(),
                                                       &del_predicates_for_zone_map);
    std::set<ColumnId> delete_column_ordinals;
    delete_predicates->get_all_column_ids(delete_column_ordinals);
    for (auto ordinal : delete_column_ordinals) {
        all_access_paths.erase(read_schema.column(ordinal)->unique_id());
    }
}

Status TabletReader::_capture_rs_readers(const ReaderParams& read_params) {
    SCOPED_RAW_TIMER(&_stats.tablet_reader_capture_rs_readers_timer_ns);
    if (read_params.rs_splits.empty()) {
        return Status::InternalError("fail to acquire data sources. tablet={}",
                                     _tablet->tablet_id());
    }

    bool eof = false;
    bool is_lower_key_included = _keys_param.start_key_include;
    bool is_upper_key_included = _keys_param.end_key_include;

    for (int i = 0; i < _keys_param.start_keys.size(); ++i) {
        // lower bound
        RowCursor& start_key = _keys_param.start_keys[i];
        RowCursor& end_key = _keys_param.end_keys[i];

        if (!is_lower_key_included) {
            if (compare_row_key(start_key, end_key) >= 0) {
                VLOG_NOTICE << "return EOF when lower key not include"
                            << ", start_key=" << start_key.to_string()
                            << ", end_key=" << end_key.to_string();
                eof = true;
                break;
            }
        } else {
            if (compare_row_key(start_key, end_key) > 0) {
                VLOG_NOTICE << "return EOF when lower key include="
                            << ", start_key=" << start_key.to_string()
                            << ", end_key=" << end_key.to_string();
                eof = true;
                break;
            }
        }

        _is_lower_keys_included.push_back(is_lower_key_included);
        _is_upper_keys_included.push_back(is_upper_key_included);
    }

    if (eof) {
        return Status::EndOfFile("reach end of scan range. tablet={}", _tablet->tablet_id());
    }

    bool need_ordered_result = true;
    if (read_params.reader_type == ReaderType::READER_QUERY) {
        if (_tablet_schema->keys_type() == DUP_KEYS) {
            // duplicated keys are allowed, no need to merge sort keys in rowset
            need_ordered_result = false;
        }
        if (_tablet_schema->keys_type() == UNIQUE_KEYS &&
            _tablet->enable_unique_key_merge_on_write()) {
            // unique keys with merge on write, no need to merge sort keys in rowset
            need_ordered_result = false;
        }
        if (_aggregation) {
            // compute engine will aggregate rows with the same key,
            // it's ok for rowset to return unordered result
            need_ordered_result = false;
        }

        if (_direct_mode) {
            // direct mode indicates that the storage layer does not need to merge,
            // it's ok for rowset to return unordered result
            need_ordered_result = false;
        }

        if (read_params.read_orderby_key) {
            need_ordered_result = true;
        }
    }

    _reader_context.reader_type = read_params.reader_type;
    _reader_context.read_row_binlog = read_params.read_row_binlog;
    _reader_context.version = read_params.version;
    _reader_context.tablet_schema = _tablet_schema;
    _reader_context.need_ordered_result = need_ordered_result;
    _reader_context.topn_filter_source_node_ids = read_params.topn_filter_source_node_ids;
    _reader_context.read_orderby_key_reverse = read_params.read_orderby_key_reverse;
    _reader_context.use_insert_order_when_same =
            read_params.use_insert_order_when_same || read_params.read_row_binlog;
    _reader_context.force_key_ordered_read = read_params.force_key_ordered_read;
    _reader_context.read_orderby_key_limit = read_params.read_orderby_key_limit;
    _reader_context.read_schema = _read_schema;
    _reader_context.read_orderby_key_columns =
            !_orderby_key_columns.empty() ? &_orderby_key_columns : nullptr;
    _reader_context.predicates = &_col_predicates;
    _reader_context.value_predicates = &_value_col_predicates;
    _reader_context.lower_bound_keys = &_keys_param.start_keys;
    _reader_context.is_lower_keys_included = &_is_lower_keys_included;
    _reader_context.upper_bound_keys = &_keys_param.end_keys;
    _reader_context.is_upper_keys_included = &_is_upper_keys_included;
    _reader_context.delete_handler = &_delete_handler;
    _reader_context.stats = &_stats;
    _reader_context.use_page_cache = read_params.use_page_cache;
    _reader_context.is_unique = tablet()->keys_type() == UNIQUE_KEYS;
    _reader_context.merged_rows = &_merged_rows;
    _reader_context.delete_bitmap = read_params.delete_bitmap;
    _reader_context.enable_unique_key_merge_on_write = tablet()->enable_unique_key_merge_on_write();
    _reader_context.enable_mor_value_predicate_pushdown =
            read_params.enable_mor_value_predicate_pushdown;
    _reader_context.record_rowids = read_params.record_rowids;
    _reader_context.rowid_conversion = read_params.rowid_conversion;
    _reader_context.is_key_column_group = read_params.is_key_column_group;
    _reader_context.common_expr_ctxs_push_down = read_params.common_expr_ctxs_push_down;
    _reader_context.output_columns = &read_params.output_columns;
    _reader_context.extra_columns = read_params.extra_columns;
    _reader_context.push_down_agg_type_opt = read_params.push_down_agg_type_opt;
    _reader_context.ttl_seconds = _tablet->ttl_seconds();
    _reader_context.score_runtime = read_params.score_runtime;
    _reader_context.collection_statistics = read_params.collection_statistics;

    _reader_context.virtual_column_exprs = read_params.virtual_column_exprs;
    _reader_context.ann_topn_runtime = read_params.ann_topn_runtime;

    _reader_context.condition_cache_digest = read_params.condition_cache_digest;
    _reader_context.all_access_paths = read_params.all_access_paths;
    _reader_context.predicate_access_paths = read_params.predicate_access_paths;

    // Force a full read of delete-condition columns: the FE can't see storage deletes and may
    // mark them meta-only (OFFSET/NULL), whose content-less read makes the delete predicate
    // match nothing and leak deleted rows.
    if (!_delete_handler.empty() && !_reader_context.all_access_paths.empty()) {
        remove_delete_columns_from_access_paths(_delete_handler, *_read_schema,
                                                _reader_context.all_access_paths);
    }

    // Propagate general read limit for DUP_KEYS and UNIQUE_KEYS with MOW
    _reader_context.general_read_limit = read_params.general_read_limit;

    return Status::OK();
}

TabletColumn TabletReader::materialize_column(const TabletColumn& orig) {
    if (!orig.is_variant_type()) {
        return orig;
    }
    TabletColumn column_with_cast_type = orig;
    auto cast_type = _reader_context.target_cast_type_for_variants.at(orig.name());
    return variant_util::get_column_by_type(cast_type, orig.name(),
                                            {
                                                    .unique_id = orig.unique_id(),
                                                    .parent_unique_id = orig.parent_unique_id(),
                                                    .path_info = *orig.path_info_ptr(),
                                            });
}

Status TabletReader::_init_params(const ReaderParams& read_params) {
    read_params.check_validation();

    _direct_mode = read_params.direct_mode;
    _aggregation = read_params.aggregation;
    _reader_type = read_params.reader_type;
    _tablet = read_params.tablet;
    _tablet_schema = read_params.tablet_schema;
    _read_schema = read_params.read_schema;
    _reader_context.runtime_state = read_params.runtime_state;
    _reader_context.target_cast_type_for_variants = read_params.target_cast_type_for_variants;

    Status res = _init_delete_condition(read_params);
    if (!res.ok()) {
        LOG(WARNING) << "fail to init delete param. res = " << res;
        return res;
    }

    RETURN_IF_ERROR(_init_column_predicates(read_params));

    res = _init_keys_param(read_params);
    if (!res.ok()) {
        LOG(WARNING) << "fail to init keys param. res=" << res;
        return res;
    }
    res = _init_orderby_keys_param(read_params);
    if (!res.ok()) {
        LOG(WARNING) << "fail to init orderby keys param. res=" << res;
        return res;
    }
    return res;
}

Status TabletReader::_init_keys_param(const ReaderParams& read_params) {
    SCOPED_RAW_TIMER(&_stats.tablet_reader_init_keys_param_timer_ns);
    if (read_params.start_key.empty()) {
        return Status::OK();
    }

    _keys_param.start_key_include = read_params.start_key_include;
    _keys_param.end_key_include = read_params.end_key_include;

    size_t start_key_size = read_params.start_key.size();
    //_keys_param.start_keys.resize(start_key_size);
    std::vector<RowCursor>(start_key_size).swap(_keys_param.start_keys);

    size_t scan_key_size = read_params.start_key.front().size();
    if (scan_key_size > _tablet_schema->num_columns()) {
        return Status::Error<INVALID_ARGUMENT>(
                "Input param are invalid. Column count is bigger than num_columns of schema. "
                "column_count={}, schema.num_columns={}",
                scan_key_size, _tablet_schema->num_columns());
    }

    for (size_t i = 0; i < start_key_size; ++i) {
        if (read_params.start_key[i].size() != scan_key_size) {
            return Status::Error<INVALID_ARGUMENT>(
                    "The start_key.at({}).size={}, not equals the scan_key_size={}", i,
                    read_params.start_key[i].size(), scan_key_size);
        }

        Status res = _keys_param.start_keys[i].init(_tablet_schema, read_params.start_key[i]);
        if (!res.ok()) {
            LOG(WARNING) << "fail to init row cursor. res = " << res;
            return res;
        }
    }

    size_t end_key_size = read_params.end_key.size();
    //_keys_param.end_keys.resize(end_key_size);
    std::vector<RowCursor>(end_key_size).swap(_keys_param.end_keys);
    for (size_t i = 0; i < end_key_size; ++i) {
        if (read_params.end_key[i].size() != scan_key_size) {
            return Status::Error<INVALID_ARGUMENT>(
                    "The end_key.at({}).size={}, not equals the scan_key_size={}", i,
                    read_params.end_key[i].size(), scan_key_size);
        }

        Status res = _keys_param.end_keys[i].init(_tablet_schema, read_params.end_key[i]);
        if (!res.ok()) {
            LOG(WARNING) << "fail to init row cursor. res = " << res;
            return res;
        }
    }

    //TODO:check the valid of start_key and end_key.(eg. start_key <= end_key)

    return Status::OK();
}

Status TabletReader::_init_orderby_keys_param(const ReaderParams& read_params) {
    SCOPED_RAW_TIMER(&_stats.tablet_reader_init_orderby_keys_param_timer_ns);
    // UNIQUE_KEYS will compare all keys as before
    if (_tablet_schema->keys_type() == DUP_KEYS || (_tablet_schema->keys_type() == UNIQUE_KEYS &&
                                                    _tablet->enable_unique_key_merge_on_write())) {
        if (!_tablet_schema->cluster_key_uids().empty()) {
            if (read_params.read_orderby_key_num_prefix_columns >
                _tablet_schema->cluster_key_uids().size()) {
                return Status::Error<ErrorCode::INTERNAL_ERROR>(
                        "read_orderby_key_num_prefix_columns={} > cluster_keys.size()={}",
                        read_params.read_orderby_key_num_prefix_columns,
                        _tablet_schema->cluster_key_uids().size());
            }
            for (uint32_t i = 0; i < read_params.read_orderby_key_num_prefix_columns; i++) {
                auto uid = _tablet_schema->cluster_key_uids()[i];
                auto index = _tablet_schema->field_index(uid);
                if (index < 0) {
                    return Status::Error<ErrorCode::INTERNAL_ERROR>(
                            "could not find cluster key column with unique_id=" +
                            std::to_string(uid) +
                            " in tablet schema, tablet_id=" + std::to_string(_tablet->tablet_id()));
                }
                int32_t ordinal = _read_schema->ordinal_by_column(_tablet_schema->column(index));
                if (ordinal < 0) {
                    break; // size check below reports the error
                }
                _orderby_key_columns.push_back(ordinal);
            }
        } else {
            // the orderby keys are the leading storage key columns; resolve
            // each to its ordinal in the read schema
            for (uint32_t i = 0; i < read_params.read_orderby_key_num_prefix_columns; i++) {
                int32_t ordinal = _read_schema->ordinal_by_column(_tablet_schema->column(i));
                if (ordinal < 0) {
                    break; // size check below reports the error
                }
                _orderby_key_columns.push_back(ordinal);
            }
        }
        if (read_params.read_orderby_key_num_prefix_columns != _orderby_key_columns.size()) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "read_orderby_key_num_prefix_columns != _orderby_key_columns.size, "
                    "read_params.read_orderby_key_num_prefix_columns={}, "
                    "_orderby_key_columns.size()={}",
                    read_params.read_orderby_key_num_prefix_columns, _orderby_key_columns.size());
        }
    }

    return Status::OK();
}

Status TabletReader::_init_column_predicates(const ReaderParams& read_params) {
    SCOPED_RAW_TIMER(&_stats.tablet_reader_init_conditions_param_timer_ns);
    auto predicates = read_params.predicates;
    // Function filter push down to storage engine
    auto is_like_predicate = [](std::shared_ptr<ColumnPredicate> _pred) {
        return dynamic_cast<LikeColumnPredicate*>(_pred.get()) != nullptr;
    };

    for (const auto& filter : read_params.function_filters) {
        predicates.emplace_back(_parse_to_predicate(filter));
        auto pred = predicates.back();

        const auto& col = *_read_schema->column(pred->column_id());
        const auto* tablet_index = _tablet_schema->get_ngram_bf_index(col.unique_id());
        if (is_like_predicate(pred) && tablet_index && config::enable_query_like_bloom_filter) {
            std::unique_ptr<segment_v2::BloomFilter> ng_bf;
            std::string pattern = pred->get_search_str();
            auto gram_bf_size = tablet_index->get_gram_bf_size();
            auto gram_size = tablet_index->get_gram_size();

            RETURN_IF_ERROR(segment_v2::BloomFilter::create(segment_v2::NGRAM_BLOOM_FILTER, &ng_bf,
                                                            gram_bf_size));
            NgramTokenExtractor _token_extractor(gram_size);

            if (_token_extractor.string_like_to_bloom_filter(pattern.data(), pattern.length(),
                                                             *ng_bf)) {
                pred->set_page_ng_bf(std::move(ng_bf));
            }
        }
    }

    int32_t delete_sign_ordinal = _read_schema->delete_sign_ordinal();
    for (auto predicate : predicates) {
        const auto& column = *_read_schema->column(predicate->column_id());
        if (column.aggregation() != FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE) {
            // When MOR value predicate pushdown is enabled, drop __DORIS_DELETE_SIGN__
            // from storage-layer predicates entirely. Delete sign must only be evaluated
            // post-merge via VExpr to prevent deleted rows from reappearing.
            if (read_params.enable_mor_value_predicate_pushdown && delete_sign_ordinal >= 0 &&
                predicate->column_id() == static_cast<uint32_t>(delete_sign_ordinal)) {
                continue;
            }
            _value_col_predicates.push_back(predicate);
        } else {
            _col_predicates.push_back(predicate);
        }
    }

    return Status::OK();
}

std::shared_ptr<ColumnPredicate> TabletReader::_parse_to_predicate(
        const FunctionFilter& function_filter) {
    const auto ordinal = function_filter._column_id;
    DORIS_CHECK_LT(ordinal, _read_schema->num_block_columns());
    const TabletColumn& column = materialize_column(*_read_schema->column(ordinal));
    return create_column_predicate(ordinal, std::make_shared<FunctionFilter>(function_filter),
                                   column.type(), &column);
}

Status TabletReader::_init_delete_condition(const ReaderParams& read_params) {
    SCOPED_RAW_TIMER(&_stats.tablet_reader_init_delete_condition_param_timer_ns);
    // If it's cumu and not allow do delete when cumu
    if (read_params.reader_type == ReaderType::READER_SEGMENT_COMPACTION ||
        (read_params.reader_type == ReaderType::READER_CUMULATIVE_COMPACTION &&
         !config::enable_delete_when_cumu_compaction)) {
        return Status::OK();
    }
    bool cumu_delete = read_params.reader_type == ReaderType::READER_CUMULATIVE_COMPACTION &&
                       config::enable_delete_when_cumu_compaction;
    // Delete sign could not be applied when delete on cumu compaction is enabled, bucause it is meant for delete with predicates.
    // If delete design is applied on cumu compaction, it will lose effect when doing base compaction.
    // `_delete_sign_available` indicates the condition where we could apply delete signs to data.
    _delete_sign_available = (((read_params.reader_type == ReaderType::READER_BASE_COMPACTION ||
                                read_params.reader_type == ReaderType::READER_FULL_COMPACTION) &&
                               config::enable_prune_delete_sign_when_base_compaction) ||
                              read_params.reader_type == ReaderType::READER_COLD_DATA_COMPACTION ||
                              read_params.reader_type == ReaderType::READER_CHECKSUM);

    // `_filter_delete` indicates the condition where we should execlude deleted tuples when reading data.
    // However, queries will not use this condition but generate special where predicates to filter data.
    // (Though a lille bit confused, it is how the current logic working...)
    _filter_delete = _delete_sign_available || cumu_delete;
    return _delete_handler.init(_read_schema, read_params.delete_predicates,
                                read_params.version.second);
}

} // namespace doris
