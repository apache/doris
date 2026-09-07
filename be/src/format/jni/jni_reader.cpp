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

#include "jni_reader.h"

#include <glog/logging.h>

#include <map>
#include <ostream>
#include <tuple>
#include <unordered_map>
#include <utility>

#include "core/block/block.h"
#include "core/types.h"
#include "format/jni/jni_data_bridge.h"
#include "format/table/partition_column_filler.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/jni-util.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;

class Block;
} // namespace doris

namespace doris {

const std::vector<SlotDescriptor*> JniReader::_s_empty_slot_descs;

// =========================================================================
// JniReader constructors
// =========================================================================

JniReader::JniReader(const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
                     RuntimeProfile* profile, Jni::PluginRef plugin_ref,
                     std::map<std::string, std::string> scanner_params,
                     std::vector<std::string> column_names, int64_t self_split_weight)
        : _file_slot_descs(file_slot_descs),
          _state(state),
          _profile(profile),
          _plugin_ref(plugin_ref),
          _connector_name(plugin_ref.plugin),
          _scanner_params(std::move(scanner_params)),
          _column_names(std::move(column_names)),
          _self_split_weight(static_cast<int32_t>(self_split_weight)) {}

JniReader::JniReader(Jni::PluginRef plugin_ref, std::map<std::string, std::string> scanner_params)
        : _file_slot_descs(_s_empty_slot_descs),
          _plugin_ref(plugin_ref),
          _connector_name(plugin_ref.plugin),
          _scanner_params(std::move(scanner_params)) {
    _is_table_schema = true;
}

Status JniReader::on_before_init_reader(ReaderInitContext* ctx) {
    _column_descs = ctx->column_descs;
    if (_col_name_to_block_idx == nullptr) {
        _col_name_to_block_idx = ctx->col_name_to_block_idx;
    }
    _partition_values.clear();
    _partition_value_is_null.clear();
    if (ctx->range == nullptr || ctx->tuple_descriptor == nullptr ||
        !ctx->range->__isset.columns_from_path_keys) {
        return Status::OK();
    }

    DORIS_CHECK(ctx->range->__isset.columns_from_path);
    DORIS_CHECK(ctx->range->columns_from_path.size() == ctx->range->columns_from_path_keys.size());
    const bool has_null_flags = ctx->range->__isset.columns_from_path_is_null;
    if (has_null_flags) {
        DORIS_CHECK(ctx->range->columns_from_path_is_null.size() ==
                    ctx->range->columns_from_path_keys.size());
    }

    std::unordered_map<std::string, const SlotDescriptor*> name_to_slot;
    for (auto* slot : ctx->tuple_descriptor->slots()) {
        name_to_slot.emplace(slot->col_name(), slot);
    }
    for (size_t i = 0; i < ctx->range->columns_from_path_keys.size(); ++i) {
        const auto& key = ctx->range->columns_from_path_keys[i];
        auto slot_it = name_to_slot.find(key);
        if (slot_it == name_to_slot.end()) {
            continue;
        }
        _partition_values.emplace(
                key, std::make_tuple(ctx->range->columns_from_path[i], slot_it->second));
        _partition_value_is_null.emplace(
                key, has_null_flags ? ctx->range->columns_from_path_is_null[i] : false);
    }
    return Status::OK();
}

Status JniReader::on_after_read_block(Block* block, size_t* read_rows) {
    if (_column_descs == nullptr || _partition_values.empty() || *read_rows == 0 ||
        _push_down_agg_type == TPushAggOp::type::COUNT) {
        return Status::OK();
    }
    return _fill_partition_columns(block, *read_rows);
}

// =========================================================================
// JniReader::open  (merged from JniConnector::open)
// =========================================================================

Status JniReader::open(RuntimeState* state, RuntimeProfile* profile) {
    _state = state;
    _profile = profile;
    if (_profile) {
        ADD_TIMER(_profile, _connector_name.c_str());
        _open_scanner_time = ADD_CHILD_TIMER(_profile, "OpenScannerTime", _connector_name.c_str());
        _java_scan_time = ADD_CHILD_TIMER(_profile, "JavaScanTime", _connector_name.c_str());
        _java_append_data_time =
                ADD_CHILD_TIMER(_profile, "JavaAppendDataTime", _connector_name.c_str());
        _java_create_vector_table_time =
                ADD_CHILD_TIMER(_profile, "JavaCreateVectorTableTime", _connector_name.c_str());
        _fill_block_time = ADD_CHILD_TIMER(_profile, "FillBlockTime", _connector_name.c_str());
        _max_time_split_weight_counter = _profile->add_conditition_counter(
                "MaxTimeSplitWeight", TUnit::UNIT, [](int64_t _c, int64_t c) { return c > _c; },
                _connector_name.c_str());
    }
    _java_scan_watcher = 0;

    JNIEnv* env = nullptr;
    int batch_size = 0;
    if (!_is_table_schema && _state) {
        batch_size = _state->batch_size();
    }
    _batch_size = batch_size;
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    SCOPED_RAW_TIMER(&_jni_scanner_open_watcher);
    if (_state) {
        _scanner_params.emplace("time_zone", _state->timezone());
    }
    RETURN_IF_ERROR(_init_jni_scanner(env, batch_size));
    // Call org.apache.doris.jni.spi.JniScanner#open
    RETURN_IF_ERROR(_jni_scanner_obj.call_void_method(env, _scanner_api->open).call());

    RETURN_ERROR_IF_EXC(env);
    _scanner_opened = true;
    return Status::OK();
}

// =========================================================================
// JniReader::_do_get_next_block  (merged from JniConnector::get_next_block)
// =========================================================================

Status JniReader::_do_get_next_block(Block* block, size_t* read_rows, bool* eof) {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    long meta_address = 0;
    {
        SCOPED_RAW_TIMER(&_java_scan_watcher);
        RETURN_IF_ERROR(_jni_scanner_obj.call_long_method(env, _scanner_api->get_next_batch_meta)
                                .call(&meta_address));
    }
    if (meta_address == 0) {
        *read_rows = 0;
        *eof = true;
        return Status::OK();
    }
    _set_meta(meta_address);
    long num_rows = _table_meta.next_meta_as_long();
    if (num_rows == 0) {
        *read_rows = 0;
        *eof = true;
        return Status::OK();
    }
    RETURN_IF_ERROR(_fill_block(block, num_rows));
    *read_rows = num_rows;
    *eof = false;
    RETURN_IF_ERROR(_jni_scanner_obj.call_void_method(env, _scanner_api->release_table).call());
    _has_read += num_rows;
    return Status::OK();
}

// =========================================================================
// JniReader::close  (merged from JniConnector::close)
// =========================================================================

Status JniReader::close() {
    if (_closed) {
        return Status::OK();
    }
    if (!_scanner_opened) {
        _closed = true;
        return Status::OK();
    }

    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));

    // _fill_block may fail before releasing the current Java table. JniScanner::releaseTable()
    // is idempotent, so close always retries it. Java close must still run when that release
    // fails, otherwise connector resources such as Paimon's static table-cache lease can leak.
    auto close_status = _jni_scanner_obj.call_void_method(env, _scanner_api->release_table).call();
    auto java_close_status = _jni_scanner_obj.call_void_method(env, _scanner_api->close).call();
    if (close_status.ok() && !java_close_status.ok()) {
        close_status = std::move(java_close_status);
    }
    if (close_status.ok()) {
        _scanner_opened = false;
        _closed = true;
    }
    return close_status;
}

// =========================================================================
// JniReader::set_batch_size
// =========================================================================

void JniReader::set_batch_size(size_t batch_size) {
    DCHECK_GT(batch_size, 0);
    if (_batch_size == batch_size) {
        return;
    }
    _batch_size = batch_size;
    if (_scanner_opened) {
        JNIEnv* env = nullptr;
        Status st = Jni::Env::Get(&env);
        if (!st) {
            LOG(WARNING) << "failed to get jni env when set_batch_size: " << st;
            return;
        }
        st = _jni_scanner_obj.call_void_method(env, _scanner_api->set_batch_size)
                     .with_arg(static_cast<int>(_batch_size))
                     .call();
        if (!st) {
            LOG(WARNING) << "failed to call setBatchSize: " << st;
        }
    }
}

// =========================================================================
// JniReader::_init_jni_scanner  (merged from JniConnector::_init_jni_scanner)
// =========================================================================

Status JniReader::_init_jni_scanner(JNIEnv* env, int batch_size) {
    return Jni::PluginRegistry::create_scanner(env, _plugin_ref, batch_size, _scanner_params,
                                               &_jni_scanner_obj, &_scanner_api);
}

// =========================================================================
// JniReader::_fill_block  (merged from JniConnector::_fill_block)
// =========================================================================

Status JniReader::_fill_block(Block* block, size_t num_rows) {
    SCOPED_RAW_TIMER(&_fill_block_watcher);
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    // Fallback: if _col_name_to_block_idx was not set by the caller (e.g. JdbcScanner),
    // build the name-to-position map from the block itself.
    std::unordered_map<std::string, uint32_t> local_name_to_idx;
    const std::unordered_map<std::string, uint32_t>* col_map = _col_name_to_block_idx;
    if (col_map == nullptr) {
        local_name_to_idx = block->get_name_to_pos_map();
        col_map = &local_name_to_idx;
    }
    for (int i = 0; i < _column_names.size(); ++i) {
        auto& column_with_type_and_name = block->get_by_position(col_map->at(_column_names[i]));
        auto& column_ptr = column_with_type_and_name.column;
        auto& column_type = column_with_type_and_name.type;
        RETURN_IF_ERROR(JniDataBridge::fill_column(_table_meta, column_ptr, column_type, num_rows));
        // Column is not released when fill_column failed. It will be released when releasing table.
        RETURN_IF_ERROR(_jni_scanner_obj.call_void_method(env, _scanner_api->release_column)
                                .with_arg(i)
                                .call());
        RETURN_ERROR_IF_EXC(env);
    }
    return Status::OK();
}

Status JniReader::_fill_partition_columns(Block* block, size_t num_rows) {
    std::unordered_map<std::string, uint32_t> local_name_to_idx;
    const std::unordered_map<std::string, uint32_t>* col_map = _col_name_to_block_idx;
    if (col_map == nullptr) {
        local_name_to_idx = block->get_name_to_pos_map();
        col_map = &local_name_to_idx;
    }

    for (const auto& desc : *_column_descs) {
        if (desc.category != ColumnCategory::PARTITION_KEY) {
            continue;
        }
        auto value_it = _partition_values.find(desc.name);
        if (value_it == _partition_values.end()) {
            continue;
        }
        auto col_it = col_map->find(desc.name);
        if (col_it == col_map->end()) {
            return Status::InternalError("Missing partition column {} in block {}", desc.name,
                                         block->dump_structure());
        }

        auto& column_with_type_and_name = block->get_by_position(col_it->second);
        auto mutable_column = std::move(*column_with_type_and_name.column).mutate();
        const auto& [value, slot_desc] = value_it->second;
        auto null_it = _partition_value_is_null.find(desc.name);
        DORIS_CHECK(null_it != _partition_value_is_null.end());
        RETURN_IF_ERROR(fill_partition_column_from_path_value(*mutable_column, *slot_desc, value,
                                                              num_rows, null_it->second));
        column_with_type_and_name.column = std::move(mutable_column);
    }
    return Status::OK();
}

// =========================================================================
// JniReader::_get_statistics  (merged from JniConnector::get_statistics)
// =========================================================================

Status JniReader::_get_statistics(JNIEnv* env, std::map<std::string, std::string>* result) {
    result->clear();
    Jni::LocalObject metrics;
    RETURN_IF_ERROR(
            _jni_scanner_obj.call_object_method(env, _scanner_api->get_statistics).call(&metrics));

    RETURN_IF_ERROR(Jni::Util::convert_to_cpp_map(env, metrics, result));
    return Status::OK();
}

// =========================================================================
// JniReader::_collect_profile_before_close
// (merged from JniConnector::_collect_profile_before_close)
// =========================================================================

void JniReader::_collect_profile_before_close() {
    if (_scanner_opened && _profile != nullptr) {
        JNIEnv* env = nullptr;
        Status st = Jni::Env::Get(&env);
        if (!st) {
            LOG(WARNING) << "failed to get jni env when collect profile: " << st;
            return;
        }
        COUNTER_UPDATE(_open_scanner_time, _jni_scanner_open_watcher);
        COUNTER_UPDATE(_fill_block_time, _fill_block_watcher);

        jlong append_data_time = 0;
        auto append_time_status =
                _jni_scanner_obj.call_long_method(env, _scanner_api->get_append_data_time)
                        .call(&append_data_time);
        jlong create_vector_table_time = 0;
        auto create_table_time_status =
                _jni_scanner_obj.call_long_method(env, _scanner_api->get_create_vector_table_time)
                        .call(&create_vector_table_time);
        if (!append_time_status.ok()) {
            LOG(WARNING) << "failed to collect JNI append-data time before close: "
                         << append_time_status;
        }
        if (!create_table_time_status.ok()) {
            LOG(WARNING) << "failed to collect JNI vector-table time before close: "
                         << create_table_time_status;
        }
        if (append_time_status.ok() && create_table_time_status.ok()) {
            COUNTER_UPDATE(_java_append_data_time, append_data_time);
            COUNTER_UPDATE(_java_create_vector_table_time, create_vector_table_time);
            COUNTER_UPDATE(_java_scan_time,
                           _java_scan_watcher - append_data_time - create_vector_table_time);
            _max_time_split_weight_counter->conditional_update(
                    _jni_scanner_open_watcher + _fill_block_watcher + _java_scan_watcher,
                    _self_split_weight);
        }

        // update scanner metrics
        std::map<std::string, std::string> statistics_result;
        st = _get_statistics(env, &statistics_result);
        if (!st) {
            LOG(WARNING) << "failed to get_statistics when collect profile: " << st;
            return;
        }

        const auto update_peak = [](int64_t previous, int64_t current) {
            return current > previous;
        };
        for (const auto& metric : statistics_result) {
            std::vector<std::string> type_and_name = split(metric.first, ":");
            if (type_and_name.size() != 2) {
                LOG(WARNING) << "Name of JNI Scanner metric should be pattern like "
                             << "'metricType:metricName'";
                continue;
            }
            int64_t metric_value = std::stoll(metric.second);
            RuntimeProfile::Counter* scanner_counter;
            if (type_and_name[0] == "timer") {
                scanner_counter =
                        ADD_CHILD_TIMER(_profile, type_and_name[1], _connector_name.c_str());
                COUNTER_UPDATE(scanner_counter, metric_value);
            } else if (type_and_name[0] == "counter") {
                scanner_counter = ADD_CHILD_COUNTER(_profile, type_and_name[1], TUnit::UNIT,
                                                    _connector_name.c_str());
                COUNTER_UPDATE(scanner_counter, metric_value);
            } else if (type_and_name[0] == "bytes") {
                scanner_counter = ADD_CHILD_COUNTER(_profile, type_and_name[1], TUnit::BYTES,
                                                    _connector_name.c_str());
                COUNTER_UPDATE(scanner_counter, metric_value);
            } else if (type_and_name[0] == "timer_gauge") {
                scanner_counter =
                        ADD_CHILD_TIMER(_profile, type_and_name[1], _connector_name.c_str());
                COUNTER_SET(scanner_counter, metric_value);
            } else if (type_and_name[0] == "gauge") {
                scanner_counter = ADD_CHILD_COUNTER(_profile, type_and_name[1], TUnit::UNIT,
                                                    _connector_name.c_str());
                COUNTER_SET(scanner_counter, metric_value);
            } else if (type_and_name[0] == "bytes_gauge") {
                scanner_counter = ADD_CHILD_COUNTER(_profile, type_and_name[1], TUnit::BYTES,
                                                    _connector_name.c_str());
                COUNTER_SET(scanner_counter, metric_value);
            } else if (type_and_name[0] == "timer_peak") {
                auto* scanner_peak_counter = _profile->add_conditition_counter(
                        type_and_name[1], TUnit::TIME_NS, update_peak, _connector_name.c_str());
                scanner_peak_counter->conditional_update(metric_value, metric_value);
            } else if (type_and_name[0] == "peak") {
                auto* scanner_peak_counter = _profile->add_conditition_counter(
                        type_and_name[1], TUnit::UNIT, update_peak, _connector_name.c_str());
                scanner_peak_counter->conditional_update(metric_value, metric_value);
            } else if (type_and_name[0] == "bytes_peak") {
                auto* scanner_peak_counter = _profile->add_conditition_counter(
                        type_and_name[1], TUnit::BYTES, update_peak, _connector_name.c_str());
                scanner_peak_counter->conditional_update(metric_value, metric_value);
            } else {
                LOG(WARNING) << "Type of JNI Scanner metric should be timer, counter, bytes, "
                             << "timer_gauge, gauge, bytes_gauge, timer_peak, peak or bytes_peak";
                continue;
            }
        }
    }
}

} // namespace doris
