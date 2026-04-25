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
#include <sstream>

#include "core/block/block.h"
#include "core/types.h"
#include "format/jni/jni_data_bridge.h"
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
                     RuntimeProfile* profile, std::string connector_class,
                     std::map<std::string, std::string> scanner_params,
                     std::vector<std::string> column_names, int64_t self_split_weight)
        : _file_slot_descs(file_slot_descs),
          _state(state),
          _profile(profile),
          _connector_class(std::move(connector_class)),
          _scanner_params(std::move(scanner_params)),
          _column_names(std::move(column_names)),
          _self_split_weight(static_cast<int32_t>(self_split_weight)) {
    _connector_name = split(_connector_class, "/").back();
}

JniReader::JniReader(std::string connector_class, std::map<std::string, std::string> scanner_params)
        : _file_slot_descs(_s_empty_slot_descs),
          _connector_class(std::move(connector_class)),
          _scanner_params(std::move(scanner_params)) {
    _is_table_schema = true;
    _connector_name = split(_connector_class, "/").back();
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
    // Call org.apache.doris.common.jni.JniScanner#open
    RETURN_IF_ERROR(_jni_scanner_obj.call_void_method(env, _jni_scanner_open).call());

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
        RETURN_IF_ERROR(_jni_scanner_obj.call_long_method(env, _jni_scanner_get_next_batch)
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
    RETURN_IF_ERROR(_jni_scanner_obj.call_void_method(env, _jni_scanner_release_table).call());
    _has_read += num_rows;
    return Status::OK();
}

// =========================================================================
// JniReader::get_table_schema  (merged from JniConnector::get_table_schema)
// =========================================================================

Status JniReader::get_table_schema(std::string& table_schema_str) {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));

    Jni::LocalString jstr;
    RETURN_IF_ERROR(
            _jni_scanner_obj.call_object_method(env, _jni_scanner_get_table_schema).call(&jstr));
    Jni::LocalStringBufferGuard cstr;
    RETURN_IF_ERROR(jstr.get_string_chars(env, &cstr));
    table_schema_str = std::string {cstr.get()};
    return Status::OK();
}

// =========================================================================
// JniReader::close  (merged from JniConnector::close)
// =========================================================================

Status JniReader::close() {
    if (!_closed) {
        _closed = true;
        JNIEnv* env = nullptr;
        RETURN_IF_ERROR(Jni::Env::Get(&env));
        if (_scanner_opened) {
            if (_profile) {
                COUNTER_UPDATE(_open_scanner_time, _jni_scanner_open_watcher);
                COUNTER_UPDATE(_fill_block_time, _fill_block_watcher);
            }

            RETURN_ERROR_IF_EXC(env);
            jlong _append = 0;
            RETURN_IF_ERROR(
                    _jni_scanner_obj.call_long_method(env, _jni_scanner_get_append_data_time)
                            .call(&_append));

            if (_profile) {
                COUNTER_UPDATE(_java_append_data_time, _append);
            }

            jlong _create = 0;
            RETURN_IF_ERROR(
                    _jni_scanner_obj
                            .call_long_method(env, _jni_scanner_get_create_vector_table_time)
                            .call(&_create));

            if (_profile) {
                COUNTER_UPDATE(_java_create_vector_table_time, _create);
                COUNTER_UPDATE(_java_scan_time, _java_scan_watcher - _append - _create);
                _max_time_split_weight_counter->conditional_update(
                        _jni_scanner_open_watcher + _fill_block_watcher + _java_scan_watcher,
                        _self_split_weight);
            }

            // _fill_block may be failed and returned, we should release table in close.
            // org.apache.doris.common.jni.JniScanner#releaseTable is idempotent
            RETURN_IF_ERROR(
                    _jni_scanner_obj.call_void_method(env, _jni_scanner_release_table).call());
            RETURN_IF_ERROR(_jni_scanner_obj.call_void_method(env, _jni_scanner_close).call());
        }
    }
    return Status::OK();
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
        st = _jni_scanner_obj.call_void_method(env, _jni_scanner_set_batch_size)
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
    RETURN_IF_ERROR(
            Jni::Util::get_jni_scanner_class(env, _connector_class.c_str(), &_jni_scanner_cls));

    Jni::MethodId scanner_constructor;
    RETURN_IF_ERROR(_jni_scanner_cls.get_method(env, "<init>", "(ILjava/util/Map;)V",
                                                &scanner_constructor));

    // prepare constructor parameters
    Jni::LocalObject hashmap_object;
    RETURN_IF_ERROR(Jni::Util::convert_to_java_map(env, _scanner_params, &hashmap_object));
    RETURN_IF_ERROR(_jni_scanner_cls.new_object(env, scanner_constructor)
                            .with_arg(batch_size)
                            .with_arg(hashmap_object)
                            .call(&_jni_scanner_obj));

    RETURN_IF_ERROR(_jni_scanner_cls.get_method(env, "open", "()V", &_jni_scanner_open));
    RETURN_IF_ERROR(_jni_scanner_cls.get_method(env, "getNextBatchMeta", "()J",
                                                &_jni_scanner_get_next_batch));
    RETURN_IF_ERROR(_jni_scanner_cls.get_method(env, "getAppendDataTime", "()J",
                                                &_jni_scanner_get_append_data_time));
    RETURN_IF_ERROR(_jni_scanner_cls.get_method(env, "getCreateVectorTableTime", "()J",
                                                &_jni_scanner_get_create_vector_table_time));
    RETURN_IF_ERROR(_jni_scanner_cls.get_method(env, "getTableSchema", "()Ljava/lang/String;",
                                                &_jni_scanner_get_table_schema));
    RETURN_IF_ERROR(_jni_scanner_cls.get_method(env, "close", "()V", &_jni_scanner_close));
    RETURN_IF_ERROR(_jni_scanner_cls.get_method(env, "releaseColumn", "(I)V",
                                                &_jni_scanner_release_column));
    RETURN_IF_ERROR(
            _jni_scanner_cls.get_method(env, "releaseTable", "()V", &_jni_scanner_release_table));
    RETURN_IF_ERROR(_jni_scanner_cls.get_method(env, "getStatistics", "()Ljava/util/Map;",
                                                &_jni_scanner_get_statistics));
    RETURN_IF_ERROR(
            _jni_scanner_cls.get_method(env, "setBatchSize", "(I)V", &_jni_scanner_set_batch_size));
    return Status::OK();
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
        RETURN_IF_ERROR(_jni_scanner_obj.call_void_method(env, _jni_scanner_release_column)
                                .with_arg(i)
                                .call());
        RETURN_ERROR_IF_EXC(env);
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
            _jni_scanner_obj.call_object_method(env, _jni_scanner_get_statistics).call(&metrics));

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
        // update scanner metrics
        std::map<std::string, std::string> statistics_result;
        st = _get_statistics(env, &statistics_result);
        if (!st) {
            LOG(WARNING) << "failed to get_statistics when collect profile: " << st;
            return;
        }

        for (const auto& metric : statistics_result) {
            std::vector<std::string> type_and_name = split(metric.first, ":");
            if (type_and_name.size() != 2) {
                LOG(WARNING) << "Name of JNI Scanner metric should be pattern like "
                             << "'metricType:metricName'";
                continue;
            }
            long metric_value = std::stol(metric.second);
            RuntimeProfile::Counter* scanner_counter;
            if (type_and_name[0] == "timer") {
                scanner_counter =
                        ADD_CHILD_TIMER(_profile, type_and_name[1], _connector_name.c_str());
            } else if (type_and_name[0] == "counter") {
                scanner_counter = ADD_CHILD_COUNTER(_profile, type_and_name[1], TUnit::UNIT,
                                                    _connector_name.c_str());
            } else if (type_and_name[0] == "bytes") {
                scanner_counter = ADD_CHILD_COUNTER(_profile, type_and_name[1], TUnit::BYTES,
                                                    _connector_name.c_str());
            } else {
                LOG(WARNING) << "Type of JNI Scanner metric should be timer, counter or bytes";
                continue;
            }
            COUNTER_UPDATE(scanner_counter, metric_value);
        }
    }
}

// =========================================================================
// MockJniReader
// =========================================================================

MockJniReader::MockJniReader(const std::vector<SlotDescriptor*>& file_slot_descs,
                             RuntimeState* state, RuntimeProfile* profile)
        : JniReader(
                  file_slot_descs, state, profile, "org/apache/doris/common/jni/MockJniScanner",
                  [&]() {
                      std::ostringstream required_fields;
                      std::ostringstream columns_types;
                      int index = 0;
                      for (const auto& desc : file_slot_descs) {
                          std::string field = desc->col_name();
                          std::string type =
                                  JniDataBridge::get_jni_type_with_different_string(desc->type());
                          if (index == 0) {
                              required_fields << field;
                              columns_types << type;
                          } else {
                              required_fields << "," << field;
                              columns_types << "#" << type;
                          }
                          index++;
                      }
                      return std::map<String, String> {{"mock_rows", "10240"},
                                                       {"required_fields", required_fields.str()},
                                                       {"columns_types", columns_types.str()}};
                  }(),
                  [&]() {
                      std::vector<std::string> names;
                      for (const auto& desc : file_slot_descs) {
                          names.emplace_back(desc->col_name());
                      }
                      return names;
                  }()) {}

Status MockJniReader::init_reader() {
    return open(_state, _profile);
}

} // namespace doris
