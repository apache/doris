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

#include "paimon_jni_reader.h"

#include <algorithm>
#include <map>
#include <string_view>
#include <vector>

#include "core/types.h"
#include "format/jni/jni_data_bridge.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/string_util.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;
class Block;
} // namespace doris

namespace doris {

namespace {
constexpr std::string_view PAIMON_JNI_SCANNER_IO_TMP_DIR = "paimon_jni_scanner_io_tmp";
} // namespace

const std::string PaimonJniReader::PAIMON_OPTION_PREFIX = "paimon.";
const std::string PaimonJniReader::HADOOP_OPTION_PREFIX = "hadoop.";
const std::string PaimonJniReader::DORIS_ENABLE_JNI_IO_MANAGER = "doris.enable_jni_io_manager";
const std::string PaimonJniReader::DORIS_JNI_IO_MANAGER_TMP_DIR = "doris.jni_io_manager.tmp_dir";

PaimonJniReader::PaimonJniReader(const std::vector<SlotDescriptor*>& file_slot_descs,
                                 RuntimeState* state, RuntimeProfile* profile,
                                 const TFileRangeDesc& range,
                                 const TFileScanRangeParams* range_params)
        : JniReader(
                  file_slot_descs, state, profile, "org/apache/doris/paimon/PaimonJniScanner",
                  build_scanner_params(file_slot_descs, state, range, range_params),
                  [&]() {
                      std::vector<std::string> names;
                      for (const auto& desc : file_slot_descs) {
                          names.emplace_back(desc->col_name());
                      }
                      return names;
                  }(),
                  range.__isset.self_split_weight ? range.self_split_weight : -1),
          _range_params(range_params),
          _scanner_params_signature(
                  build_scanner_params(file_slot_descs, state, range, range_params)) {
    set_remaining_table_level_row_count(range);
}

std::map<std::string, std::string> PaimonJniReader::build_scanner_params(
        const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
        const TFileRangeDesc& range, const TFileScanRangeParams* range_params) {
    DORIS_CHECK(state != nullptr);
    DORIS_CHECK(range_params != nullptr);
    std::vector<std::string> column_names;
    std::vector<std::string> column_types;
    column_names.reserve(file_slot_descs.size());
    column_types.reserve(file_slot_descs.size());
    for (const auto* desc : file_slot_descs) {
        column_names.emplace_back(desc->col_name());
        column_types.emplace_back(JniDataBridge::get_jni_type_with_different_string(desc->type()));
    }

    const auto& paimon_params = range.table_format_params.paimon_params;
    std::map<std::string, std::string> params;
    if (range_params->__isset.paimon_predicate && !range_params->paimon_predicate.empty()) {
        params["paimon_predicate"] = range_params->paimon_predicate;
    } else if (paimon_params.__isset.paimon_predicate) {
        params["paimon_predicate"] = paimon_params.paimon_predicate;
    }
    params["required_fields"] = join(column_names, ",");
    params["columns_types"] = join(column_types, "#");
    params["time_zone"] = state->timezone();
    if (range_params->__isset.serialized_table) {
        params["serialized_table"] = range_params->serialized_table;
    }
    if (range_params->__isset.paimon_options && !range_params->paimon_options.empty()) {
        for (const auto& kv : range_params->paimon_options) {
            params[PAIMON_OPTION_PREFIX + kv.first] = kv.second;
        }
    } else if (paimon_params.__isset.paimon_options) {
        for (const auto& kv : paimon_params.paimon_options) {
            params[PAIMON_OPTION_PREFIX + kv.first] = kv.second;
        }
    }
    const std::string enable_io_manager_key = PAIMON_OPTION_PREFIX + DORIS_ENABLE_JNI_IO_MANAGER;
    const std::string io_manager_tmp_dir_key = PAIMON_OPTION_PREFIX + DORIS_JNI_IO_MANAGER_TMP_DIR;
    const auto enable_io_manager_it = params.find(enable_io_manager_key);
    if (enable_io_manager_it != params.end() && iequal(enable_io_manager_it->second, "true") &&
        params.find(io_manager_tmp_dir_key) == params.end()) {
        std::vector<std::string> tmp_dirs;
        const auto& store_paths = state->exec_env()->store_paths();
        tmp_dirs.reserve(store_paths.size());
        for (const auto& store_path : store_paths) {
            tmp_dirs.push_back(store_path.path + "/" + std::string(PAIMON_JNI_SCANNER_IO_TMP_DIR));
        }
        DORIS_CHECK(!tmp_dirs.empty());
        params[io_manager_tmp_dir_key] = join(tmp_dirs, ":");
    }
    if (range_params->__isset.properties && !range_params->properties.empty()) {
        for (const auto& kv : range_params->properties) {
            params[HADOOP_OPTION_PREFIX + kv.first] = kv.second;
        }
    } else if (paimon_params.__isset.hadoop_conf) {
        for (const auto& kv : paimon_params.hadoop_conf) {
            params[HADOOP_OPTION_PREFIX + kv.first] = kv.second;
        }
    }
    return params;
}

void PaimonJniReader::set_remaining_table_level_row_count(const TFileRangeDesc& range) {
    _remaining_table_level_row_count = range.table_format_params.__isset.table_level_row_count
                                               ? range.table_format_params.table_level_row_count
                                               : -1;
}

bool PaimonJniReader::can_reuse_for(const TFileRangeDesc& range) const {
    return _scanner_params_signature ==
           build_scanner_params(_file_slot_descs, _state, range, _range_params);
}

Status PaimonJniReader::prepare_split(const TFileRangeDesc& range, ReaderInitContext* ctx) {
    DORIS_CHECK(ctx != nullptr);
    DORIS_CHECK(!_current_split_prepared);
    RETURN_IF_ERROR(on_before_init_reader(ctx));
    set_remaining_table_level_row_count(range);
    if (_push_down_agg_type == TPushAggOp::type::COUNT && _remaining_table_level_row_count >= 0) {
        return Status::OK();
    }

    set_self_split_weight(range.__isset.self_split_weight ? range.self_split_weight : -1);
    RETURN_IF_ERROR(prepare_jni_scanner_for_split(range));
    _current_split_prepared = true;
    return Status::OK();
}

Status PaimonJniReader::finish_split() {
    if (!_current_split_prepared) {
        return Status::OK();
    }
    RETURN_IF_ERROR(publish_current_split_profile());
    RETURN_IF_ERROR(reset_jni_scanner_current_split());
    _current_split_prepared = false;
    return Status::OK();
}

Status PaimonJniReader::prepare_jni_scanner_for_split(const TFileRangeDesc& range) {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));

    Jni::LocalObject hashmap_object;
    const auto& paimon_params = range.table_format_params.paimon_params;
    std::map<std::string, std::string> split_params;
    split_params["paimon_split"] = paimon_params.paimon_split;
    RETURN_IF_ERROR(Jni::Util::convert_to_java_map(env, split_params, &hashmap_object));
    RETURN_IF_ERROR(jni_scanner_obj()
                            .call_void_method(env, jni_scanner_prepare_for_split())
                            .with_arg(hashmap_object)
                            .call());
    RETURN_ERROR_IF_EXC(env);
    return Status::OK();
}

Status PaimonJniReader::reset_jni_scanner_current_split() {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    RETURN_IF_ERROR(
            jni_scanner_obj().call_void_method(env, jni_scanner_reset_current_split()).call());
    RETURN_ERROR_IF_EXC(env);
    return Status::OK();
}

Status PaimonJniReader::_do_get_next_block(Block* block, size_t* read_rows, bool* eof) {
    if (_push_down_agg_type == TPushAggOp::type::COUNT && _remaining_table_level_row_count >= 0) {
        auto rows = std::min(_remaining_table_level_row_count,
                             (int64_t)_state->query_options().batch_size);
        _remaining_table_level_row_count -= rows;
        auto mutable_columns_guard = block->mutate_columns_scoped();
        auto& mutate_columns = mutable_columns_guard.mutable_columns();
        for (auto& col : mutate_columns) {
            col->resize(rows);
        }
        *read_rows = rows;
        if (_remaining_table_level_row_count == 0) {
            *eof = true;
        }

        return Status::OK();
    }
    return JniReader::_do_get_next_block(block, read_rows, eof);
}

Status PaimonJniReader::init_reader() {
    return open(_state, _profile);
}
} // namespace doris
