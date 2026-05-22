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

#include "hudi_jni_reader.h"

#include <map>

#include "common/config.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/core/types.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;
namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"
const std::string HudiJniReader::HOODIE_CONF_PREFIX = "hoodie.";
const std::string HudiJniReader::HADOOP_CONF_PREFIX = "hadoop_conf.";

namespace {
// FE may put BDP/HDFS auth either in THdfsParams.hdfs_conf (preferred) or in scan range properties (fallback).
void merge_hudi_jni_auth_param(std::map<String, String>* params, const String& key, const String& value) {
    if (key == "HADOOP_USER_TOKEN") {
        (*params)["HADOOP_USER_TOKEN"] = value;
    } else if (key == "BEE_USER" || key == "BEE_SOURCE") {
        (*params)[HudiJniReader::HADOOP_CONF_PREFIX + key] = value;
    }
}
} // namespace

HudiJniReader::HudiJniReader(const TFileScanRangeParams& scan_params,
                             const THudiFileDesc& hudi_params,
                             const std::vector<SlotDescriptor*>& file_slot_descs,
                             RuntimeState* state, RuntimeProfile* profile)
        : JniReader(file_slot_descs, state, profile),
          _scan_params(scan_params),
          _hudi_params(hudi_params) {
    const bool use_new_reader = hudi_params.__isset.serialized_input_split &&
                                !hudi_params.serialized_input_split.empty();
    std::vector<std::string> required_fields;
    for (const auto& desc : _file_slot_descs) {
        required_fields.emplace_back(desc->col_name());
    }
    // Legacy HadoopHudiJniScanner needs delta_file_paths; JDHadoopHudiJniScanner reads from serialized split.
    const std::string delta_paths =
            use_new_reader ? std::string() : join(_hudi_params.delta_logs, ",");
    const std::string data_file_len =
            use_new_reader ? "-1" : std::to_string(_hudi_params.data_file_length);
    std::map<String, String> params = {
            {"query_id", print_id(_state->query_id())},
            {"base_path", _hudi_params.base_path},
            {"data_file_path", _hudi_params.data_file_path},
            {"data_file_length", data_file_len},
            {"delta_file_paths", delta_paths},
            {"hudi_column_names", join(_hudi_params.column_names, ",")},
            {"hudi_column_types", join(_hudi_params.column_types, "#")},
            {"hudi_primary_keys", join(_hudi_params.primary_keys, ",")},
            {"required_fields", join(required_fields, ",")},
            {"instant_time", _hudi_params.instant_time},
            {"serde", _hudi_params.serde},
            {"input_format", _hudi_params.input_format},
            {"time_zone", state->timezone_obj().name()}};
    params["HADOOP_USER_NAME"] = scan_params.hdfs_params.user;
    // Use session variable if set, otherwise fallback to BE config
    int64_t timeout_ms = config::hudi_init_reader_timeout_ms;
    if (_state->query_options().__isset.hudi_init_reader_timeout_ms) {
        if (_state->query_options().hudi_init_reader_timeout_ms >= 0) {
            // Use explicitly set value
            timeout_ms = _state->query_options().hudi_init_reader_timeout_ms;
        } else {
            // -1 means use default: half of query_timeout in milliseconds
            timeout_ms = _state->query_options().query_timeout * 500;
        }
    }
    params["hudi_init_reader_timeout_ms"] = std::to_string(timeout_ms);
    params["hoodie_memory_spillable_map_path"] = config::hoodie_memory_spillable_map_path;
    // Forward all hdfs_conf entries (catalog + BDP), not only auth keys. Previously only TOKEN/BEE_* were
    // merged, dropping fs defaults and other Hadoop settings from THdfsParams.
    for (const THdfsConf& conf : scan_params.hdfs_params.hdfs_conf) {
        merge_hudi_jni_auth_param(&params, conf.key, conf.value);
        if (conf.key == "HADOOP_USER_TOKEN" || conf.key == "BEE_USER" || conf.key == "BEE_SOURCE") {
            continue;
        }
        if (conf.key.starts_with(HOODIE_CONF_PREFIX)) {
            params[conf.key] = conf.value;
        } else {
            params[HADOOP_CONF_PREFIX + conf.key] = conf.value;
        }
    }
    // Use compatible hadoop client to read data
    for (const auto& kv : _scan_params.properties) {
        // Properties fallback: same keys as hdfs_conf; also avoids wrongly prefixing TOKEN with hadoop_conf.
        merge_hudi_jni_auth_param(&params, kv.first, kv.second);
        if (kv.first == "HADOOP_USER_TOKEN" || kv.first == "BEE_USER" || kv.first == "BEE_SOURCE") {
            continue;
        }
        if (kv.first.starts_with(HOODIE_CONF_PREFIX)) {
            params[kv.first] = kv.second;
        } else {
            params[HADOOP_CONF_PREFIX + kv.first] = kv.second;
        }
    }

    if (use_new_reader) {
        params["serialized_input_split"] = _hudi_params.serialized_input_split;
        if (_hudi_params.__isset.database_name && !_hudi_params.database_name.empty()) {
            params["database_name"] = _hudi_params.database_name;
        }
        if (_hudi_params.__isset.table_name && !_hudi_params.table_name.empty()) {
            params["table_name"] = _hudi_params.table_name;
        }
        if (_hudi_params.__isset.start_instant && !_hudi_params.start_instant.empty()) {
            params["start_instant"] = _hudi_params.start_instant;
        }
        if (_hudi_params.__isset.end_instant && !_hudi_params.end_instant.empty()) {
            params["end_instant"] = _hudi_params.end_instant;
        }
        if (_hudi_params.__isset.serialized_hoodie_table && !_hudi_params.serialized_hoodie_table.empty()) {
            params["serialized_hoodie_table"] = _hudi_params.serialized_hoodie_table;
        }
    }

    auto it = params.find("HADOOP_USER_TOKEN");
    if (it == params.end() || it->second.empty()) {
        LOG(WARNING) << "HudiJniReader missing HADOOP_USER_TOKEN, query_id="
                     << print_id(_state->query_id()) << ", base_path=" << _hudi_params.base_path;
    }
    // THdfsParams.user may be empty while location properties carry hadoop.username (see AuthenticationConfig).
    auto userIt = params.find("HADOOP_USER_NAME");
    if (userIt == params.end() || userIt->second.empty()) {
        static const std::string kHadoopUsernameConf = HADOOP_CONF_PREFIX + std::string("hadoop.username");
        auto fromConf = params.find(kHadoopUsernameConf);
        if (fromConf != params.end() && !fromConf->second.empty()) {
            params["HADOOP_USER_NAME"] = fromConf->second;
        }
    }

    std::string scanner_class = "org/apache/doris/hudi/HadoopHudiJniScanner";
    if (use_new_reader) {
        scanner_class = "org/apache/doris/hudi/JDHadoopHudiJniScanner";
    }
    if (_hudi_params.__isset.hudi_jni_scanner && !_hudi_params.hudi_jni_scanner.empty()) {
        scanner_class = _hudi_params.hudi_jni_scanner;
    }
    _jni_connector = std::make_unique<JniConnector>(scanner_class, params, required_fields);
}

Status HudiJniReader::init_reader() {
    if (_profile != nullptr && _hudi_init_reader_timer == nullptr) {
        _hudi_init_reader_timer = ADD_TIMER(_profile, "HudiInitReaderTime");
    }
    if (_hudi_init_reader_timer != nullptr) {
        SCOPED_TIMER(_hudi_init_reader_timer);
    }
    RETURN_IF_ERROR(_jni_connector->init());
    return _jni_connector->open(_state, _profile);
}

Status HudiJniReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    RETURN_IF_ERROR(JniReader::get_next_block(block, read_rows, eof));
    if (*eof) {
        RETURN_IF_ERROR(close());
    }
    return Status::OK();
}
#include "common/compile_check_end.h"
} // namespace doris::vectorized
