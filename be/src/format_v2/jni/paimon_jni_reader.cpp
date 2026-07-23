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

#include "format_v2/jni/paimon_jni_reader.h"

#include <string_view>
#include <utility>

#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/string_util.h"

namespace doris::format::paimon {
namespace {

constexpr std::string_view PAIMON_OPTION_PREFIX = "paimon.";
constexpr std::string_view HADOOP_OPTION_PREFIX = "hadoop.";
constexpr std::string_view DORIS_ENABLE_JNI_IO_MANAGER = "doris.enable_jni_io_manager";
constexpr std::string_view DORIS_JNI_IO_MANAGER_TMP_DIR = "doris.jni_io_manager.tmp_dir";
constexpr std::string_view PAIMON_JNI_SCANNER_IO_TMP_DIR = "paimon_jni_scanner_io_tmp";

const std::string* get_paimon_predicate(const TFileScanRangeParams* scan_params,
                                        const TPaimonFileDesc& paimon_params) {
    if (scan_params != nullptr && scan_params->__isset.paimon_predicate &&
        !scan_params->paimon_predicate.empty()) {
        return &scan_params->paimon_predicate;
    }
    if (paimon_params.__isset.paimon_predicate && !paimon_params.paimon_predicate.empty()) {
        return &paimon_params.paimon_predicate;
    }
    return nullptr;
}

} // namespace

Status PaimonJniReader::validate_scan_range(const TFileRangeDesc& range) const {
    if (!range.__isset.table_format_params) {
        return Status::InternalError("missing table_format_params for paimon jni reader");
    }
    if (!range.table_format_params.__isset.paimon_params) {
        return Status::InternalError("missing paimon_params for paimon jni reader");
    }
    if (!range.table_format_params.paimon_params.__isset.paimon_split ||
        range.table_format_params.paimon_params.paimon_split.empty()) {
        return Status::InternalError(
                "missing paimon_split for paimon jni reader, possibly caused by FE/BE protocol "
                "mismatch");
    }
    if (!range.table_format_params.paimon_params.__isset.reader_type ||
        range.table_format_params.paimon_params.reader_type != TPaimonReaderType::PAIMON_JNI) {
        return Status::InternalError(
                "invalid reader_type for paimon jni reader, possibly caused by FE/BE protocol "
                "mismatch");
    }
    if (_scan_params == nullptr || !_scan_params->__isset.serialized_table ||
        _scan_params->serialized_table.empty()) {
        return Status::InternalError(
                "missing serialized_table for paimon jni reader, possibly caused by FE/BE "
                "protocol mismatch");
    }
    if (get_paimon_predicate(_scan_params, range.table_format_params.paimon_params) == nullptr) {
        return Status::InternalError(
                "missing paimon_predicate for paimon jni reader, possibly caused by FE/BE "
                "protocol mismatch");
    }
    return Status::OK();
}

std::string PaimonJniReader::connector_class() const {
    return "org/apache/doris/paimon/PaimonJniScanner";
}

Status PaimonJniReader::build_scanner_params(std::map<std::string, std::string>* params) const {
    DORIS_CHECK(params != nullptr);
    DORIS_CHECK(_scan_params != nullptr);
    params->clear();

    const auto& paimon_params = _current_range.table_format_params.paimon_params;
    const auto* paimon_predicate = get_paimon_predicate(_scan_params, paimon_params);
    DORIS_CHECK(paimon_predicate != nullptr);
    (*params)["paimon_predicate"] = *paimon_predicate;
    (*params)["serialized_table"] = _scan_params->serialized_table;

    if (_scan_params->__isset.paimon_options && !_scan_params->paimon_options.empty()) {
        for (const auto& kv : _scan_params->paimon_options) {
            (*params)[std::string(PAIMON_OPTION_PREFIX) + kv.first] = kv.second;
        }
    } else if (paimon_params.__isset.paimon_options) {
        // Rolling upgrades can pair this BE with an older FE that only sends options per split.
        for (const auto& kv : paimon_params.paimon_options) {
            (*params)[std::string(PAIMON_OPTION_PREFIX) + kv.first] = kv.second;
        }
    }
    const std::string enable_io_manager_key =
            std::string(PAIMON_OPTION_PREFIX) + std::string(DORIS_ENABLE_JNI_IO_MANAGER);
    const std::string io_manager_tmp_dir_key =
            std::string(PAIMON_OPTION_PREFIX) + std::string(DORIS_JNI_IO_MANAGER_TMP_DIR);
    auto enable_io_manager_it = params->find(enable_io_manager_key);
    if (enable_io_manager_it != params->end() && iequal(enable_io_manager_it->second, "true") &&
        params->find(io_manager_tmp_dir_key) == params->end()) {
        DORIS_CHECK(_runtime_state != nullptr);
        // Keep Format V2 consistent with the legacy JNI path. Paimon creates and
        // removes its own paimon-* child directory under these Doris storage-root scoped
        // parent directories.
        (*params)[io_manager_tmp_dir_key] =
                build_default_io_manager_tmp_dirs(_runtime_state->exec_env()->store_paths());
    }
    if (_scan_params->__isset.properties && !_scan_params->properties.empty()) {
        for (const auto& kv : _scan_params->properties) {
            (*params)[std::string(HADOOP_OPTION_PREFIX) + kv.first] = kv.second;
        }
    } else if (paimon_params.__isset.hadoop_conf) {
        for (const auto& kv : paimon_params.hadoop_conf) {
            (*params)[std::string(HADOOP_OPTION_PREFIX) + kv.first] = kv.second;
        }
    }
    // TODO: Remove legacy split-level paimon_predicate, paimon_options and hadoop_conf from thrift
    // after the minimum supported FE always sends their scan-level replacements.
    return Status::OK();
}

Status PaimonJniReader::open_jni_scanner_for_split() {
    if (!jni_scanner_opened()) {
        RETURN_IF_ERROR(JniTableReader::open_jni_scanner_for_split());
    }
    return _prepare_for_split();
}

Status PaimonJniReader::close_jni_scanner_for_split() {
    if (!_current_split_prepared) {
        return Status::OK();
    }
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    _publish_jni_scanner_split_timing(env);
    return _reset_current_split();
}

Status PaimonJniReader::_prepare_for_split() {
    DORIS_CHECK(jni_scanner_opened());
    DORIS_CHECK(!_current_split_prepared);
    std::map<std::string, std::string> split_params;
    split_params["paimon_split"] = _current_range.table_format_params.paimon_params.paimon_split;

    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    Jni::LocalObject hashmap_object;
    RETURN_IF_ERROR(Jni::Util::convert_to_java_map(env, split_params, &hashmap_object));
    RETURN_IF_ERROR(jni_scanner_obj()
                            .call_void_method(env, jni_scanner_prepare_for_split())
                            .with_arg(hashmap_object)
                            .call());
    RETURN_ERROR_IF_EXC(env);
    _current_split_prepared = true;
    reset_jni_eof();
    return Status::OK();
}

Status PaimonJniReader::close() {
    auto reset_status = _reset_current_split();
    auto close_status = JniTableReader::close();
    if (reset_status.ok() && !close_status.ok()) {
        reset_status = std::move(close_status);
    }
    return reset_status;
}

Status PaimonJniReader::_reset_current_split() {
    if (!_current_split_prepared) {
        return Status::OK();
    }
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));
    RETURN_IF_ERROR(
            jni_scanner_obj().call_void_method(env, jni_scanner_reset_current_split()).call());
    RETURN_ERROR_IF_EXC(env);
    _current_split_prepared = false;
    return Status::OK();
}

std::string PaimonJniReader::build_default_io_manager_tmp_dirs(
        const std::vector<StorePath>& store_paths) {
    std::vector<std::string> tmp_dirs;
    tmp_dirs.reserve(store_paths.size());
    for (const auto& store_path : store_paths) {
        tmp_dirs.push_back(store_path.path + "/" + std::string(PAIMON_JNI_SCANNER_IO_TMP_DIR));
    }
    DORIS_CHECK(!tmp_dirs.empty());
    return join(tmp_dirs, ":");
}

} // namespace doris::format::paimon
