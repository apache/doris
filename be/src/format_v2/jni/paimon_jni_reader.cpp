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

namespace doris::format::paimon {
namespace {

constexpr std::string_view PAIMON_OPTION_PREFIX = "paimon.";
constexpr std::string_view HADOOP_OPTION_PREFIX = "hadoop.";

} // namespace

Status PaimonJniReader::validate_scan_range(const TFileRangeDesc& range,
                                            const TFileScanRangeParams* scan_params) {
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
    if (scan_params == nullptr || !scan_params->__isset.serialized_table ||
        scan_params->serialized_table.empty()) {
        return Status::InternalError(
                "missing serialized_table for paimon jni reader, possibly caused by FE/BE "
                "protocol mismatch");
    }
    if (!scan_params->__isset.paimon_predicate || scan_params->paimon_predicate.empty()) {
        return Status::InternalError(
                "missing paimon_predicate for paimon jni reader, possibly caused by FE/BE "
                "protocol mismatch");
    }
    return Status::OK();
}

Status PaimonJniReader::prepare_split(const format::SplitReadOptions& options) {
    _current_range = options.current_range;
    RETURN_IF_ERROR(validate_scan_range(_current_range, _scan_params));
    return format::JniTableReader::prepare_split(options);
}

std::string PaimonJniReader::connector_class() const {
    return "org/apache/doris/paimon/PaimonJniScanner";
}

Status PaimonJniReader::build_scanner_params(std::map<std::string, std::string>* params) const {
    DORIS_CHECK(params != nullptr);
    DORIS_CHECK(_scan_params != nullptr);
    params->clear();

    const auto& paimon_params = _current_range.table_format_params.paimon_params;
    (*params)["paimon_split"] = paimon_params.paimon_split;
    (*params)["paimon_predicate"] = _scan_params->paimon_predicate;
    (*params)["serialized_table"] = _scan_params->serialized_table;

    if (_scan_params->__isset.paimon_options && !_scan_params->paimon_options.empty()) {
        for (const auto& kv : _scan_params->paimon_options) {
            (*params)[std::string(PAIMON_OPTION_PREFIX) + kv.first] = kv.second;
        }
    }
    if (_scan_params->__isset.properties && !_scan_params->properties.empty()) {
        for (const auto& kv : _scan_params->properties) {
            (*params)[std::string(HADOOP_OPTION_PREFIX) + kv.first] = kv.second;
        }
    }
    // TODO: Remove legacy split-level paimon_predicate, paimon_options and hadoop_conf from thrift
    // after all readers stop using them. Format V2 Paimon JNI consumes the scan-level fields
    // planned by current FE and intentionally does not fall back to deprecated split-level fields.
    return Status::OK();
}

int64_t PaimonJniReader::self_split_weight() const {
    return _current_range.__isset.self_split_weight ? _current_range.self_split_weight : -1;
}

} // namespace doris::format::paimon
