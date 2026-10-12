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

#include "format_v2/jni/trino_connector_jni_reader.h"

#include <string_view>

#include "common/config.h"

namespace doris::format::trino_connector {
namespace {

constexpr std::string_view TRINO_CONNECTOR_OPTION_PREFIX = "trino.";
constexpr std::string_view TRINO_CONNECTOR_NAME = "connector.name";
constexpr std::string_view TRINO_CONNECTOR_PLUGIN_DIR = "trino_connector_plugin_dir";

} // namespace

Status TrinoConnectorJniReader::validate_scan_range(const TFileRangeDesc& range) const {
    if (!range.__isset.table_format_params) {
        return Status::InternalError("missing table_format_params for trino connector jni reader");
    }
    if (!range.table_format_params.__isset.trino_connector_params) {
        return Status::InternalError(
                "missing trino_connector_params for trino connector jni reader");
    }

    const auto& trino_params = range.table_format_params.trino_connector_params;
    if (!trino_params.__isset.catalog_name || trino_params.catalog_name.empty()) {
        return Status::InternalError(
                "missing catalog_name for trino connector jni reader, possibly caused by FE/BE "
                "protocol mismatch");
    }
    if (!trino_params.__isset.trino_connector_options ||
        !trino_params.trino_connector_options.contains(std::string(TRINO_CONNECTOR_NAME))) {
        return Status::InternalError(
                "missing trino connector.name option for trino connector jni reader, possibly "
                "caused by FE/BE protocol mismatch");
    }
    if (!trino_params.__isset.trino_connector_split || trino_params.trino_connector_split.empty()) {
        return Status::InternalError(
                "missing trino_connector_split for trino connector jni reader, possibly caused "
                "by FE/BE protocol mismatch");
    }
    if (!trino_params.__isset.trino_connector_table_handle ||
        trino_params.trino_connector_table_handle.empty()) {
        return Status::InternalError(
                "missing trino_connector_table_handle for trino connector jni reader, possibly "
                "caused by FE/BE protocol mismatch");
    }
    if (!trino_params.__isset.trino_connector_column_handles ||
        trino_params.trino_connector_column_handles.empty()) {
        return Status::InternalError(
                "missing trino_connector_column_handles for trino connector jni reader, possibly "
                "caused by FE/BE protocol mismatch");
    }
    if (!trino_params.__isset.trino_connector_column_metadata ||
        trino_params.trino_connector_column_metadata.empty()) {
        return Status::InternalError(
                "missing trino_connector_column_metadata for trino connector jni reader, possibly "
                "caused by FE/BE protocol mismatch");
    }
    if (!trino_params.__isset.trino_connector_trascation_handle ||
        trino_params.trino_connector_trascation_handle.empty()) {
        return Status::InternalError(
                "missing trino_connector_trascation_handle for trino connector jni reader, "
                "possibly caused by FE/BE protocol mismatch");
    }
    return Status::OK();
}

Status TrinoConnectorJniReader::prepare_split(const format::SplitReadOptions& options) {
    {
        // Plugin discovery can dominate a cold split. Use non-overlapping common scopes because
        // the JNI base method subsequently enters the same RuntimeProfile counters.
        SCOPED_TIMER(_profile.total_timer);
        SCOPED_TIMER(_profile.prepare_split_timer);
        SCOPED_TIMER(connector_total_timer());
        RETURN_IF_ERROR(validate_scan_range(options.current_range));
    }
    return format::JniTableReader::prepare_split(options);
}

Jni::PluginRef TrinoConnectorJniReader::plugin_ref() const {
    return Jni::plugin::TRINO_CONNECTOR_SCANNER;
}

Status TrinoConnectorJniReader::build_scanner_params(
        std::map<std::string, std::string>* params) const {
    DORIS_CHECK(params != nullptr);
    params->clear();

    const auto& trino_params = _current_range.table_format_params.trino_connector_params;
    (*params)["catalog_name"] = trino_params.catalog_name;
    (*params)["db_name"] = trino_params.db_name;
    (*params)["table_name"] = trino_params.table_name;
    (*params)["trino_connector_split"] = trino_params.trino_connector_split;
    (*params)["trino_connector_table_handle"] = trino_params.trino_connector_table_handle;
    (*params)["trino_connector_column_handles"] = trino_params.trino_connector_column_handles;
    (*params)["trino_connector_column_metadata"] = trino_params.trino_connector_column_metadata;
    (*params)["trino_connector_predicate"] = trino_params.trino_connector_predicate;
    (*params)["trino_connector_trascation_handle"] = trino_params.trino_connector_trascation_handle;

    for (const auto& kv : trino_params.trino_connector_options) {
        (*params)[std::string(TRINO_CONNECTOR_OPTION_PREFIX) + kv.first] = kv.second;
    }
    // Where the plugin should look for the Trino connector plugins it loads inside itself.
    // BE used to push this by calling a static setter on a class it named directly, which
    // stopped being possible once a plugin's classes became private to its own classloader.
    // A scan parameter is the channel that survives isolation.
    (*params)[std::string(TRINO_CONNECTOR_PLUGIN_DIR)] = doris::config::trino_connector_plugin_dir;
    return Status::OK();
}

} // namespace doris::format::trino_connector
