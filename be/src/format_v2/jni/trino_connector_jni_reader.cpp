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
#include "util/jni-util.h"

namespace doris::format::trino_connector {
namespace {

constexpr std::string_view TRINO_CONNECTOR_OPTION_PREFIX = "trino.";
constexpr std::string_view TRINO_CONNECTOR_NAME = "connector.name";

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
    RETURN_IF_ERROR(validate_scan_range(options.current_range));
    RETURN_IF_ERROR(_set_spi_plugins_dir());
    return format::JniTableReader::prepare_split(options);
}

std::string TrinoConnectorJniReader::connector_class() const {
    return "org/apache/doris/trinoconnector/TrinoConnectorJniScanner";
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
    return Status::OK();
}

Status TrinoConnectorJniReader::_set_spi_plugins_dir() const {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(Jni::Env::Get(&env));

    Jni::LocalClass plugin_loader_cls;
    const std::string plugin_loader_class =
            "org/apache/doris/trinoconnector/TrinoConnectorPluginLoader";
    RETURN_IF_ERROR(
            Jni::Util::get_jni_scanner_class(env, plugin_loader_class.c_str(), &plugin_loader_cls));

    Jni::MethodId set_plugins_dir_method;
    RETURN_IF_ERROR(plugin_loader_cls.get_static_method(
            env, "setPluginsDir", "(Ljava/lang/String;)V", &set_plugins_dir_method));

    Jni::LocalString trino_connector_plugin_path;
    RETURN_IF_ERROR(Jni::LocalString::new_string(
            env, doris::config::trino_connector_plugin_dir.c_str(), &trino_connector_plugin_path));

    return plugin_loader_cls.call_static_void_method(env, set_plugins_dir_method)
            .with_arg(trino_connector_plugin_path)
            .call();
}

} // namespace doris::format::trino_connector
