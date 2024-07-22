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

#include "trino_connector_jni_reader.h"

#include <map>
#include <ostream>

#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "util/jni-util.h"
#include "vec/core/types.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;

namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

const std::string TrinoConnectorJniReader::TRINO_CONNECTOR_OPTION_PREFIX = "trino.";

TrinoConnectorJniReader::TrinoConnectorJniReader(
        const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
        RuntimeProfile* profile, const TFileRangeDesc& range)
        : JniReader(file_slot_descs, state, profile) {
    std::vector<std::string> column_names;
    std::vector<std::string> column_types;
    for (const auto& desc : _file_slot_descs) {
        std::string field = desc->col_name();
        column_names.emplace_back(field);
        column_types.emplace_back(JniConnector::get_jni_type(desc->type()));
    }
    std::map<String, String> params = {
            {"catalog_name", range.table_format_params.trino_connector_params.catalog_name},
            {"db_name", range.table_format_params.trino_connector_params.db_name},
            {"table_name", range.table_format_params.trino_connector_params.table_name},
            {"trino_connector_split",
             range.table_format_params.trino_connector_params.trino_connector_split},
            {"trino_connector_table_handle",
             range.table_format_params.trino_connector_params.trino_connector_table_handle},
            {"trino_connector_column_handles",
             range.table_format_params.trino_connector_params.trino_connector_column_handles},
            {"trino_connector_column_metadata",
             range.table_format_params.trino_connector_params.trino_connector_column_metadata},
            {"trino_connector_predicate",
             range.table_format_params.trino_connector_params.trino_connector_predicate},
            {"trino_connector_trascation_handle",
             range.table_format_params.trino_connector_params.trino_connector_trascation_handle},
            {"required_fields", join(column_names, ",")},
            {"columns_types", join(column_types, "#")}};

    // Used to create trino connector options
    for (const auto& kv :
         range.table_format_params.trino_connector_params.trino_connector_options) {
        params[TRINO_CONNECTOR_OPTION_PREFIX + kv.first] = kv.second;
    }
    _jni_connector = std::make_unique<JniConnector>(
            "org/apache/doris/trinoconnector/TrinoConnectorJniScanner", params, column_names);
}

Status TrinoConnectorJniReader::init_reader(
        std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    RETURN_IF_ERROR(_jni_connector->init(colname_to_value_range));
    RETURN_IF_ERROR(_set_spi_plugins_dir());
    return _jni_connector->open(_state, _profile);
}

Status TrinoConnectorJniReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    return _jni_connector->get_next_block(block, read_rows, eof);
}

Status TrinoConnectorJniReader::get_columns(
        std::unordered_map<std::string, TypeDescriptor>* name_to_type,
        std::unordered_set<std::string>* missing_cols) {
    for (auto& desc : _file_slot_descs) {
        name_to_type->emplace(desc->col_name(), desc->type());
    }
    return Status::OK();
}

Status TrinoConnectorJniReader::_set_spi_plugins_dir() {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    // get PluginLoader class
    jclass plugin_loader_cls;
    std::string plugin_loader_str = "org/apache/doris/trinoconnector/TrinoConnectorPluginLoader";
    RETURN_IF_ERROR(
            JniUtil::get_jni_scanner_class(env, plugin_loader_str.c_str(), &plugin_loader_cls));
    if (!plugin_loader_cls) {
        if (env->ExceptionOccurred()) {
            env->ExceptionDescribe();
        }
        return Status::InternalError("Fail to get JniScanner class.");
    }
    RETURN_ERROR_IF_EXC(env);

    // get method: setPluginsDir(String pluginsDir)
    jmethodID set_plugins_dir_method =
            env->GetStaticMethodID(plugin_loader_cls, "setPluginsDir", "(Ljava/lang/String;)V");
    RETURN_ERROR_IF_EXC(env);

    // call: setPluginsDir(String pluginsDir)
    jstring trino_connector_plugin_path =
            env->NewStringUTF(doris::config::trino_connector_plugin_dir.c_str());
    env->CallStaticVoidMethod(plugin_loader_cls, set_plugins_dir_method,
                              trino_connector_plugin_path);
    RETURN_ERROR_IF_EXC(env);

    return Status::OK();
}

} // namespace doris::vectorized
