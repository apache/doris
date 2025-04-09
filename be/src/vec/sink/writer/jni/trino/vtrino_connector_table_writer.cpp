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

#include "vec/sink/writer/jni/trino/vtrino_connector_table_writer.h"

#include <gen_cpp/DataSinks_types.h>

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/jni-util.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {

const std::string VTrinoConnectorTableWriter::TRINO_CONNECTOR_OPTION_PREFIX = "trino.";

VTrinoConnectorTableWriter::VTrinoConnectorTableWriter(
        const TDataSink& t_sink, const VExprContextSPtrs& output_exprs,
        std::shared_ptr<pipeline::Dependency> dep, std::shared_ptr<pipeline::Dependency> fin_dep)
        : AsyncResultWriter(output_exprs, dep, fin_dep) {
    const TTrinoConnnectorTableSink& t_trino_sink = t_sink.trino_connector_table_sink;

    params = {{"catalog_name", t_trino_sink.catalog_name},
              {"db_name", t_trino_sink.db_name},
              {"table_name", t_trino_sink.table_name},
              {"trino_connector_table_handle", t_trino_sink.trino_connector_table_handle},
              {"trino_connector_column_handles", t_trino_sink.trino_connector_column_handles},
              {"trino_connector_column_metadata", t_trino_sink.trino_connector_column_metadata},
              {"trino_connector_transaction_handle",
               t_trino_sink.trino_connector_transaction_handle}};

    // Used to create trino connector options
    for (const auto& kv : t_trino_sink.trino_connector_options) {
        params[TRINO_CONNECTOR_OPTION_PREFIX + kv.first] = kv.second;
    }
}

Status VTrinoConnectorTableWriter::_set_spi_plugins_dir() {
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
        return Status::InternalError("Fail to get TrinoConnectorPluginLoader class.");
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

Status VTrinoConnectorTableWriter::_init_writer() {
    _jni_connector = std::make_unique<JniConnector>(
            "org/apache/doris/trinoconnector/TrinoConnectorJniWriter", params);

    return _jni_connector->open_writer(_state, _profile);
}

Status VTrinoConnectorTableWriter::write(RuntimeState* state, vectorized::Block& block) {
    if (!_jni_connector) {
        _state = state;
        _profile = state->runtime_profile();
        RETURN_IF_ERROR(_set_spi_plugins_dir());
        RETURN_IF_ERROR(_init_writer());
    }

    Block output_block;
    RETURN_IF_ERROR(_projection_block(block, &output_block));

    auto num_rows = output_block.rows();
    if (num_rows == 0) {
        return Status::OK();
    }

    RETURN_IF_ERROR(_jni_connector->write(&output_block));

    LOG(INFO) << "Successfully wrote " << num_rows << " rows to Trino connector";
    return Status::OK();
}

Status VTrinoConnectorTableWriter::finish(RuntimeState* state) {
    if (!_jni_connector) {
        return Status::OK();
    }

    RETURN_IF_ERROR(_jni_connector->finish());

    LOG(INFO) << "Successfully finished writing to Trino connector";
    return Status::OK();
}

} // namespace doris::vectorized
