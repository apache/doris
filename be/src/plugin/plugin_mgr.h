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

#ifndef DORIS_BE_PLUGIN_PLUGIN_MGR_H
#define DORIS_BE_PLUGIN_PLUGIN_MGR_H

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "common/status.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "plugin/plugin.h"
#include "plugin/plugin_loader.h"

namespace doris {

typedef std::unordered_map<std::string, std::unique_ptr<PluginLoader>> PluginLoaderMap;

class PluginMgr {
public:
    PluginMgr() {}

    ~PluginMgr() {}

    Status install_plugin(const TPluginMetaInfo& info);

    Status uninstall_plugin(const TPluginMetaInfo& info);

    Status register_builtin_plugin(const std::string& name, int type, const Plugin* plugin);

    Status get_plugin(const std::string& name, int type, std::shared_ptr<Plugin>* plugin);

    Status get_plugin(const std::string& name, std::shared_ptr<Plugin>* plugin);

    Status get_plugin_list(int type, std::vector<std::shared_ptr<Plugin>>* plugin_list);

    Status get_all_plugin_info(std::vector<TPluginInfo>* plugin_info_list);

private:
    PluginLoaderMap _plugins[PLUGIN_TYPE_MAX];

    std::mutex _lock;
};

} // namespace doris

#endif // DORIS_BE_PLUGIN_PLUGIN_LOADER_H
