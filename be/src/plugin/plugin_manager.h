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

#ifndef DORIS_BE_PLUGIN_PLUGIN_MANAGER_H
#define DORIS_BE_PLUGIN_PLUGIN_MANAGER_H


#include <string>
#include <unordered_map>
#include <memory>
#include <mutex>

#include "common/status.h"
#include "plugin/plugin_loader.h"
#include "plugin/plugin.h"


namespace doris {

typedef std::unordered_map<std::string, std::unique_ptr<PluginLoader>> PluginLoaderMap;

class PluginManager {

public:
    
    PluginManager() {}
    
    ~PluginManager() {}
    
//    Status load_plugin(Plugin* plugin);
//    
//    Status unload_plugin(Plugin* plugin);

    Status register_builtin_plugin(const std::string& name, int type, Plugin* plugin);
    
    Status get_plugin(const std::string& name, int type, std::shared_ptr<Plugin>* plugin);
    
    Status get_plugin(const std::string& name, std::shared_ptr<Plugin>* plugin);

    Status get_plugin_list(int type, std::vector<std::shared_ptr<Plugin>>* plugin_list);

private:
    PluginLoaderMap _plugins[PLUGIN_TYPE_MAX];
  
    std::mutex _lock;
};

}

#endif // DORIS_BE_PLUGIN_PLUGIN_LOADER_H
