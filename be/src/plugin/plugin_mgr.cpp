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

#include "plugin/plugin_mgr.h"

#include <boost/foreach.hpp>

#include "common/config.h"
#include "gutil/strings/substitute.h"

namespace doris {

using namespace strings;

#define PLUGIN_TYPE_CHECK(_type)                                                                 \
    {                                                                                            \
        if (_type >= PLUGIN_TYPE_MAX) {                                                          \
            return Status::InvalidArgument(strings::Substitute("error plugin type: $0", _type)); \
        }                                                                                        \
    }

Status PluginMgr::install_plugin(const TPluginMetaInfo& info) {
    {
        std::lock_guard<std::mutex> l(_lock);
        auto iter = _plugins[info.type].find(info.name);

        if (iter != _plugins[info.type].end()) {
            return Status::AlreadyExist("plugin " + info.name + " is already installed");
        }
    }

    DCHECK(info.__isset.so_name);
    DCHECK(info.__isset.source);

    std::unique_ptr<PluginLoader> loader = std::unique_ptr<PluginLoader>(new DynamicPluginLoader(
            info.name, info.type, info.source, info.so_name, config::plugin_path));

    Status st = loader->install();

    if (!st.ok() && !st.is_already_exist()) {
        RETURN_IF_ERROR(loader->uninstall());
        return st;
    }

    {
        std::lock_guard<std::mutex> l(_lock);
        auto iter = _plugins[info.type].find(info.name);

        if (iter != _plugins[info.type].end()) {
            return Status::AlreadyExist("plugin " + info.name + " is already installed");
        } else {
            _plugins[info.type][info.name] = std::move(loader);
        };
    }

    return Status::OK();
}

Status PluginMgr::uninstall_plugin(const TPluginMetaInfo& info) {
    std::lock_guard<std::mutex> l(_lock);

    auto iter = _plugins[info.type].find(info.name);

    if (iter != _plugins[info.type].end()) {
        _plugins[info.type].erase(iter);
    }

    return Status::OK();
}

Status PluginMgr::get_plugin(const std::string& name, int type, std::shared_ptr<Plugin>* plugin) {
    PLUGIN_TYPE_CHECK(type);

    std::lock_guard<std::mutex> l(_lock);

    auto iter = _plugins[type].find(name);

    if (iter != _plugins[type].end()) {
        *plugin = iter->second->plugin();
        return Status::OK();
    }

    return Status::NotFound(strings::Substitute("not found type $0 plugin $1", type, name));
}

Status PluginMgr::get_plugin(const std::string& name, std::shared_ptr<Plugin>* plugin) {
    for (int i = 0; i < PLUGIN_TYPE_MAX; ++i) {
        std::lock_guard<std::mutex> l(_lock);

        auto iter = _plugins[i].find(name);

        if (iter != _plugins[i].end()) {
            *plugin = iter->second->plugin();
            return Status::OK();
        }
    }

    return Status::NotFound(strings::Substitute("not found plugin $0", name));
}

Status PluginMgr::get_plugin_list(int type, std::vector<std::shared_ptr<Plugin>>* plugin_list) {
    PLUGIN_TYPE_CHECK(type);

    std::lock_guard<std::mutex> l(_lock);

    BOOST_FOREACH (const PluginLoaderMap::value_type& iter, _plugins[type]) {
        plugin_list->push_back(iter.second->plugin());
    }

    return Status::OK();
}

Status PluginMgr::register_builtin_plugin(const std::string& name, int type,
                                          const doris::Plugin* plugin) {
    PLUGIN_TYPE_CHECK(type);

    std::lock_guard<std::mutex> l(_lock);

    auto iter = _plugins[type].find(name);
    if (iter != _plugins[type].end()) {
        return Status::AlreadyExist(
                strings::Substitute("the type $0 plugin $1 already register", type, name));
    }

    std::unique_ptr<PluginLoader> loader =
            std::unique_ptr<PluginLoader>(new BuiltinPluginLoader(name, type, plugin));

    Status st = loader->install();
    if (!st.ok()) {
        RETURN_IF_ERROR(loader->uninstall());
        return st;
    }

    _plugins[type][name] = std::move(loader);

    return Status::OK();
}

Status PluginMgr::get_all_plugin_info(std::vector<TPluginInfo>* plugin_info_list) {
    for (int i = 0; i < PLUGIN_TYPE_MAX; ++i) {
        BOOST_FOREACH (const PluginLoaderMap::value_type& iter, _plugins[i]) {
            TPluginInfo info;
            info.__set_plugin_name(iter.second->name());
            info.__set_type(iter.second->type());

            plugin_info_list->push_back(info);
        }
    }

    return Status::OK();
}

} // namespace doris
