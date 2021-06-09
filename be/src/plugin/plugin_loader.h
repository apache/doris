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

#ifndef DORIS_BE_PLUGIN_PLUGIN_LOADER_H
#define DORIS_BE_PLUGIN_PLUGIN_LOADER_H

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "gen_cpp/Types_types.h"
#include "plugin/plugin.h"

namespace doris {

class PluginLoader {
public:
    PluginLoader(const std::string& name, int type) : _name(name), _type(type), _close(false) {}

    virtual ~PluginLoader(){};

    virtual Status install() = 0;

    virtual Status uninstall() = 0;

    virtual std::shared_ptr<Plugin>& plugin() { return _plugin; };

    const std::string& name() { return _name; }

    int type() { return _type; }

protected:
    virtual Status open_valid();

    virtual Status close_valid();

protected:
    std::string _name;

    int _type;

    std::shared_ptr<Plugin> _plugin;

    bool _close;
};

class DynamicPluginLoader : public PluginLoader {
public:
    DynamicPluginLoader(const std::string& name, int type, const std::string& source,
                        const std::string& so_name, const std::string& install_path)
            : PluginLoader(name, type),
              _source(source),
              _so_name(so_name),
              _install_path(install_path),
              _plugin_handler(nullptr){};

    virtual ~DynamicPluginLoader() {
        // just close plugin, but don't clean install path (maybe other plugin has used)
        WARN_IF_ERROR(close_plugin(), "close plugin failed.");
    };

    virtual Status install();

    virtual Status uninstall();

private:
    Status open_plugin();

    Status close_plugin();

private:
    std::string _source;

    std::string _so_name;

    std::string _install_path;

    void* _plugin_handler;
};

class BuiltinPluginLoader : public PluginLoader {
public:
    BuiltinPluginLoader(const std::string& name, int type, const Plugin* plugin);

    virtual ~BuiltinPluginLoader() { WARN_IF_ERROR(uninstall(), "close plugin failed."); }

    virtual Status install();

    virtual Status uninstall();
};

} // namespace doris
#endif //DORIS_BE_PLUGIN_PLUGIN_LOADER_H
