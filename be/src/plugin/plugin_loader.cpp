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

#include "plugin/plugin_loader.h"

#include <boost/algorithm/string/predicate.hpp>
#include <cstring>

#include "env/env.h"
#include "gutil/strings/substitute.h"
#include "gutil/strings/util.h"
#include "http/http_client.h"
#include "plugin/plugin_zip.h"
#include "util/dynamic_util.h"
#include "util/file_utils.h"
#include "util/md5.h"
#include "util/time.h"

namespace doris {

using namespace strings;

static const std::string PLUGIN_VERSION_SYMBOL = "_plugin_interface_version";
static const std::string PLUGIN_SIZE_SYMBOL = "_sizeof_plugin";
static const std::string PLUGIN_STRUCT_SYMBOL = "_plugin";

Status PluginLoader::open_valid() {
    return Status::OK();
}

Status PluginLoader::close_valid() {
    if (_plugin.get() != nullptr && (_plugin->flags & PLUGIN_NOT_DYNAMIC_UNINSTALL)) {
        return Status::InternalError(
                strings::Substitute("plugin $0 not allow dynamic uninstall", _name));
    }

    return Status::OK();
}

Status DynamicPluginLoader::install() {
    // check already install
    std::string so_path = _install_path + "/" + _name + "/" + _so_name;
    if (!FileUtils::check_exist(so_path)) {
        // no, need download zip install
        PluginZip zip(_source);

        RETURN_NOT_OK_STATUS_WITH_WARN(zip.extract(_install_path, _name), "plugin install failed");
    }

    // open plugin
    RETURN_NOT_OK_STATUS_WITH_WARN(open_plugin(), "plugin install failed");

    RETURN_NOT_OK_STATUS_WITH_WARN(open_valid(), "plugin install failed");

    // plugin init
    // todo: what should be send?
    if (_plugin->init != nullptr) {
        _plugin->init(&_plugin->handler);
    }

    return Status::OK();
}

/**
 * open & valid Plugin:
 * 1. check .so file exists
 * 2. check .so version symbol
 * 3. check .so plugin symbol
 */
Status DynamicPluginLoader::open_plugin() {
    // check .so file
    std::string so_path = _install_path + "/" + _name + "/" + _so_name;
    if (!FileUtils::check_exist(so_path)) {
        return Status::InternalError("plugin install not found " + _so_name);
    }

    RETURN_IF_ERROR(dynamic_open(so_path.c_str(), &_plugin_handler));

    void* symbol;
    // check version symbol
    RETURN_IF_ERROR(
            dynamic_lookup(_plugin_handler, (_name + PLUGIN_VERSION_SYMBOL).c_str(), &symbol));

    if (DORIS_PLUGIN_VERSION > *(int*)symbol) {
        return Status::InternalError("plugin compile version too old");
    }

    RETURN_IF_ERROR(dynamic_lookup(_plugin_handler, (_name + PLUGIN_SIZE_SYMBOL).c_str(), &symbol));

    int plugin_size = *(int*)symbol;
    if (plugin_size != sizeof(Plugin)) {
        return Status::InternalError("plugin struct error");
    }

    // check Plugin declaration
    RETURN_IF_ERROR(
            dynamic_lookup(_plugin_handler, (_name + PLUGIN_STRUCT_SYMBOL).c_str(), &symbol));

    Plugin* end_plugin = (Plugin*)((char*)symbol + plugin_size);

    if (end_plugin->handler != nullptr || end_plugin->init != nullptr ||
        end_plugin->close != nullptr) {
        return Status::InternalError("plugin struct error");
    }

    _plugin = std::make_shared<Plugin>();
    std::memcpy(_plugin.get(), symbol, plugin_size);

    return Status::OK();
}

Status DynamicPluginLoader::uninstall() {
    // close plugin
    RETURN_IF_ERROR(close_plugin());

    // remove plugin install path
    RETURN_IF_ERROR(FileUtils::remove_all(_install_path + "/" + _name));

    return Status::OK();
}

Status DynamicPluginLoader::close_plugin() {
    if (_close) {
        return Status::OK();
    }

    if (_plugin.get() != nullptr) {
        RETURN_IF_ERROR(close_valid());

        if (_plugin->close != nullptr) {
            // todo: what should be send?
            _plugin->close(&_plugin->handler);
        }
    }

    // builtin plugin don't need dynamic uninstall
    if (_plugin_handler != nullptr) {
        dynamic_close(_plugin_handler);
    }

    _close = true;
    return Status::OK();
}

BuiltinPluginLoader::BuiltinPluginLoader(const std::string& name, int type,
                                         const doris::Plugin* plugin)
        : PluginLoader(name, type) {
    _plugin = std::make_shared<Plugin>();
    std::memcpy(_plugin.get(), plugin, sizeof(Plugin));
}

Status BuiltinPluginLoader::install() {
    RETURN_IF_ERROR(open_valid());
    LOG(INFO) << "plugin: " << _plugin.get();

    if (_plugin->init != nullptr) {
        _plugin->init(&_plugin->handler);
    }

    return Status::OK();
}

Status BuiltinPluginLoader::uninstall() {
    if (_close) {
        return Status::OK();
    }

    if (_plugin.get() != nullptr) {
        RETURN_IF_ERROR(close_valid());

        if (_plugin->close != nullptr) {
            // todo: what should be send?
            _plugin->close(&_plugin->handler);
        }

        _plugin.reset();
    }

    _close = true;
    return Status::OK();
}

} // namespace doris
