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

#include "service/http/action/jni_plugin_status_action.h"

#include <fmt/format.h>

#include <string>

#include "common/config.h"
#include "common/status.h"
#include "service/http/http_channel.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"
#include "service/http/http_status.h"
#include "util/jni_plugin_registry.h"

namespace doris {

namespace {

// The two characters a JSON string may not carry raw. A plugin directory is a BE config, so it
// cannot contain a control character without the config having been quoted already.
std::string json_string(const std::string& value) {
    std::string escaped;
    escaped.reserve(value.size() + 2);
    escaped += '"';
    for (char c : value) {
        if (c == '"' || c == '\\') {
            escaped += '\\';
        }
        escaped += c;
    }
    escaped += '"';
    return escaped;
}

} // namespace

void JniPluginStatusAction::handle(HttpRequest* req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, "application/json");

    if (!config::enable_java_support) {
        HttpChannel::send_reply(req, HttpStatus::OK,
                                R"({"javaSupport":false,"plugins":[]})"
                                "\n");
        return;
    }

    // Java is only reached when the registry is already up. Both branches below report from
    // this process and from disk alone.
    if (!Jni::PluginRegistry::registry_initialized()) {
        HttpChannel::send_reply(
                req, HttpStatus::OK,
                fmt::format(
                        R"({{"javaSupport":true,"registryInitialized":false,"pluginDir":{},)"
                        R"("anyPluginDeployed":{},"plugins":[],)"
                        R"("note":"No plugin has been loaded yet, so there is nothing to report.)"
                        R"( Plugins load on first use; set java_plugin_warmup=true to load them)"
                        R"( at startup instead."}})"
                        "\n",
                        json_string(config::jni_plugin_dir),
                        Jni::PluginRegistry::any_plugin_deployed() ? "true" : "false"));
        return;
    }

    std::string status;
    if (Status st = Jni::PluginRegistry::plugin_status_json(&status); !st.ok()) {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR,
                                fmt::format(R"({{"error":{}}})"
                                            "\n",
                                            json_string(st.to_string())));
        return;
    }
    HttpChannel::send_reply(req, HttpStatus::OK, status + "\n");
}

} // end namespace doris
