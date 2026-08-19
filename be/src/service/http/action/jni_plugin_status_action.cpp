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

#include <optional>
#include <string>

#include "common/config.h"
#include "common/status.h"
#include "service/http/http_channel.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"
#include "service/http/http_status.h"
#include "util/jni-util.h"
#include "util/jni_plugin_registry.h"

namespace doris {

namespace {

// The quote, the backslash and every control character - the three things a JSON string may
// not carry raw. Control characters are not hypothetical here: one of the two callers feeds
// this a Status::to_string(), and a JNI_ERROR carries a bare newline plus a C++ stack trace,
// which would make the error response of this endpoint unparseable by whatever is monitoring
// it. Mirrors PluginRuntime.appendJsonString on the Java side.
std::string json_string(const std::string& value) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string escaped;
    escaped.reserve(value.size() + 2);
    escaped += '"';
    for (char c : value) {
        switch (c) {
        case '"':
            escaped += "\\\"";
            break;
        case '\\':
            escaped += "\\\\";
            break;
        case '\n':
            escaped += "\\n";
            break;
        case '\r':
            escaped += "\\r";
            break;
        case '\t':
            escaped += "\\t";
            break;
        default:
            if (static_cast<unsigned char>(c) < 0x20) {
                escaped += "\\u00";
                escaped += kHex[(static_cast<unsigned char>(c) >> 4) & 0xF];
                escaped += kHex[static_cast<unsigned char>(c) & 0xF];
            } else {
                escaped += c;
            }
            break;
        }
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

    // Java is only reached when the registry is already up. Every branch below reports from
    // this process and from disk alone - none of them creates a JVM, which is why the base is
    // read through jni_base_outcome() rather than asked for through ensure_jni_base().
    if (!Jni::PluginRegistry::registry_initialized()) {
        // The state this PR introduced and this endpoint exists to name: the JVM is up but
        // doris-jni-spi.jar could not be resolved, so no Java code can run in this process - not
        // a plugin, not warmup, nothing. Reported as its own answer because the alternative is
        // the "nothing has loaded yet" note below, which is both wrong and actively unhelpful:
        // it tells an operator to set java_plugin_warmup=true, and warmup fails the same way.
        // HDFS through libhdfs keeps working here, hence javaSupport false with no other symptom.
        if (std::optional<Status> base = Jni::Util::jni_base_outcome();
            base.has_value() && !base->ok()) {
            HttpChannel::send_reply(
                    req, HttpStatus::OK,
                    fmt::format(
                            R"({{"javaSupport":false,"registryInitialized":false,"pluginDir":{},)"
                            R"("anyPluginDeployed":{},"plugins":[],"error":{},)"
                            R"("note":"The JVM is up but the Java plugin SPI could not be)"
                            R"( resolved, so no Java code can run in this process. HDFS through)"
                            R"( libhdfs is unaffected. Check that DORIS_HOME/lib/jni/spi holds)"
                            R"( doris-jni-spi.jar and doris-jni-bootstrap.jar; warmup cannot)"
                            R"( help, it fails the same way."}})"
                            "\n",
                            json_string(config::jni_plugin_dir),
                            Jni::PluginRegistry::any_plugin_deployed() ? "true" : "false",
                            json_string(base->to_string())));
            return;
        }
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
