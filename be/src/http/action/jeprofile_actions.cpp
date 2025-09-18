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

#include "http/action/jeprofile_actions.h"

#include <jemalloc/jemalloc.h>
#include <unistd.h>

#include <string>

#include "agent/utils.h"
#include "http/ev_http_server.h"
#include "http/http_channel.h"
#include "http/http_handler.h"
#include "http/http_handler_with_auth.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "runtime/memory/heap_profiler.h"

namespace doris {

const static std::string HEADER_JSON = "application/json";
const static std::string START_HEAP_PROFILE_NOTICE =
        "`curl http://be_host:be_webport/jeheap/active/true` to start heap profiler, note that "
        "`JEMALLOC_CONF` in `be/conf/be.conf` must contain `prof:true`, will only track and sample "
        "the memory "
        "allocated and freed after the heap profiler started, it cannot analyze the "
        "memory allocated and freed before. Therefore, dumping the heap profile "
        "immediately after start heap profiler may prompt `No nodes to print`, try rerun your "
        "query and dump the heap profile. Sometimes restarting BE and then immediately executing "
        "`curl http://be_host:be_webport/jeheap/active/true` makes it easier to analyze the "
        "problem.\n"
        "If you want to analyze the memory during the BE process startup, need to modify be.conf "
        "and restart the BE process, open `be/conf/be.conf` and add `,prof_active:true` after "
        "`JEMALLOC_CONF`, or modify `prof_active:false` to `prof:true`.\n";

static bool compile_check(HttpRequest* req) {
#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER)
    HttpChannel::send_reply(
            req, HttpStatus::INTERNAL_SERVER_ERROR,
            "Jemalloc heap dump is not available with ASAN(address sanitizer) builds.\n");
    return false;
#elif !defined(USE_JEMALLOC)
    HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR,
                            "jemalloc heap dump is not available without setting USE_JEMALLOC.\n");
    return false;
#else
    return true;
#endif
}

static bool conf_check(HttpRequest* req) {
    if (!HeapProfiler::instance()->check_active_heap_profiler()) {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR,
                                "Jemalloc heap profiler is not enabled, refer to the following "
                                "method to enable it.\n" +
                                        START_HEAP_PROFILE_NOTICE);
        return false;
    }
    return true;
}

void SetJeHeapProfileActiveActions::handle(HttpRequest* req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    if (compile_check(req)) {
        if (req->param("prof_value") == "true") {
            HeapProfiler::instance()->heap_profiler_start();
            HttpChannel::send_reply(req, HttpStatus::OK,
                                    "Jemalloc heap profiler started\n" + START_HEAP_PROFILE_NOTICE);
        } else {
            HeapProfiler::instance()->heap_profiler_stop();
            HttpChannel::send_reply(req, HttpStatus::OK, "heap profiler stoped\n");
        }
    }
}

void SetJeHeapProfileResetActions::handle(HttpRequest* req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    if (compile_check(req)) {
        const auto& lg_sample_str = req->param("reset_value");
        size_t lg_sample = std::stol(lg_sample_str);
        if (lg_sample > 0 && HeapProfiler::instance()->check_enable_heap_profiler() &&
            HeapProfiler::instance()->heap_profiler_reset(lg_sample)) {
            HttpChannel::send_reply(req, HttpStatus::OK,
                                    fmt::format("Jemalloc reset all memory profile statistics and "
                                                "update the sample rate:{}\n",
                                                lg_sample_str));
        } else {
            HttpChannel::send_reply(req, HttpStatus::OK,
                                    "Jemalloc have not reset.\nThe `JEMALLOC_CONF` in "
                                    "`be/conf/be.conf` must contain `prof:true`.\n");
        }
    }
}

void DumpJeHeapProfileToDotActions::handle(HttpRequest* req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    if (compile_check(req) && conf_check(req)) {
        std::string dot = HeapProfiler::instance()->dump_heap_profile_to_dot();
        if (dot.empty()) {
            HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR,
                                    "dump heap profile to dot failed, see be.INFO\n");
        } else {
            std::string msg;
            AgentUtils util;
            dot += "\n-------------------------------------------------------\n";
            util.exec_cmd("type addr2line", &msg);
            dot += "addr2line: " + msg + "\n";
            dot += "Copy the text after `digraph` in the above output to "
                   "http://www.webgraphviz.com to generate a dot graph.\n"
                   "after start heap profiler, if there is no operation, will print `No nodes to "
                   "print`."
                   "If there are many errors: `addr2line: Dwarf Error`,"
                   "or other FAQ, reference doc: "
                   "https://doris.apache.org/community/developer-guide/debug-tool/#4-qa\n";
            HttpChannel::send_reply(req, HttpStatus::OK, dot);
        }
    }
}

void DumpJeHeapProfileActions::handle(HttpRequest* req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    if (compile_check(req) && conf_check(req)) {
        std::string profile_file_name = HeapProfiler::instance()->dump_heap_profile();
        if (profile_file_name.empty()) {
            HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR,
                                    "jemalloc heap dump failed\n");
        } else {
            HttpChannel::send_reply(req, HttpStatus::OK,
                                    fmt::format("jemalloc heap dump success, dump file path: {}\n",
                                                profile_file_name));
        }
    }
}

} // namespace doris
