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

#include "pprof_action.h"
#include <brpc/http_method.h>

#include "util/bfd_parser.h"

namespace doris {

// pprof default sample time in seconds.
static const std::string SECOND_KEY = "seconds";
static const int kPprofDefaultSampleSecs = 30;

PProfHandler::PProfHandler(ExecEnv* exec_env) : BaseHttpHandler("pprof", exec_env) {}

void PProfHandler::handle_sync(brpc::Controller* cntl) {
    const std::string& path = cntl->http_request().unresolved_path();
    const brpc::HttpMethod method = cntl->http_request().method();
    const brpc::HttpMethod GET = brpc::HttpMethod::HTTP_METHOD_GET;
    if (path == "heap" && method == GET) {
        _do_heap_action(cntl);
    } else if (path == "growth" && method == GET) {
        _do_growth_action(cntl);
    } else if (path == "profile" && method == GET) {
        _do_profile_action(cntl);
    } else if (path == "pmuprofile" && method == GET) {
        _do_pmu_profile_action(cntl);
    } else if (path == "contention" && method == GET) {
        _do_content_action(cntl);
    } else if (path == "cmdline" && method == GET) {
        _do_cmd_line_action(cntl);
    } else if (path == "symbol") {
        _do_symbol_action(cntl, method);
    }
}

bool PProfHandler::support_method(brpc::HttpMethod method) const {
    return method == brpc::HTTP_METHOD_GET || method == brpc::HTTP_METHOD_HEAD || method == brpc::HTTP_METHOD_POST;
}

// Protect, only one thread can work
static std::mutex kPprofHandlerMutex;

void PProfHandler::_do_heap_action(brpc::Controller* cntl) {
    std::lock_guard<std::mutex> lock(kPprofHandlerMutex);
#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER) || \
        defined(USE_JEMALLOC)
    (void)kPprofDefaultSampleSecs; // Avoid unused variable warning.

    std::string str = "Heap profiling is not available with address sanitizer or jemalloc builds.";

    on_succ(cntl, str);
#else
    int seconds = kPprofDefaultSampleSecs;
    const std::string& seconds_str = req->param(SECOND_KEY);
    if (!seconds_str.empty()) {
        seconds = std::atoi(seconds_str.c_str());
    }

    std::stringstream tmp_prof_file_name;
    // Build a temporary file name that is hopefully unique.
    tmp_prof_file_name << config::pprof_profile_dir << "/heap_profile." << getpid() << "."
                       << rand();

    HeapProfilerStart(tmp_prof_file_name.str().c_str());
    // Sleep to allow for some samples to be collected.
    sleep(seconds);
    const char* profile = GetHeapProfile();
    HeapProfilerStop();
    std::string str = profile;
    delete profile;

    const std::string& readable_str = req->param("readable");
    if (!readable_str.empty()) {
        std::stringstream readable_res;
        Status st = PprofUtils::get_readable_profile(str, false, &readable_res);
        if (!st.ok()) {
            HttpChannel::send_reply(req, st.to_string());
        } else {
            HttpChannel::send_reply(req, readable_res.str());
        }
    } else {
        HttpChannel::send_reply(req, str);
    }
#endif
}

void PProfHandler::_do_growth_action(brpc::Controller* cntl) {
#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER) || \
        defined(USE_JEMALLOC)
    std::string str =
            "Growth profiling is not available with address sanitizer or jemalloc builds.";
    on_succ(cntl, str);
#else
    std::lock_guard<std::mutex> lock(kPprofHandlerMutex);

    std::string heap_growth_stack;
    MallocExtension::instance()->GetHeapGrowthStacks(&heap_growth_stack);

    HttpChannel::send_reply(req, heap_growth_stack);
#endif
}

void PProfHandler::_do_profile_action(brpc::Controller* cntl) {
#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER) || \
        defined(USE_JEMALLOC)
    std::string str = "CPU profiling is not available with address sanitizer or jemalloc builds.";
    on_succ(cntl, str);
#else
    std::lock_guard<std::mutex> lock(kPprofHandlerMutex);

    int seconds = kPprofDefaultSampleSecs;
    const std::string& seconds_str = req->param(SECOND_KEY);
    if (!seconds_str.empty()) {
        seconds = std::atoi(seconds_str.c_str());
    }

    const std::string& type_str = req->param("type");
    if (type_str != "flamegraph") {
        // use pprof the sample the CPU
        std::ostringstream tmp_prof_file_name;
        tmp_prof_file_name << config::pprof_profile_dir << "/doris_profile." << getpid() << "."
                           << rand();
        ProfilerStart(tmp_prof_file_name.str().c_str());
        sleep(seconds);
        ProfilerStop();

        if (type_str != "text") {
            // return raw content via http response directly
            std::ifstream prof_file(tmp_prof_file_name.str().c_str(), std::ios::in);
            std::stringstream ss;
            if (!prof_file.is_open()) {
                ss << "Unable to open cpu profile: " << tmp_prof_file_name.str();
                std::string str = ss.str();
                HttpChannel::send_reply(req, str);
                return;
            }
            ss << prof_file.rdbuf();
            prof_file.close();
            std::string str = ss.str();
            HttpChannel::send_reply(req, str);
        }

        // text type. we will return readable content via http response
        std::stringstream readable_res;
        Status st = PprofUtils::get_readable_profile(tmp_prof_file_name.str(), true, &readable_res);
        if (!st.ok()) {
            HttpChannel::send_reply(req, st.to_string());
        } else {
            HttpChannel::send_reply(req, readable_res.str());
        }
    } else {
        // generate flamegraph
        std::string svg_file_content;
        std::string flamegraph_install_dir =
                std::string(std::getenv("DORIS_HOME")) + "/tools/FlameGraph/";
        Status st = PprofUtils::generate_flamegraph(30, flamegraph_install_dir, false,
                                                    &svg_file_content);
        if (!st.ok()) {
            HttpChannel::send_reply(req, st.to_string());
        } else {
            HttpChannel::send_reply(req, svg_file_content);
        }
    }
#endif
}

void PProfHandler::_do_pmu_profile_action(brpc::Controller* cntl) {}

void PProfHandler::_do_content_action(brpc::Controller* cntl) {}

void PProfHandler::_do_cmd_line_action(brpc::Controller* cntl) {
    FILE* fp = fopen("/proc/self/cmdline", "r");
    if (fp == nullptr) {
        std::string str = "Unable to open file: /proc/self/cmdline";
        on_succ(cntl, str);
        return;
    }

    std::string str;
    char buf[1024];
    if (fscanf(fp, "%1023s ", buf) == 1) {
        str = buf;
    } else {
        str = "Unable to read file: /proc/self/cmdline";
    }

    fclose(fp);

    on_succ(cntl, str);
}

void PProfHandler::_do_symbol_action(brpc::Controller* cntl, brpc::HttpMethod method) {
    // TODO: Implement symbol resolution. Without this, the binary needs to be passed
    // to pprof to resolve all symbols.
    if (method == brpc::HttpMethod::HTTP_METHOD_GET) {
        std::stringstream ss;
        ss << "num_symbols: " << get_exec_env()->bfd_parser()->num_symbols();
        std::string str = ss.str();

        on_succ(cntl, str);
        return;
    } else if (method == brpc::HttpMethod::HTTP_METHOD_HEAD) {
        on_succ(cntl, "");
        return;
    } else if (method == brpc::HttpMethod::HTTP_METHOD_POST) {
        std::string request = cntl->request_attachment().to_string();
        // parse address
        std::string result;
        const char* ptr = request.c_str();
        const char* end = request.c_str() + request.size();
        while (ptr < end && *ptr != '\0') {
            std::string file_name;
            std::string func_name;
            unsigned int lineno = 0;
            const char* old_ptr = ptr;
            if (!get_exec_env()->bfd_parser()->decode_address(ptr, &ptr, &file_name, &func_name,
                                                              &lineno)) {
                result.append(old_ptr, ptr - old_ptr);
                result.push_back('\t');
                result.append(func_name);
                result.push_back('\n');
            }
            if (ptr < end && *ptr == '+') {
                ptr++;
            }
        }

        on_succ(cntl, result);
    }
}

} // namespace doris