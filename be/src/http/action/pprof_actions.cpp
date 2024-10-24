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

#include "http/action/pprof_actions.h"

#include "http/http_handler_with_auth.h"

#if !defined(__SANITIZE_ADDRESS__) && !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && \
        !defined(THREAD_SANITIZER) && !defined(USE_JEMALLOC)
#include <gperftools/heap-profiler.h>    // IWYU pragma: keep
#include <gperftools/malloc_extension.h> // IWYU pragma: keep
#endif
#if !defined(__SANITIZE_ADDRESS__) && !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && \
        !defined(THREAD_SANITIZER)
#include <gperftools/profiler.h> // IWYU pragma: keep
#endif
#include <stdio.h>

#include <fstream>
#include <memory>
#include <mutex>
#include <string>

#include "common/config.h"
#include "common/object_pool.h"
#include "http/ev_http_server.h"
#include "http/http_channel.h"
#include "http/http_handler.h"
#include "http/http_method.h"
#include "http/http_request.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "util/bfd_parser.h"
#include "util/pprof_utils.h" // IWYU pragma: keep

namespace doris {

// pprof default sample time in seconds.
[[maybe_unused]] static const std::string SECOND_KEY = "seconds";
static const int kPprofDefaultSampleSecs = 30;

// Protect, only one thread can work
static std::mutex kPprofActionMutex;

class HeapAction : public HttpHandlerWithAuth {
public:
    HeapAction(ExecEnv* exec_env) : HttpHandlerWithAuth(exec_env) {}

    virtual ~HeapAction() {}

    virtual void handle(HttpRequest* req) override;
};

void HeapAction::handle(HttpRequest* req) {
    std::lock_guard<std::mutex> lock(kPprofActionMutex);
#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER) || \
        defined(USE_JEMALLOC)
    (void)kPprofDefaultSampleSecs; // Avoid unused variable warning.

    std::string str = "Heap profiling is not available with address sanitizer or jemalloc builds.";

    HttpChannel::send_reply(req, str);
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

class GrowthAction : public HttpHandlerWithAuth {
public:
    GrowthAction(ExecEnv* exec_env) : HttpHandlerWithAuth(exec_env) {}

    virtual ~GrowthAction() {}

    virtual void handle(HttpRequest* req) override;
};

void GrowthAction::handle(HttpRequest* req) {
#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER) || \
        defined(USE_JEMALLOC)
    std::string str =
            "Growth profiling is not available with address sanitizer or jemalloc builds.";
    HttpChannel::send_reply(req, str);
#else
    std::lock_guard<std::mutex> lock(kPprofActionMutex);

    std::string heap_growth_stack;
    MallocExtension::instance()->GetHeapGrowthStacks(&heap_growth_stack);

    HttpChannel::send_reply(req, heap_growth_stack);
#endif
}

class ProfileAction : public HttpHandlerWithAuth {
public:
    ProfileAction(ExecEnv* exec_env) : HttpHandlerWithAuth(exec_env) {}

    virtual ~ProfileAction() {}

    virtual void handle(HttpRequest* req) override;
};

void ProfileAction::handle(HttpRequest* req) {
#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER)
    std::string str = "CPU profiling is not available with address sanitizer or jemalloc builds.";
    HttpChannel::send_reply(req, str);
#else
    std::lock_guard<std::mutex> lock(kPprofActionMutex);

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
            return;
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
        Status st = PprofUtils::generate_flamegraph(seconds, flamegraph_install_dir, false,
                                                    &svg_file_content);
        if (!st.ok()) {
            HttpChannel::send_reply(req, st.to_string());
        } else {
            HttpChannel::send_reply(req, svg_file_content);
        }
    }
#endif
}

class PmuProfileAction : public HttpHandlerWithAuth {
public:
    PmuProfileAction(ExecEnv* exec_env) : HttpHandlerWithAuth(exec_env) {}

    virtual ~PmuProfileAction() {}
    virtual void handle(HttpRequest* req) override {}
};

class ContentionAction : public HttpHandlerWithAuth {
public:
    ContentionAction(ExecEnv* exec_env) : HttpHandlerWithAuth(exec_env) {}

    virtual ~ContentionAction() {}

    virtual void handle(HttpRequest* req) override {}
};

class CmdlineAction : public HttpHandlerWithAuth {
public:
    CmdlineAction(ExecEnv* exec_env) : HttpHandlerWithAuth(exec_env) {}

    virtual ~CmdlineAction() {}
    virtual void handle(HttpRequest* req) override;
};

void CmdlineAction::handle(HttpRequest* req) {
    FILE* fp = fopen("/proc/self/cmdline", "r");
    if (fp == nullptr) {
        std::string str = "Unable to open file: /proc/self/cmdline";
        HttpChannel::send_reply(req, str);
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

    HttpChannel::send_reply(req, str);
}

class SymbolAction : public HttpHandlerWithAuth {
public:
    SymbolAction(BfdParser* parser, ExecEnv* exec_env)
            : HttpHandlerWithAuth(exec_env), _parser(parser) {}
    virtual ~SymbolAction() {}

    virtual void handle(HttpRequest* req) override;

private:
    BfdParser* _parser;
};

void SymbolAction::handle(HttpRequest* req) {
    // TODO: Implement symbol resolution. Without this, the binary needs to be passed
    // to pprof to resolve all symbols.
    if (req->method() == HttpMethod::GET) {
        std::stringstream ss;
        ss << "num_symbols: " << _parser->num_symbols();
        std::string str = ss.str();

        HttpChannel::send_reply(req, str);
        return;
    } else if (req->method() == HttpMethod::HEAD) {
        HttpChannel::send_reply(req);
        return;
    } else if (req->method() == HttpMethod::POST) {
        std::string request = req->get_request_body();
        // parse address
        std::string result;
        const char* ptr = request.c_str();
        const char* end = request.c_str() + request.size();
        while (ptr < end && *ptr != '\0') {
            std::string file_name;
            std::string func_name;
            unsigned int lineno = 0;
            const char* old_ptr = ptr;
            if (!_parser->decode_address(ptr, &ptr, &file_name, &func_name, &lineno)) {
                result.append(old_ptr, ptr - old_ptr);
                result.push_back('\t');
                result.append(func_name);
                result.push_back('\n');
            }
            if (ptr < end && *ptr == '+') {
                ptr++;
            }
        }

        HttpChannel::send_reply(req, result);
    }
}

Status PprofActions::setup(ExecEnv* exec_env, EvHttpServer* http_server, ObjectPool& pool) {
    if (!config::pprof_profile_dir.empty()) {
        RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(config::pprof_profile_dir));
    }

    http_server->register_handler(HttpMethod::GET, "/pprof/heap",
                                  pool.add(new HeapAction(exec_env)));
    http_server->register_handler(HttpMethod::GET, "/pprof/growth",
                                  pool.add(new GrowthAction(exec_env)));
    http_server->register_handler(HttpMethod::GET, "/pprof/profile",
                                  pool.add(new ProfileAction(exec_env)));
    http_server->register_handler(HttpMethod::GET, "/pprof/pmuprofile",
                                  pool.add(new PmuProfileAction(exec_env)));
    http_server->register_handler(HttpMethod::GET, "/pprof/contention",
                                  pool.add(new ContentionAction(exec_env)));
    http_server->register_handler(HttpMethod::GET, "/pprof/cmdline",
                                  pool.add(new CmdlineAction(exec_env)));
    auto action = pool.add(new SymbolAction(exec_env->bfd_parser(), exec_env));
    http_server->register_handler(HttpMethod::GET, "/pprof/symbol", action);
    http_server->register_handler(HttpMethod::HEAD, "/pprof/symbol", action);
    http_server->register_handler(HttpMethod::POST, "/pprof/symbol", action);
    return Status::OK();
}

} // namespace doris
