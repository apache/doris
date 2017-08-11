// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#include <fstream>
#include <sstream>
#include <iostream>
#include <mutex>

#include <gperftools/profiler.h>
#include <gperftools/heap-profiler.h>
#include <gperftools/malloc_extension.h>

#include "common/config.h"
#include "http/http_handler.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/webserver.h"
#include "runtime/exec_env.h"
#include "util/bfd_parser.h"

namespace palo {

// pprof default sample time in seconds.
static const std::string SECOND_KEY = "seconds";
static const int kPprofDefaultSampleSecs = 30; 

// Protect, only one thread can work
static std::mutex kPprofActionMutex;

class HeapAction : public HttpHandler {
public:
    HeapAction() { }
    virtual ~HeapAction() { }

    virtual void handle(HttpRequest *req, HttpChannel *channel) override;
};

void HeapAction::handle(HttpRequest* req, HttpChannel* channel) {
#ifdef ADDRESS_SANITIZER
    (void)kPprofDefaultSampleSecs; // Avoid unused variable warning.

    std::string str = "Heap profiling is not available with address sanitizer builds.";
    HttpResponse response(HttpStatus::OK, &str);
    channel->send_response(response);
#else
    std::lock_guard<std::mutex> lock(kPprofActionMutex);

    int seconds = kPprofDefaultSampleSecs;
    const std::string& seconds_str = req->param(SECOND_KEY);
    if (!seconds_str.empty()) {
        seconds = std::atoi(seconds_str.c_str());
    }

    std::stringstream tmp_prof_file_name;
    // Build a temporary file name that is hopefully unique.
    tmp_prof_file_name << config::pprof_profile_dir << "/heap_profile."
        << getpid() << "." << rand();

    HeapProfilerStart(tmp_prof_file_name.str().c_str());
    // Sleep to allow for some samples to be collected.
    sleep(seconds);
    const char* profile = GetHeapProfile();
    HeapProfilerStop();
    std::string str = profile;
    delete profile;
    HttpResponse response(HttpStatus::OK, &str);
    channel->send_response(response);
#endif
}

class GrowthAction : public HttpHandler {
public:
    GrowthAction() { }
    virtual ~GrowthAction() { }

    virtual void handle(HttpRequest *req, HttpChannel *channel) override;
};

void GrowthAction::handle(HttpRequest* req, HttpChannel* channel) {
#ifdef ADDRESS_SANITIZER
    std::string str = "Growth profiling is not available with address sanitizer builds.";
    HttpResponse response(HttpStatus::OK, &str);
    channel->send_response(response);
#else
    std::lock_guard<std::mutex> lock(kPprofActionMutex);

    std::string heap_growth_stack;
    MallocExtension::instance()->GetHeapGrowthStacks(&heap_growth_stack);
    HttpResponse response(HttpStatus::OK, &heap_growth_stack);
    channel->send_response(response);
#endif
}

class ProfileAction : public HttpHandler {
public:
    ProfileAction() { }
    virtual ~ProfileAction() { }

    virtual void handle(HttpRequest *req, HttpChannel *channel) override;
};

void ProfileAction::handle(HttpRequest *req, HttpChannel *channel) {
#ifdef ADDRESS_SANITIZER
    std::string str = "CPU profiling is not available with address sanitizer builds.";
    HttpResponse response(HttpStatus::OK, &str);
    channel->send_response(response);
#else
    std::lock_guard<std::mutex> lock(kPprofActionMutex);

    int seconds = kPprofDefaultSampleSecs;
    const std::string& seconds_str = req->param(SECOND_KEY);
    if (!seconds_str.empty()) {
        seconds = std::atoi(seconds_str.c_str());
    }

    std::ostringstream tmp_prof_file_name;
    // Build a temporary file name that is hopefully unique.
    tmp_prof_file_name << config::pprof_profile_dir << "/palo_profile." 
        << getpid() << "." << rand();
    ProfilerStart(tmp_prof_file_name.str().c_str());
    sleep(seconds);
    ProfilerStop();
    std::ifstream prof_file(tmp_prof_file_name.str().c_str(), std::ios::in);
    std::stringstream ss;
    if (!prof_file.is_open()) {
        ss << "Unable to open cpu profile: " << tmp_prof_file_name.str();
        std::string str = ss.str();
        HttpResponse response(HttpStatus::OK, &str);
        channel->send_response(response);
        return;
    }
    ss << prof_file.rdbuf();
    prof_file.close();
    std::string str = ss.str();
    HttpResponse response(HttpStatus::OK, &str);
    channel->send_response(response);
#endif
}

class PmuProfileAction : public HttpHandler {
public:
    PmuProfileAction() { }
    virtual ~PmuProfileAction() { }
    virtual void handle(HttpRequest *req, HttpChannel *channel) override {
    }
};

class ContentionAction : public HttpHandler {
public:
    ContentionAction() { }
    virtual ~ContentionAction() { }

    virtual void handle(HttpRequest *req, HttpChannel *channel) override {
    }
};

class CmdlineAction : public HttpHandler {
public:
    CmdlineAction() { }
    virtual ~CmdlineAction() { }
    virtual void handle(HttpRequest *req, HttpChannel *channel) override;
};

void CmdlineAction::handle(HttpRequest* req, HttpChannel* channel) {
    FILE* fp = fopen("/proc/self/cmdline", "r");
    if (fp == nullptr) {
        std::string str = "Unable to open file: /proc/self/cmdline";
        HttpResponse response(HttpStatus::OK, &str);
        channel->send_response(response);
        return;
    }
    char buf[1024];
    fscanf(fp, "%s ", buf);
    fclose(fp);
    std::string str = buf;
    HttpResponse response(HttpStatus::OK, &str);
    channel->send_response(response);
}

class SymbolAction : public HttpHandler {
public:
    SymbolAction(BfdParser* parser) : _parser(parser) { }
    virtual ~SymbolAction() { }

    virtual void handle(HttpRequest *req, HttpChannel *channel) override;

private:
    BfdParser* _parser;
};

static Status save_to_string(std::string* str, int64_t len, HttpChannel* channel) {
    const int64_t BUF_SIZE = 4096;
    char buf[BUF_SIZE];
    int64_t to_read = len;
    while (to_read > 0) {
        int64_t to_read_this_time = std::min(to_read, BUF_SIZE);
        int64_t read_this_time = channel->read(buf, to_read_this_time);
        if (to_read_this_time != read_this_time) {
            // what can i do??
            char errmsg[64];
            LOG(INFO) << "read chunked data failed, need=" << to_read_this_time
                << " and read=" << read_this_time
                << ",syserr=" << strerror_r(errno, errmsg, 64);
            return Status("Failed when receiving http packet.");
        }
        str->append(buf, read_this_time);
        // write will write all buf into file, so that write_len == read_this_time
        to_read -= read_this_time;
    }
    return Status::OK;
}

void SymbolAction::handle(HttpRequest* req, HttpChannel* channel) {
    // TODO: Implement symbol resolution. Without this, the binary needs to be passed
    // to pprof to resolve all symbols.
    if (req->method() == HttpMethod::GET) {
        std::stringstream ss;
        ss << "num_symbols: " << _parser->num_symbols();
        std::string str = ss.str();
        HttpResponse response(HttpStatus::OK, &str);
        channel->send_response(response);
        return;
    } else if (req->method() == HttpMethod::HEAD) {
        HttpResponse response(HttpStatus::OK);
#if 0
        response.add_header(
            std::string(HttpHeaders::CONTENT_LENGTH), std::to_string(file_size));
        response.add_header(
            std::string(HttpHeaders::CONTENT_TYPE),
            get_content_type(file_path));
#endif

        channel->send_response_header(response);
        return;
    } else if (req->method() == HttpMethod::POST) {
        // read buf
        std::string request;
        if (!req->header(HttpHeaders::CONTENT_LENGTH).empty()) {
            Status st;
            int64_t len = std::stol(req->header(HttpHeaders::CONTENT_LENGTH));
            if (len > 32 * 1024 * 1024) {
                st = Status("File size exceed max size(32MB) we can support.");
            } else {
                st = save_to_string(&request, len, channel);
            }
            if (!st.ok()) {
                std::string str = st.get_error_msg();
                HttpResponse response(HttpStatus::OK, &str);
                channel->send_response(response);
                return;
            }
        } else {
            std::stringstream ss;
            ss << "There is no " << HttpHeaders::CONTENT_LENGTH << " in request headers";
            std::string str = ss.str();
            HttpResponse response(HttpStatus::OK, &str);
            channel->send_response(response);
            return;
        }
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
        HttpResponse response(HttpStatus::OK, &result);
        channel->send_response(response);
    }
}

Status PprofActions::setup(ExecEnv* exec_env, Webserver* http_server) {
    http_server->register_handler(HttpMethod::GET, "/pprof/heap",
                                  new HeapAction());
    http_server->register_handler(HttpMethod::GET, "/pprof/growth",
                                  new GrowthAction());
    http_server->register_handler(HttpMethod::GET, "/pprof/profile",
                                  new ProfileAction());
    http_server->register_handler(HttpMethod::GET, "/pprof/pmuprofile",
                                  new PmuProfileAction());
    http_server->register_handler(HttpMethod::GET, "/pprof/contention",
                                  new ContentionAction());
    http_server->register_handler(HttpMethod::GET, "/pprof/cmdline",
                                  new CmdlineAction());
    auto action = new SymbolAction(exec_env->bfd_parser());
    http_server->register_handler(HttpMethod::GET, "/pprof/symbol", action);
    http_server->register_handler(HttpMethod::HEAD, "/pprof/symbol", action);
    http_server->register_handler(HttpMethod::POST, "/pprof/symbol", action);
    return Status::OK;
}

}
