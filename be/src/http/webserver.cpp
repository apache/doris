// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "http/webserver.h"

#include <stdio.h>
#include <signal.h>
#include <string>
#include <map>
#include <boost/lexical_cast.hpp>
#include <boost/foreach.hpp>
#include <boost/bind.hpp>
#include <boost/mem_fn.hpp>
#include <boost/algorithm/string.hpp>

#include "common/logging.h"
#include "util/cpu_info.h"
#include "util/disk_info.h"
#include "util/mem_info.h"
#include "util/url_coding.h"
#include "util/logging.h"
#include "util/debug_util.h"
#include "util/palo_metrics.h"
#include "util/runtime_profile.h"
#include "http/http_response.h"

namespace palo {

const char* get_default_documnet_root();

// Mongoose requires a non-null return from the callback to signify successful processing
static void* PROCESSING_COMPLETE = reinterpret_cast<void*>(1);

// Returns $PALO_HOME if set, otherwise /tmp/impala_www
const char* get_default_documnet_root() {
    std::stringstream ss;
    char* palo_home = getenv("PALO_HOME");

    if (palo_home == NULL) {
        return ""; // Empty document root means don't serve static files
    } else {
        ss << palo_home;
    }

    // Deliberate memory leak, but this should be called exactly once.
    std::string* str = new std::string(ss.str());
    return str->c_str();
}

Webserver::Webserver() : 
        _port(config::webserver_port),
        _interface(config::webserver_interface),
        _context(nullptr) {
    pthread_rwlock_init(&_rw_lock, nullptr);                
}

Webserver::~Webserver() {
    stop();
    pthread_rwlock_destroy(&_rw_lock);                
}

Status Webserver::start() {
    LOG(INFO) << "Starting webserver on " << _interface
              << (_interface.empty() ? "all interfaces, port " : ":") << _port;

    std::string port_as_string = boost::lexical_cast<std::string>(_port);
    std::stringstream listening_spec;
    listening_spec << _interface << (_interface.empty() ? "" : ":") << port_as_string;
    std::string listening_str = listening_spec.str();
    std::vector<const char*> options;

    if (!config::webserver_doc_root.empty() && config::enable_webserver_doc_root) {
        LOG(INFO) << "Document root: " << config::webserver_doc_root;
        options.push_back("document_root");
        options.push_back(config::webserver_doc_root.c_str());
    } else {
        LOG(INFO) << "Document root disabled";
    }

    options.push_back("listening_ports");
    options.push_back(listening_str.c_str());

    // Options must be a NULL-terminated list
    options.push_back(NULL);

    // mongoose ignores SIGCHLD and we need it to run kinit. This means that since
    // mongoose does not reap its own children CGI programs must be avoided.
    // Save the signal handler so we can restore it after mongoose sets it to be ignored.
    sighandler_t sig_chld = signal(SIGCHLD, SIG_DFL);

    // To work around not being able to pass member functions as C callbacks, we store a
    // pointer to this server in the per-server state, and register a static method as the
    // default callback. That method unpacks the pointer to this and calls the real
    // callback.
    _context = mg_start(&Webserver::mongoose_callback_static, reinterpret_cast<void*>(this),
                        &options[0]);

    // Restore the child signal handler so wait() works properly.
    signal(SIGCHLD, sig_chld);

    if (_context == NULL) {
        std::stringstream error_msg;
        error_msg << "Could not start webserver on port: " << _port;
        return Status(error_msg.str());
    }

    LOG(INFO) << "Webserver started";
    return Status::OK;
}

void Webserver::stop() {
    if (_context != NULL) {
        mg_stop(_context);
    }
}

bool Webserver::register_handler(const HttpMethod& method, 
                                 const std::string& path, 
                                 HttpHandler* handler) {
    if (handler == nullptr) {
        LOG(WARNING) << "dummy handler for http method " << method << " with path " << path;
        return false;
    }

    bool result = false;
    pthread_rwlock_wrlock(&_rw_lock);
    PathTrie<HttpHandler*>* root = nullptr;
    switch (method) {
    case GET:
        root = &_get_handlers;
        break;
    case PUT:
        root = &_put_handlers;
        break;
    case POST:
        root = &_post_handlers;
        break;
    case DELETE:
        root = &_delete_handlers;
        break;
    case HEAD:
        root = &_head_handlers;
        break;
    case OPTIONS:
        root = &_options_handlers;
        break;
    default:
        LOG(WARNING) << "No such method(" << method << ") for http";
        result = false;
    }
    result = root->insert(path, handler);
    pthread_rwlock_unlock(&_rw_lock);
    return result;
}

HttpHandler* Webserver::get_handler(HttpRequest& req) {
    std::string path = req.raw_path();

    pthread_rwlock_rdlock(&_rw_lock);
    HttpHandler* handler = nullptr;
    switch (req.method()) {
    case GET:
        _get_handlers.retrieve(path, &handler, req.params());
        break;
    case PUT:
        _put_handlers.retrieve(path, &handler, req.params());
        break;
    case POST:
        _post_handlers.retrieve(path, &handler, req.params());
        break;
    case DELETE:
        _delete_handlers.retrieve(path, &handler, req.params());
        break;
    case HEAD:
        _head_handlers.retrieve(path, &handler, req.params());
        break;
    case OPTIONS:
        _options_handlers.retrieve(path, &handler, req.params());
        break;
    default:
        LOG(WARNING) << "No such method(" << req.method() << ") for http";
        break;
    }
    pthread_rwlock_unlock(&_rw_lock);
    return handler;
}

void* Webserver::mongoose_callback_static(
        enum mg_event event,
        struct mg_connection* connection) {
    if (event == MG_NEW_REQUEST) {
        Webserver* instance = reinterpret_cast<Webserver*>(mg_get_user_data(connection));
        return instance->mongoose_callback(connection);
    } else if (event == MG_EVENT_LOG) {
        LOG(INFO) << "mongoose cry: " << mg_get_log_message(connection);
        return PROCESSING_COMPLETE;
    } else if (event == MG_REQUEST_COMPLETE) {
        return PROCESSING_COMPLETE;
    } 
    LOG(INFO) << "unknown mongoose event " << event;
    // No operation
    return nullptr;
}

class PaloMetrics;
void* Webserver::mongoose_callback(struct mg_connection* mg_conn) {
    HttpRequest request(mg_conn);
    HttpChannel channel(request, mg_conn);
    
    // No such handler
    HttpHandler* handler = get_handler(request);
    if (handler == nullptr) {
        std::stringstream ss;
        ss << "{\"status\":\"FAILED\","
            << "\"msg\":\"No handler for URI " << request.uri() 
            << " of method " << to_method_desc(request.method()) << "\"}";
        std::string str = ss.str();
        HttpResponse response(HttpStatus::NOT_FOUND, &str);
        channel.send_response(response);
        return PROCESSING_COMPLETE;
    }

    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        handler->handle(&request, &channel);
    }
    PaloMetrics::http_requests_total.increment(1);
    PaloMetrics::http_request_duration_us.increment(duration_ns / 1000);
    PaloMetrics::http_request_send_bytes.increment(channel.send_bytes());

    // return code to mongoose
    return PROCESSING_COMPLETE;
}

}
