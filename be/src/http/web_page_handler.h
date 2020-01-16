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

#ifndef DORIS_BE_SRC_COMMON_UTIL_WEB_PAGE_HANDLER_H
#define DORIS_BE_SRC_COMMON_UTIL_WEB_PAGE_HANDLER_H

#include <string>
#include <sstream>
#include <vector>
#include <map>

#include <boost/function.hpp>
#include <boost/thread/mutex.hpp>

#include "http/http_handler.h"

namespace doris {

class EvHttpServer;

// This a handler for webpage request
// and this handler manage all the page handler
class WebPageHandler : public HttpHandler {
public:
    typedef std::map<std::string, std::string> ArgumentMap;
    typedef boost::function<void (const ArgumentMap& args, std::stringstream* output)> 
            PageHandlerCallback;
    WebPageHandler(EvHttpServer* http_server);

    virtual ~WebPageHandler() {
    }

    void handle(HttpRequest *req) override;

    // Just use old code
    void register_page(const std::string& path, const PageHandlerCallback& callback);

private:
    void bootstrap_page_header(std::stringstream* output);
    void bootstrap_page_footer(std::stringstream* output);
    void root_handler(const ArgumentMap& args, std::stringstream* output);

    // all
    class PageHandlers {
    public:
        void add_callback(const PageHandlerCallback& callback) {
            _callbacks.push_back(callback);
        }
        const std::vector<PageHandlerCallback>& callbacks() const {
            return _callbacks;
        }
    private:
        std::vector<PageHandlerCallback> _callbacks;
    };

    EvHttpServer* _http_server;
    // Lock guarding the _path_handlers map
    boost::mutex _map_lock;
    // Map of path to a PathHandler containing a list of handlers for that
    // path. More than one handler may register itself with a path so that many
    // components may contribute to a single page.
    typedef std::map<std::string, PageHandlers> PageHandlersMap;
    PageHandlersMap _page_map;
};

}

#endif 
