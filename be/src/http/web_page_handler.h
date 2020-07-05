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
    virtual ~WebPageHandler();

    void handle(HttpRequest *req) override;

    // Register a route 'path'.
    // If 'is_on_nav_bar' is true, a link to the page will be placed on the navbar
    // in the header of styled pages. The link text is given by 'alias'.
    void register_page(const std::string& path, const std::string& alias,
                       const PageHandlerCallback& callback, bool is_on_nav_bar);

private:
    void bootstrap_page_header(std::stringstream* output);
    void bootstrap_page_footer(std::stringstream* output);
    void root_handler(const ArgumentMap& args, std::stringstream* output);

    // Container class for a list of path handler callbacks for a single URL.
    class PathHandler {
    public:
        PathHandler(bool is_styled, bool is_on_nav_bar, std::string alias,
                    PageHandlerCallback callback)
                : is_styled_(is_styled),
                  is_on_nav_bar_(is_on_nav_bar),
                  alias_(std::move(alias)),
                  callback_(std::move(callback)) {}

        bool is_styled() const { return is_styled_; }
        bool is_on_nav_bar() const { return is_on_nav_bar_; }
        const std::string& alias() const { return alias_; }
        const PageHandlerCallback& callback() const { return callback_; }

    private:
        // If true, the page appears is rendered styled.
        bool is_styled_;

        // If true, the page appears in the navigation bar.
        bool is_on_nav_bar_;

        // Alias used when displaying this link on the nav bar.
        std::string alias_;

        // Callback to render output for this page.
        PageHandlerCallback callback_;
    };

    EvHttpServer* _http_server;
    // Lock guarding the _path_handlers map
    boost::mutex _map_lock;
    // Map of path to a PathHandler containing a list of handlers for that
    // path. More than one handler may register itself with a path so that many
    // components may contribute to a single page.
    typedef std::map<std::string, PathHandler*> PageHandlersMap;
    PageHandlersMap _page_map;
};

}

#endif 
