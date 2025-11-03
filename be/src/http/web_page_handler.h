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

#pragma once

#include <functional>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>

#include "http/http_handler.h"
#include "http/http_handler_with_auth.h"

namespace doris {

class EvHttpServer;
class EasyJson;
class HttpRequest;

// This a handler for webpage request
// and this handler manage all the page handler
class WebPageHandler : public HttpHandlerWithAuth {
public:
    typedef std::map<std::string, std::string> ArgumentMap;
    typedef std::function<void(const ArgumentMap& args, std::stringstream* output)>
            PageHandlerCallback;
    typedef std::function<void(const ArgumentMap& args, EasyJson* output)>
            TemplatePageHandlerCallback;

    WebPageHandler(EvHttpServer* http_server, ExecEnv* exec_env);
    virtual ~WebPageHandler();

    void handle(HttpRequest* req) override;

    // Register a route 'path' to be rendered via template.
    // The appropriate template to use is determined by 'path'.
    // If 'is_on_nav_bar' is true, a link to the page will be placed on the navbar
    // in the header of styled pages. The link text is given by 'alias'.
    void register_template_page(const std::string& path, const std::string& alias,
                                const TemplatePageHandlerCallback& callback, bool is_on_nav_bar);

    // Register a route 'path'. See the register_template_page for details.
    void register_page(const std::string& path, const std::string& alias,
                       const PageHandlerCallback& callback, bool is_on_nav_bar);

private:
    void root_handler(const ArgumentMap& args, EasyJson* output);

    // Returns a mustache tag that renders the partial at path when
    // passed to mustache::RenderTemplate.
    std::string mustache_partial_tag(const std::string& path) const;

    // Returns whether or not a mustache template corresponding
    // to the given path can be found.
    bool mustache_template_available(const std::string& path) const;

    // Renders the main HTML template with the pre-rendered string 'content'
    // in the main body of the page, into 'output'.
    void render_main_template(const std::string& content, std::stringstream* output);

    // Renders the template corresponding to 'path' (if available), using
    // fields in 'ej'.
    void render(const std::string& path, const EasyJson& ej, bool use_style,
                std::stringstream* output);

    bool static_pages_available() const;

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

    std::string _www_path;
    EvHttpServer* _http_server;
    // Lock guarding the _path_handlers map
    std::mutex _map_lock;
    // Map of path to a PathHandler containing a list of handlers for that
    // path. More than one handler may register itself with a path so that many
    // components may contribute to a single page.
    typedef std::map<std::string, PathHandler*> PageHandlersMap;
    PageHandlersMap _page_map;
};

} // namespace doris
