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

#include "http/web_page_handler.h"

#include <boost/bind.hpp>
#include <boost/mem_fn.hpp>

#include "common/config.h"
#include "env/env.h"
#include "gutil/stl_util.h"
#include "gutil/strings/substitute.h"
#include "http/ev_http_server.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_status.h"
#include "http/utils.h"
#include "olap/file_helper.h"
#include "util/cpu_info.h"
#include "util/debug_util.h"
#include "util/disk_info.h"
#include "util/mem_info.h"

using strings::Substitute;

namespace doris {

static std::string s_html_content_type = "text/html";

WebPageHandler::WebPageHandler(EvHttpServer* server) : _http_server(server) {
    // Make WebPageHandler to be static file handler, static files, e.g. css, png, will be handled by WebPageHandler.
    _http_server->register_static_file_handler(this);

    PageHandlerCallback root_callback =
            boost::bind<void>(boost::mem_fn(&WebPageHandler::root_handler), this, _1, _2);
    register_page("/", "Home", root_callback, false /* is_on_nav_bar */);
}

WebPageHandler::~WebPageHandler() {
    STLDeleteValues(&_page_map);
}

void WebPageHandler::register_page(const std::string& path, const string& alias,
                                   const PageHandlerCallback& callback, bool is_on_nav_bar) {
    boost::mutex::scoped_lock lock(_map_lock);
    CHECK(_page_map.find(path) == _page_map.end());
    // first time, register this to web server
    _http_server->register_handler(HttpMethod::GET, path, this);
    _page_map[path] = new PathHandler(true /* is_styled */, is_on_nav_bar, alias, callback);
}

void WebPageHandler::handle(HttpRequest* req) {
    LOG(INFO) << req->debug_string();

    PathHandler* handler = nullptr;
    {
        boost::mutex::scoped_lock lock(_map_lock);
        auto iter = _page_map.find(req->raw_path());
        if (iter != _page_map.end()) {
            handler = iter->second;
        }
    }

    if (handler == nullptr) {
        // Try to handle static file request
        do_file_response(std::string(getenv("DORIS_HOME")) + "/www/" + req->raw_path(), req);
        // Has replied in do_file_response, so we return here.
        return;
    }

    const auto& params = *req->params();

    // Should we render with css styles?
    bool use_style = (params.find("raw") == params.end());

    std::stringstream content;
    // Append header    
    if (use_style) {
        bootstrap_page_header(&content); 
    }

    // Append content
    handler->callback()(params, &content);

    // Append footer
    if (use_style) {
        bootstrap_page_footer(&content);
    }

    req->add_output_header(HttpHeaders::CONTENT_TYPE, s_html_content_type.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, content.str());
}

static const std::string PAGE_HEADER = R"(
<!DOCTYPE html>
<html>
  <head>
    <title>Doris</title>
    <meta charset='utf-8'/>
    <link href='/bootstrap/css/bootstrap.min.css' rel='stylesheet' media='screen' />
    <link href='/bootstrap/css/bootstrap-table.min.css' rel='stylesheet' media='screen' />
    <script src='/jquery-3.2.1.min.js' defer></script>
    <script src='/bootstrap/js/bootstrap.min.js' defer></script>
    <script src='/bootstrap/js/bootstrap-table.min.js' defer></script>
    <script src='/doris.js' defer></script>
    <link href='/doris.css' rel='stylesheet' />
  </head>
  <body>
)";

static const std::string NAVIGATION_BAR_PREFIX = R"(
    <nav class="navbar navbar-default">
      <div class="container-fluid">
        <div class="navbar-header">
          <a class="navbar-brand" style="padding-top: 5px;" href="/">
            <img src="/logo.png" width='40' height='40' alt="Doris" />
          </a>
        </div>
        <div id="navbar" class="navbar-collapse collapse">
          <ul class="nav navbar-nav">
)";

static const std::string NAVIGATION_BAR_SUFFIX = R"(
          </ul>
        </div><!--/.nav-collapse -->
      </div><!--/.container-fluid -->
    </nav>
)";

static const std::string PAGE_FOOTER = R"(
  </body>
</html>
)";

void WebPageHandler::bootstrap_page_header(std::stringstream* output) {
    boost::mutex::scoped_lock lock(_map_lock);
    (*output) << PAGE_HEADER;
    (*output) << NAVIGATION_BAR_PREFIX;
    for (auto& iter : _page_map) {
        (*output) << "<li><a href=\"" << iter.first << "\">" << iter.first << "</a></li>";
    }
    (*output) << NAVIGATION_BAR_SUFFIX;
}

void WebPageHandler::bootstrap_page_footer(std::stringstream* output) {
    (*output) << PAGE_FOOTER;
}

void WebPageHandler::root_handler(const ArgumentMap& args, std::stringstream* output) {
    // _path_handler_lock already held by MongooseCallback
    (*output) << "<h2>Version</h2>";
    (*output) << "<pre>" << get_version_string(false) << "</pre>" << std::endl;
    (*output) << "<h2>Hardware Info</h2>";
    (*output) << "<pre>";
    (*output) << CpuInfo::debug_string();
    (*output) << MemInfo::debug_string();
    (*output) << DiskInfo::debug_string();
    (*output) << "</pre>";

    (*output) << "<h2>Status Pages</h2>";
    for (auto& iter : _page_map) {
        (*output) << "<a href=\"" << iter.first << "\">" << iter.first << "</a><br/>";
    }
}

} // namespace doris
