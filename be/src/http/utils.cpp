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

#include "http/utils.h"

#include <fcntl.h>
#include <stdint.h>
#include <sys/stat.h>
#include <unistd.h>

#include <memory>
#include <ostream>
#include <vector>

#include "common/logging.h"
#include "common/status.h"
#include "common/utils.h"
#include "http/http_channel.h"
#include "http/http_common.h"
#include "http/http_headers.h"
#include "http/http_method.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "util/path_util.h"
#include "util/url_coding.h"

namespace doris {

std::string encode_basic_auth(const std::string& user, const std::string& passwd) {
    std::string auth = user + ":" + passwd;
    std::string encoded_auth;
    base64_encode(auth, &encoded_auth);
    static std::string s_prefix = "Basic ";
    return s_prefix + encoded_auth;
}

bool parse_basic_auth(const HttpRequest& req, std::string* user, std::string* passwd) {
    const char k_basic[] = "Basic ";
    auto& auth = req.header(HttpHeaders::AUTHORIZATION);
    if (auth.compare(0, sizeof(k_basic) - 1, k_basic, sizeof(k_basic) - 1) != 0) {
        return false;
    }
    std::string encoded_str = auth.substr(sizeof(k_basic) - 1);
    std::string decoded_auth;
    if (!base64_decode(encoded_str, &decoded_auth)) {
        return false;
    }
    auto pos = decoded_auth.find(':');
    if (pos == std::string::npos) {
        return false;
    }
    user->assign(decoded_auth.c_str(), pos);
    passwd->assign(decoded_auth.c_str() + pos + 1);

    return true;
}

bool parse_basic_auth(const HttpRequest& req, AuthInfo* auth) {
    auto& token = req.header("token");
    auto& auth_code = req.header(HTTP_AUTH_CODE);
    if (!token.empty()) {
        auth->token = token;
    } else if (!auth_code.empty()) {
        auth->auth_code = std::stoll(auth_code);
    } else {
        std::string full_user;
        if (!parse_basic_auth(req, &full_user, &auth->passwd)) {
            return false;
        }
        auto pos = full_user.find('@');
        if (pos != std::string::npos) {
            auth->user.assign(full_user.data(), pos);
            auth->cluster.assign(full_user.data() + pos + 1);
        } else {
            auth->user = full_user;
        }
    }

    // set user ip
    if (req.remote_host() != nullptr) {
        auth->user_ip.assign(req.remote_host());
    } else {
        auth->user_ip.assign("");
    }

    return true;
}

// Do a simple decision, only deal a few type
std::string get_content_type(const std::string& file_name) {
    std::string file_ext = path_util::file_extension(file_name);
    VLOG_TRACE << "file_name: " << file_name << "; file extension: [" << file_ext << "]";
    if (file_ext == std::string(".html") || file_ext == std::string(".htm")) {
        return std::string("text/html; charset=utf-8");
    } else if (file_ext == std::string(".js")) {
        return std::string("application/javascript; charset=utf-8");
    } else if (file_ext == std::string(".css")) {
        return std::string("text/css; charset=utf-8");
    } else if (file_ext == std::string(".txt")) {
        return std::string("text/plain; charset=utf-8");
    } else if (file_ext == std::string(".png")) {
        return std::string("image/png");
    } else if (file_ext == std::string(".ico")) {
        return std::string("image/x-icon");
    } else {
        return "text/plain; charset=utf-8";
    }
    return "";
}

void do_file_response(const std::string& file_path, HttpRequest* req) {
    if (file_path.find("..") != std::string::npos) {
        LOG(WARNING) << "Not allowed to read relative path: " << file_path;
        HttpChannel::send_error(req, HttpStatus::FORBIDDEN);
        return;
    }

    // read file content and send response
    int fd = open(file_path.c_str(), O_RDONLY);
    if (fd < 0) {
        LOG(WARNING) << "Failed to open file: " << file_path;
        HttpChannel::send_error(req, HttpStatus::NOT_FOUND);
        return;
    }
    struct stat st;
    auto res = fstat(fd, &st);
    if (res < 0) {
        close(fd);
        LOG(WARNING) << "Failed to open file: " << file_path;
        HttpChannel::send_error(req, HttpStatus::NOT_FOUND);
        return;
    }

    int64_t file_size = st.st_size;

    // TODO(lingbin): process "IF_MODIFIED_SINCE" header
    // TODO(lingbin): process "RANGE" header
    const std::string& range_header = req->header(HttpHeaders::RANGE);
    if (!range_header.empty()) {
        // analyse range header
    }

    req->add_output_header(HttpHeaders::CONTENT_TYPE, get_content_type(file_path).c_str());

    if (req->method() == HttpMethod::HEAD) {
        close(fd);
        req->add_output_header(HttpHeaders::CONTENT_LENGTH, std::to_string(file_size).c_str());
        HttpChannel::send_reply(req);
        return;
    }

    HttpChannel::send_file(req, fd, 0, file_size);
}

void do_dir_response(const std::string& dir_path, HttpRequest* req) {
    bool exists = true;
    std::vector<io::FileInfo> files;
    Status st = io::global_local_filesystem()->list(dir_path, true, &files, &exists);
    if (!st.ok()) {
        LOG(WARNING) << "Failed to scan dir. " << st;
        HttpChannel::send_error(req, HttpStatus::INTERNAL_SERVER_ERROR);
    }

    const std::string FILE_DELIMITER_IN_DIR_RESPONSE = "\n";

    std::stringstream result;
    for (auto& file : files) {
        result << file.file_name << FILE_DELIMITER_IN_DIR_RESPONSE;
    }

    std::string result_str = result.str();
    HttpChannel::send_reply(req, result_str);
}

} // namespace doris
