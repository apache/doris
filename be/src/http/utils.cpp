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

#include <ostream>
#include <vector>

#include "common/config.h"
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
#include "olap/wal/wal_manager.h"
#include "runtime/exec_env.h"
#include "util/md5.h"
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
    const auto& auth = req.header(HttpHeaders::AUTHORIZATION);
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
    const auto& token = req.header("token");
    const auto& auth_code = req.header(HTTP_AUTH_CODE);

    std::tuple<std::string, std::string, std::string> tmp;
    auto& [user, pass, cluster] = tmp;
    bool valid_basic_auth = parse_basic_auth(req, &user, &pass);
    if (valid_basic_auth) { // always set the basic auth, the user may be useful
        auto pos = user.find('@');
        if (pos != std::string::npos) {
            cluster.assign(user.c_str() + pos + 1);
            user.assign(user.c_str(), pos); // user is updated
        }
        auth->user = user;
        auth->passwd = pass;
        auth->cluster = cluster;
    }

    if (!token.empty()) {
        auth->token = token;
    } else if (!auth_code.empty()) {
        auth->auth_code = std::stoll(auth_code);
    } else if (!valid_basic_auth) {
        return false;
    }

    // set user ip
    auth->user_ip.assign(req.remote_host() != nullptr ? req.remote_host() : "");

    return true;
}

// Do a simple decision, only deal a few type
std::string get_content_type(const std::string& file_name) {
    std::string file_ext = path_util::file_extension(file_name);
    VLOG_TRACE << "file_name: " << file_name << "; file extension: [" << file_ext << "]";
    if (file_ext == std::string(".html") || file_ext == std::string(".htm")) {
        return "text/html; charset=utf-8";
    } else if (file_ext == std::string(".js")) {
        return "application/javascript; charset=utf-8";
    } else if (file_ext == std::string(".css")) {
        return "text/css; charset=utf-8";
    } else if (file_ext == std::string(".txt")) {
        return "text/plain; charset=utf-8";
    } else if (file_ext == std::string(".png")) {
        return "image/png";
    } else if (file_ext == std::string(".ico")) {
        return "image/x-icon";
    } else {
        return "text/plain; charset=utf-8";
    }
}

void do_file_response(const std::string& file_path, HttpRequest* req,
                      bufferevent_rate_limit_group* rate_limit_group, bool is_acquire_md5) {
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

    if (is_acquire_md5) {
        Md5Digest md5;

        void* buf = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
        md5.update(buf, file_size);
        md5.digest();
        munmap(buf, file_size);

        req->add_output_header(HttpHeaders::CONTENT_MD5, md5.hex().c_str());
    }

    if (req->method() == HttpMethod::HEAD) {
        close(fd);
        req->add_output_header(HttpHeaders::CONTENT_LENGTH, std::to_string(file_size).c_str());
        HttpChannel::send_reply(req);
        return;
    }

    HttpChannel::send_file(req, fd, 0, file_size, rate_limit_group);
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

bool load_size_smaller_than_wal_limit(int64_t content_length) {
    // 1. req->header(HttpHeaders::CONTENT_LENGTH) will return streamload content length. If it is empty or equals to 0, it means this streamload
    // is a chunked streamload and we are not sure its size.
    // 2. if streamload content length is too large, like larger than 80% of the WAL constrain.
    //
    // This two cases, we are not certain that the Write-Ahead Logging (WAL) constraints allow for writing down
    // these blocks within the limited space. So we need to set group_commit = false to avoid dead lock.
    size_t max_available_size = ExecEnv::GetInstance()->wal_mgr()->get_max_available_size();
    return (content_length < 0.8 * max_available_size);
}

} // namespace doris
