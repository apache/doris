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

#include "http/download_action.h"

#include <string>
#include <sstream>

#include "boost/lexical_cast.hpp"

#include "agent/cgroups_mgr.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_status.h"
#include "util/defer_op.h"
#include "util/file_utils.h"

namespace palo {

const std::string FILE_PARAMETER = "file";
const std::string DB_PARAMETER = "db";
const std::string LABEL_PARAMETER = "label";

DownloadAction::DownloadAction(ExecEnv* exec_env, const std::string& base_dir) :
        _exec_env(exec_env),
        _base_dir(base_dir) {
}

void DownloadAction::handle(HttpRequest *req, HttpChannel *channel) {
    LOG(INFO) << "accept one request " << req->debug_string();

    // add tid to cgroup in order to limit read bandwidth
    CgroupsMgr::apply_system_cgroup();
    // Get 'file' parameter, then assembly file absolute path
    const std::string& file_path = req->param(FILE_PARAMETER);
    if (file_path.empty()) {
        std::string error_msg = std::string(
                "parameter " + FILE_PARAMETER + " not specified in url.");
        HttpResponse response(HttpStatus::OK, &error_msg);
        channel->send_response(response);
        return;
    }
    std::string file_absolute_path = _base_dir;
    if (!file_absolute_path.empty()) {
        const std::string& db_name = req->param(DB_PARAMETER);
        if (!db_name.empty()) {
            file_absolute_path += "/" + db_name;
        }
        const std::string& label_name = req->param(LABEL_PARAMETER);
        if (!label_name.empty()) {
            file_absolute_path += "/" + label_name;
        }
        file_absolute_path += "/" + file_path;
    } else {
        // in tablet download, a absolute path is given.
        file_absolute_path = file_path;
    }
    VLOG_ROW << "absolute download path: " << file_absolute_path;

    if (FileUtils::is_dir(file_absolute_path)) {
        do_dir_response(file_absolute_path, req, channel);
        return;
    } else {
        do_file_response(file_absolute_path, req, channel);
    }
    LOG(INFO) << "deal with requesst finished! ";

}

void DownloadAction::do_dir_response(
        const std::string& dir_path, HttpRequest *req, HttpChannel *channel) {
    std::vector<std::string> files;
    Status status = FileUtils::scan_dir(dir_path, &files);
    if (!status.ok()) {
        LOG(WARNING) << "Failed to scan dir. dir=" << dir_path;
        HttpResponse response(HttpStatus::INTERNAL_SERVER_ERROR);
        channel->send_response(response);
    }

    const std::string FILE_DELIMETER_IN_DIR_RESPONSE = "\n";

    std::stringstream result;
    for (const std::string& file_name : files) {
        result << file_name << FILE_DELIMETER_IN_DIR_RESPONSE;
    }

    std::string result_str = result.str();
    HttpResponse response(HttpStatus::OK, &result_str);
    channel->send_response(response);
    return;
} 


void DownloadAction::do_file_response(
        const std::string& file_path, HttpRequest *req, HttpChannel *channel) {
    // read file content and send response
    FILE* fp = fopen(file_path.c_str(), "rb");
    if (fp == nullptr) {
        LOG(WARNING) << "Failed to open file: " << file_path;
        HttpResponse response(HttpStatus::NOT_FOUND);
        channel->send_response(response);
        return;
    }
    DeferOp close_file(std::bind(&fclose, fp));
    int64_t file_size = get_file_size(fp);

    // TODO(lingbin): process "IF_MODIFIED_SINCE" header
    // TODO(lingbin): process "RANGE" header
    const std::string& range_header = req->header(HttpHeaders::RANGE);
    if (!range_header.empty()) {
        // analyse range header
    }

    HttpResponse response(HttpStatus::OK);
    response.add_header(
            std::string(HttpHeaders::CONTENT_LENGTH),
            boost::lexical_cast<std::string>(file_size));
    response.add_header(
            std::string(HttpHeaders::CONTENT_TYPE),
            get_content_type(file_path));

    if (req->method() == HttpMethod::HEAD) {
        channel->send_response_header(response);
        return;
    }

    channel->send_response_header(response);
    const int BUFFER_SIZE = 4096;
    char *buffer = new char[BUFFER_SIZE];
    int32_t readed_size = 0;
    bool eos = false;
    do {
        Status status = get_file_content(fp, buffer, BUFFER_SIZE, &readed_size, &eos);
        if (!status.ok()) {
            LOG(ERROR) << "Something is wrong when read file: " << file_path;
            break;
        }
        channel->append_response_content(response, buffer, readed_size);
    } while (!eos);

    delete[] buffer;
}

Status DownloadAction::get_file_content(
        FILE* fp, char* buffer, int32_t buffer_size,
        int32_t* readed_size, bool* eos) {
    *readed_size = fread(buffer, sizeof(char), buffer_size, fp);
    if (*readed_size != buffer_size) {
        if (::ferror(fp)) {
            return Status("something wrong when read file");
        } else if (::feof(fp)) {
            *eos = true;
            return Status::OK;
        }
    }
    *eos = false;
    return Status::OK;
}

int64_t DownloadAction::get_file_size(FILE* fp) {
    int64_t current_pos = ::ftell(fp);
    ::fseek(fp, 0, SEEK_END);
    int64_t file_size = ftell(fp);
    ::fseek(fp, current_pos, SEEK_SET);
    return file_size;
}

// If 'file_name' contains a dot but does not consist solely of one or to two dots,
// returns the substring of file_name starting at the rightmost dot and ending at the path's end.
// Otherwise, returns an empty string
std::string DownloadAction::get_file_extension(const std::string& file_name) {
    // Get file Extention
    std::string file_extension;
    for (int i = file_name.size() - 1; i > 0; --i) {
        if (file_name[i] == '/') {
            break;
        }
        if (file_name[i] == '.' && file_name[i-1] != '.') {
            return std::string(file_name, i);
        }
    }
    return file_extension;
}

// Do a simple decision, only deal a few type
std::string DownloadAction::get_content_type(const std::string& file_name) {
    std::string file_ext = get_file_extension(file_name);
    LOG(INFO) << "file_name: " << file_name << "; file extension: [" << file_ext << "]";
    if (file_ext == std::string(".html")
            || file_ext == std::string(".htm")) {
        return std::string("text/html; charset=utf-8");
    } else if (file_ext == std::string(".js")) {
        return std::string("application/javascript; charset=utf-8");
    } else if (file_ext == std::string(".css")) {
        return std::string("text/css; charset=utf-8");
    } else if (file_ext == std::string(".txt")) {
        return std::string("text/plain; charset=utf-8");
    } else {
        return "text/plain; charset=utf-8";
    }
    return std::string();
}

} // end namespace palo

