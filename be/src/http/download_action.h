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

#ifndef  BDG_PALO_BE_SRC_COMMON_UTIL_DOWNLOAD_ACTION_H
#define  BDG_PALO_BE_SRC_COMMON_UTIL_DOWNLOAD_ACTION_H

#include <boost/scoped_ptr.hpp>

#include "exec/csv_scanner.h"
#include "exec/scan_node.h"
#include "runtime/descriptors.h"

namespace palo {

class ExecEnv;

// A simple handler that serves incoming HTTP requests of file-download to send their respective HTTP responses.
//
// TODO(lingbin): implements two useful header ('If-Modified-Since' and 'RANGE') to reduce
// transmission consumption.
// We use parameter named 'file' to specify the static resource path, it relative to the
// root path of http server.
class DownloadAction : public HttpHandler {
public:
    DownloadAction(ExecEnv* exec_env, const std::string& base_dir);

    virtual ~DownloadAction() {}

    virtual void handle(HttpRequest *req, HttpChannel *channel);
private:
    void do_file_response(const std::string& dir_path, HttpRequest *req, HttpChannel *channel);
    void do_dir_response(const std::string& dir_path, HttpRequest *req, HttpChannel *channel);

    Status get_file_content(
            FILE* fp, char* buffer, int32_t buffer_size,
            int32_t* readed_size, bool* eos);

    int64_t get_file_size(FILE* fp);

    std::string get_file_extension(const std::string& file_name);

    std::string get_content_type(const std::string& file_name);

    ExecEnv* _exec_env;
    std::string _base_dir;

}; // end class DownloadAction

} // end namespace palo
#endif // BDG_PALO_BE_SRC_COMMON_UTIL_DOWNLOAD_ACTION_H

