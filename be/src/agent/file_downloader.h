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

#ifndef BDG_PALO_BE_SRC_AGENT_FILE_DOWNLOADER_H
#define BDG_PALO_BE_SRC_AGENT_FILE_DOWNLOADER_H

#include <iostream>
#include <pthread.h>
#include <cstdint>
#include <sstream>
#include <string>
#include "curl/curl.h"
#include "agent/status.h"
#include "olap/olap_define.h"
#include "olap/file_helper.h"

namespace palo {

const uint32_t GET_LENGTH_TIMEOUT = 10;
const uint32_t CURL_OPT_CONNECTTIMEOUT = 120;
// down load file
class FileDownloader {
public:
    enum OutputType {
        NONE,
        STREAM,
        FILE
    };

    struct FileDownloaderParam {
        std::string username;
        std::string password;
        std::string remote_file_path;
        std::string local_file_path;
        uint32_t curl_opt_timeout;
    };

    explicit FileDownloader(const FileDownloaderParam& param);
    virtual ~FileDownloader() {};
    
    // Download file from remote server
    virtual AgentStatus download_file();

    // List remote dir file
    virtual AgentStatus list_file_dir(std::string* file_list_string);
    
    // Get file length of remote file
    //
    // Output parameters:
    // * length: The pointer of size of remote file 
    virtual AgentStatus get_length(uint64_t* length);
private:
    static size_t _write_file_callback(
            void* buffer, size_t size, size_t nmemb, void* downloader);    
    static size_t _write_stream_callback(
            void* buffer, size_t size, size_t nmemb, void* downloader);    

    AgentStatus _install_opt(
            OutputType output_type, CURL* curl, char* errbuf,
            std::stringstream* output_stream, FileHandler* file_handler);

    void _get_err_info(char * errbuf, CURLcode res);
    
    const FileDownloaderParam& _downloader_param;
    
    DISALLOW_COPY_AND_ASSIGN(FileDownloader);
};  // class FileDownloader
}  // namespace palo
#endif  // BDG_PALO_BE_SRC_AGENT_FILE_DOWNLOADER_H
