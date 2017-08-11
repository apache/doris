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

#include "agent/file_downloader.h"
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include "olap/olap_define.h"
#include "olap/file_helper.h"
#include "olap/utils.h"

using std::ofstream;
using std::ostream;
using std::string;
using std::stringstream;

namespace palo {

FileDownloader::FileDownloader(const FileDownloaderParam& param) :
        _downloader_param(param) {
}

size_t FileDownloader::_write_file_callback(
        void* buffer, size_t size, size_t nmemb, void* param) {
    OLAPStatus status = OLAP_SUCCESS;
    size_t len = size * nmemb;

    if (param == NULL) {
        status = OLAP_ERR_OTHER_ERROR;
        OLAP_LOG_WARNING("File downloader output file handler is NULL pointer.");
        return -1;
    }

    if (status == OLAP_SUCCESS) {
        status = static_cast<FileHandler*>(param)->write(buffer, len);
        if (status != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("File downloader callback write failed.");
        }
    }

    return len;
}

size_t FileDownloader::_write_stream_callback(
        void* buffer, size_t size, size_t nmemb, void* param) {
    AgentStatus status = PALO_SUCCESS;
    size_t len = size * nmemb;

    if (param == NULL) {
        status = PALO_FILE_DOWNLOAD_INVALID_PARAM;
        OLAP_LOG_WARNING("File downloader output stream is NULL pointer.");
        return -1;
    }

    if (status == PALO_SUCCESS) {
        static_cast<ostream*>(param)->write(static_cast<const char*>(buffer), len);
    }

    return len;
}

AgentStatus FileDownloader::_install_opt(
        OutputType output_type, CURL* curl, char* errbuf,
        stringstream* output_stream, FileHandler* file_handler) {
    AgentStatus status = PALO_SUCCESS;
    CURLcode curl_ret = CURLE_OK;

    // Set request URL
    curl_ret = curl_easy_setopt(curl, CURLOPT_URL, _downloader_param.remote_file_path.c_str());

    if (curl_ret != CURLE_OK) {
        status = PALO_FILE_DOWNLOAD_INSTALL_OPT_FAILED;
        OLAP_LOG_WARNING("curl setopt URL failed.[error=%s]", curl_easy_strerror(curl_ret));
    }

    // Set username
    if (status == PALO_SUCCESS) {
        curl_ret = curl_easy_setopt(curl, CURLOPT_USERNAME, _downloader_param.username.c_str());

        if (curl_ret != CURLE_OK) {
            status = PALO_FILE_DOWNLOAD_INSTALL_OPT_FAILED;
            OLAP_LOG_WARNING("curl setopt USERNAME failed.[error=%s]",
                             curl_easy_strerror(curl_ret));
        }
    }

    // Set password
    if (status == PALO_SUCCESS) {
        curl_ret = curl_easy_setopt(curl, CURLOPT_PASSWORD, _downloader_param.password.c_str());

        if (curl_ret != CURLE_OK) {
            status = PALO_FILE_DOWNLOAD_INSTALL_OPT_FAILED;
            OLAP_LOG_WARNING("curl setopt USERNAME failed.[error=%s]",
                             curl_easy_strerror(curl_ret));
        }
    }

    // Set process timeout
    if (status == PALO_SUCCESS) {
        curl_ret = curl_easy_setopt(curl, CURLOPT_TIMEOUT, _downloader_param.curl_opt_timeout);

        if (curl_ret != CURLE_OK) {
            status = PALO_FILE_DOWNLOAD_INSTALL_OPT_FAILED;
            OLAP_LOG_WARNING("curl setopt TIMEOUT failed.[error=%s]", curl_easy_strerror(curl_ret));
        }
    }

    // Set low speed limit and low speed time
    if (status == PALO_SUCCESS) {
        curl_ret = curl_easy_setopt(
                curl, CURLOPT_LOW_SPEED_LIMIT,
                config::download_low_speed_limit_kbps * 1024);

        if (curl_ret != CURLE_OK) {
            status = PALO_FILE_DOWNLOAD_INSTALL_OPT_FAILED;
            OLAP_LOG_WARNING(
                "curl setopt CURLOPT_LOW_SPEED_LIMIT failed.[error=%s]",
                curl_easy_strerror(curl_ret));
        }
    }

    if (status == PALO_SUCCESS) {
        curl_ret = curl_easy_setopt(
                curl, CURLOPT_LOW_SPEED_TIME, config::download_low_speed_time);

        if (curl_ret != CURLE_OK) {
            status = PALO_FILE_DOWNLOAD_INSTALL_OPT_FAILED;
            OLAP_LOG_WARNING(
                "curl setopt CURLOPT_LOW_SPEED_TIME failed.[error=%s]",
                curl_easy_strerror(curl_ret));
        }
    }

    // Set max recv speed(bytes/s)
    if (status == PALO_SUCCESS) {
        curl_ret = curl_easy_setopt(
                curl, CURLOPT_MAX_RECV_SPEED_LARGE, config::max_download_speed_kbps * 1024);

        if (curl_ret != CURLE_OK) {
            status = PALO_FILE_DOWNLOAD_INSTALL_OPT_FAILED;
            OLAP_LOG_WARNING(
                    "curl setopt MAX_RECV_SPEED failed.[error=%s]", curl_easy_strerror(curl_ret));
        }
    }

    // Forbid signals
    if (status == PALO_SUCCESS) {
        curl_ret = curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);

        if (curl_ret != CURLE_OK) {
            status = PALO_FILE_DOWNLOAD_INSTALL_OPT_FAILED;
            OLAP_LOG_WARNING("curl setopt nosignal failed.[error=%s]",
                             curl_easy_strerror(curl_ret));
        }
    }

    if (strncmp(_downloader_param.remote_file_path.c_str(), "http", 4) == 0) {
        if (status == PALO_SUCCESS) {
            curl_ret = curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);

            if (curl_ret != CURLE_OK) {
                OLAP_LOG_WARNING(
                    "curl setopt CURLOPT_FOLLOWLOCATION failed.[error=%s]",
                    curl_easy_strerror(curl_ret));
            }
        }

        if (status == PALO_SUCCESS) {
            curl_ret = curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 20);

            if (curl_ret != CURLE_OK) {
                OLAP_LOG_WARNING(
                    "curl setopt CURLOPT_MAXREDIRS failed.[error=%s]",
                    curl_easy_strerror(curl_ret));
            }
        }
    }

    // Set nobody
    if (status == PALO_SUCCESS) {
        if (output_type == OutputType::NONE) {
            curl_ret = curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
            if (curl_ret != CURLE_OK) {
                OLAP_LOG_WARNING("curl setopt CURLOPT_NOBODY failed.[error=%s]",
                                 curl_easy_strerror(curl_ret));
            }
        } else if (output_type == OutputType::STREAM) {
            // Set callback function
            curl_ret = curl_easy_setopt(
                    curl,
                    CURLOPT_WRITEFUNCTION,
                    &FileDownloader::_write_stream_callback);

            if (curl_ret != CURLE_OK) {
                status = PALO_FILE_DOWNLOAD_INSTALL_OPT_FAILED;
                OLAP_LOG_WARNING("curl setopt WRITEDATA failed.[error=%s]",
                                 curl_easy_strerror(curl_ret));
            }

            // Set callback function args
            if (status == PALO_SUCCESS) {
                curl_ret = curl_easy_setopt(curl, CURLOPT_WRITEDATA,
                                            static_cast<void*>(output_stream));

                if (curl_ret != CURLE_OK) {
                    status = PALO_FILE_DOWNLOAD_INSTALL_OPT_FAILED;
                    OLAP_LOG_WARNING("curl setopt WRITEDATA failed.[error=%s]",
                                     curl_easy_strerror(curl_ret));
                }
            }
        } else if (output_type == OutputType::FILE) {
            // Set callback function
            curl_ret = curl_easy_setopt(
                    curl,
                    CURLOPT_WRITEFUNCTION,
                    &FileDownloader::_write_file_callback);

            if (curl_ret != CURLE_OK) {
                status = PALO_FILE_DOWNLOAD_INSTALL_OPT_FAILED;
                OLAP_LOG_WARNING("curl setopt WRITEDATA failed.[error=%s]",
                                 curl_easy_strerror(curl_ret));
            }

            // Set callback function args
            if (status == PALO_SUCCESS) {
                curl_ret = curl_easy_setopt(curl, CURLOPT_WRITEDATA,
                                            static_cast<void*>(file_handler));

                if (curl_ret != CURLE_OK) {
                    status = PALO_FILE_DOWNLOAD_INSTALL_OPT_FAILED;
                    OLAP_LOG_WARNING("curl setopt WRITEDATA failed.[error=%s]",
                                     curl_easy_strerror(curl_ret));
                }
            }
        }
    }

    // set verbose mode
    /*
    if (status == PALO_SUCCESS) {
        curl_easy_setopt(curl, CURLOPT_VERBOSE, config::curl_verbose_mode);
        if (curl_ret != CURLE_OK) {
            status = PALO_FILE_DOWNLOAD_INSTALL_OPT_FAILED;
            OLAP_LOG_WARNING("curl setopt VERBOSE MODE failed.[error=%s]",
                             curl_easy_strerror(curl_ret));
        }
    }
    */

    // set err buf
    if (status == PALO_SUCCESS) {
        curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errbuf);
        if (curl_ret != CURLE_OK) {
            status = PALO_FILE_DOWNLOAD_INSTALL_OPT_FAILED;
            OLAP_LOG_WARNING("curl setopt ERR BUF failed.[error=%s]",
                             curl_easy_strerror(curl_ret));
        }
        errbuf[0] = 0;
    }

    return status;
}

void FileDownloader::_get_err_info(char * errbuf, CURLcode res) {
    if (res != CURLE_OK) {
        size_t len = strlen(errbuf);
        if (len) {
            OLAP_LOG_WARNING("(%d): %s%s", res, errbuf, ((errbuf[len - 1] != '\n') ? "\n" : "")); 
        } else {
            OLAP_LOG_WARNING("(%d): %s", res, curl_easy_strerror(res)); 
        }
    }
}

AgentStatus FileDownloader::get_length(uint64_t* length) {
    AgentStatus status = PALO_SUCCESS;
    CURL* curl = NULL;
    CURLcode curl_ret = CURLE_OK;
    curl = curl_easy_init();

    // Init curl
    if (curl == NULL) {
        status = PALO_FILE_DOWNLOAD_CURL_INIT_FAILED;
        OLAP_LOG_WARNING("internal error to get NULL curl");
    }

    // Set curl opt
    char errbuf[CURL_ERROR_SIZE];
    if (status == PALO_SUCCESS) {
        status = _install_opt(OutputType::NONE, curl, errbuf, NULL, NULL);

        if (PALO_SUCCESS != status) {
            OLAP_LOG_WARNING("install curl opt failed.");
        }
    }

    // Get result
    if (status == PALO_SUCCESS) {
        curl_ret = curl_easy_perform(curl);

        if (curl_ret != CURLE_OK) {
            status = PALO_FILE_DOWNLOAD_GET_LENGTH_FAILED;
            OLAP_LOG_WARNING("curl get length failed.[path=%s]",
                _downloader_param.remote_file_path.c_str());
            _get_err_info(errbuf, curl_ret);
        } else {
            double content_length = 0.0f;
            curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &content_length);
            *length = (uint64_t)content_length;
        }
    }

    if (curl != NULL) {
        curl_easy_cleanup(curl);
    }

    return status;
}

AgentStatus FileDownloader::download_file() {
    AgentStatus status = PALO_SUCCESS;
    CURL* curl = NULL;
    CURLcode curl_ret = CURLE_OK;
    curl = curl_easy_init();

    if (curl == NULL) {
        status = PALO_FILE_DOWNLOAD_CURL_INIT_FAILED;
        OLAP_LOG_WARNING("internal error to get NULL curl");
    }

    FileHandler* file_handler = new FileHandler();
    OLAPStatus olap_status = OLAP_SUCCESS;
    // Prepare some infomation
    if (status == PALO_SUCCESS) {
        olap_status = file_handler->open_with_mode(
                _downloader_param.local_file_path,
                O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);

        if (olap_status != OLAP_SUCCESS) {
            status = PALO_FILE_DOWNLOAD_INVALID_PARAM;
            OLAP_LOG_WARNING("open loacal file failed.[file_path=%s]",
                   _downloader_param.local_file_path.c_str());
        }
    }

    char errbuf[CURL_ERROR_SIZE];
    if (status == PALO_SUCCESS) {
        status = _install_opt(OutputType::FILE, curl, errbuf, NULL, file_handler);

        if (PALO_SUCCESS != status) {
            OLAP_LOG_WARNING("install curl opt failed.");
        }
    }

    if (status == PALO_SUCCESS) {
        curl_ret = curl_easy_perform(curl);

        if (curl_ret != CURLE_OK) {
            status = PALO_FILE_DOWNLOAD_FAILED;
            OLAP_LOG_WARNING(
                "curl easy perform failed.[path=%s]",
                _downloader_param.remote_file_path.c_str());
            _get_err_info(errbuf, curl_ret);
        }
    }

    if (file_handler != NULL) {
        file_handler->close();
        delete file_handler;
        file_handler = NULL;
    }

    if (curl != NULL) {
        curl_easy_cleanup(curl);
    }

    return status;
}

AgentStatus FileDownloader::list_file_dir(string* file_list_string) {
    AgentStatus status = PALO_SUCCESS;
    CURL* curl = NULL;
    CURLcode curl_ret = CURLE_OK;
    curl = curl_easy_init();

    // Init curl
    if (curl == NULL) {
        status = PALO_FILE_DOWNLOAD_CURL_INIT_FAILED;
        OLAP_LOG_WARNING("internal error to get NULL curl");
    }

    stringstream output_string_stream;
    // Set curl opt
    char errbuf[CURL_ERROR_SIZE];
    if (status == PALO_SUCCESS) {
        status = _install_opt(OutputType::STREAM, curl, errbuf, &output_string_stream, NULL);

        if (PALO_SUCCESS != status) {
            OLAP_LOG_WARNING("install curl opt failed.");
        }
    }

    // Get result
    if (status == PALO_SUCCESS) {
        curl_ret = curl_easy_perform(curl);

        if (curl_ret != CURLE_OK) {
            status = PALO_FILE_DOWNLOAD_LIST_DIR_FAIL;
            OLAP_LOG_WARNING(
                "curl list file dir failed.[path=%s]",
                _downloader_param.remote_file_path.c_str());
            _get_err_info(errbuf, curl_ret);
        }
    }
    
    if (curl != NULL) {
        curl_easy_cleanup(curl);
    }
    *file_list_string = output_string_stream.str();

    return status;
}
}  // namespace palo
