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

#ifndef BDG_PALO_BE_SRC_AGENT_MOCK_MOCK_FILE_DOWNLOADER_H
#define BDG_PALO_BE_SRC_AGENT_MOCK_MOCK_FILE_DOWNLOADER_H

#include "gmock/gmock.h"
#include "agent/file_downloader.h"

namespace palo {

class MockFileDownloader : public FileDownloader {
public:
    explicit MockFileDownloader(const FileDownloaderParam& param);
    MOCK_METHOD0(download_file, AgentStatus());
    MOCK_METHOD1(list_file_dir, AgentStatus(std::string* file_list_string));
    MOCK_METHOD1(get_length, AgentStatus(uint64_t* length));
};  // class MockFileDownloader
}  // namespace palo
#endif  // BDG_PALO_BE_SRC_AGENT_MOCK_MOCK_FILE_DOWNLOADER_H
