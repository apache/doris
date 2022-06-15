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

#include <map>
#include <string>

#include "gen_cpp/PlanNodes_types.h"
#include "exec/file_writer.h"
#include "exec/hdfs_builder.h"

namespace doris {
class HDFSWriter : public FileWriter {

public:
    HDFSWriter(std::map<std::string, std::string>& properties, const std::string& path);
    ~HDFSWriter();
    Status open() override;

    // Writes up to count bytes from the buffer pointed buf to the file.
    // NOTE: the number of bytes written may be less than count if.
    Status write(const uint8_t* buf, size_t buf_len, size_t* written_len) override;

    Status close() override;

private:
    Status _connect();
    THdfsParams _parse_properties(std::map<std::string, std::string>& prop);

    std::map<std::string, std::string> _properties;
    std::string _namenode = "";
    std::string _path = "";
    hdfsFS _hdfs_fs = nullptr;
    hdfsFile _hdfs_file = nullptr;
    bool _closed = false;
    THdfsParams _hdfs_params;
    HDFSCommonBuilder _builder;
};

}
