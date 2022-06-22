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

#include <stdio.h>

#include "io/file_writer.h"

namespace doris {

class RuntimeState;

class LocalFileWriter : public FileWriter {
public:
    LocalFileWriter(const std::string& path, int64_t start_offset);

    ~LocalFileWriter() override;

    Status open() override;

    virtual Status write(const uint8_t* buf, size_t buf_len, size_t* written_len) override;

    virtual Status close() override;

private:
    static Status _check_file_path(const std::string& file_path);

    std::string _path;
    int64_t _start_offset;
    FILE* _fp;
};

} // end namespace doris
