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

#include <hdfs/hdfs.h>

#include "filesystem/write_stream.h"

namespace doris {

class HdfsWriteStream final : public WriteStream {
public:
    HdfsWriteStream(hdfsFS fs, hdfsFile file);
    ~HdfsWriteStream() override;

    Status write(const char* from, size_t put_n) override;

    Status sync() override;

    Status close() override;

    bool closed() const override { return _closed; }

private:
    hdfsFS _fs = nullptr;     // owned
    hdfsFile _file = nullptr; // owned

    bool _closed = false;
};

} // namespace doris
