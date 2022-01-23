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

#include "exec/file_writer.h"
#include "util/s3_uri.h"

namespace Aws {
namespace Utils {
class TempFile;
}
namespace S3 {
class S3Client;
}
} // namespace Aws

namespace doris {
class S3Writer : public FileWriter {
public:
    S3Writer(const std::map<std::string, std::string>& properties, const std::string& path,
             int64_t start_offset);
    ~S3Writer();
    Status open() override;

    // Writes up to count bytes from the buffer pointed buf to the file.
    // NOTE: the number of bytes written may be less than count if.
    Status write(const uint8_t* buf, size_t buf_len, size_t* written_len) override;

    Status close() override;

private:
    Status _sync();

    const std::map<std::string, std::string>& _properties;
    std::string _path;
    S3URI _uri;
    std::shared_ptr<Aws::S3::S3Client> _client;
    std::shared_ptr<Aws::Utils::TempFile> _temp_file;
};

} // end namespace doris
