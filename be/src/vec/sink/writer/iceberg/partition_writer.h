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

namespace doris {
namespace io {
class FileSystem;
}

class ObjectPool;
class RuntimeState;
class RuntimeProfile;

namespace iceberg {
class Schema;
}

namespace vectorized {

class Block;
class VFileFormatTransformer;

class IPartitionWriter {
public:
    struct WriteInfo {
        std::string write_path;
        std::string original_write_path;
        std::string target_path;
        TFileType::type file_type;
    };

    IPartitionWriter() = default;
    virtual ~IPartitionWriter() = default;

    virtual Status open(RuntimeState* state, RuntimeProfile* profile, const RowDescriptor* row_desc,
                        ObjectPool* pool) = 0;

    virtual Status write(vectorized::Block& block) = 0;

    virtual Status close(const Status& status) = 0;

    virtual const std::string& file_name() const = 0;

    virtual int file_name_index() const = 0;

    virtual size_t written_len() = 0;
};
} // namespace vectorized
} // namespace doris
