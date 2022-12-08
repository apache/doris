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

#include "gen_cpp/PlanNodes_types.h"
#include "io/file_reader.h"
#include "io/file_writer.h"

namespace doris {

namespace io {
class FileSystem;
} // namespace io

// This class is used to create hdfs readers and writers.
// Because libhdfs3 does not support the arm64 environment,
// we use this class to shield the upper layer from the need to deal with the platform environment
// when creating a reader or writer.
//
// If in the arm64 environment, creating a reader or writer through this class will return an error.
class HdfsReaderWriter {
public:
    static Status create_reader(const THdfsParams& hdfs_params, const std::string& path,
                                int64_t start_offset, FileReader** reader);

    static Status create_reader(const std::map<std::string, std::string>& properties,
                                const std::string& path, int64_t start_offset, FileReader** reader);

    static Status create_writer(const std::map<std::string, std::string>& properties,
                                const std::string& path, std::unique_ptr<FileWriter>& writer);
};
} // namespace doris
