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

#include "io/hdfs_reader_writer.h"

#include "io/hdfs_file_reader.h"
#include "io/hdfs_writer.h"

namespace doris {

Status HdfsReaderWriter::create_reader(const THdfsParams& hdfs_params, const std::string& path,
                                       int64_t start_offset, FileReader** reader) {
    *reader = new HdfsFileReader(hdfs_params, path, start_offset);
    return Status::OK();
}

Status HdfsReaderWriter::create_reader(const std::map<std::string, std::string>& hdfs_params,
                                       const std::string& path, int64_t start_offset,
                                       FileReader** reader) {
    *reader = new HdfsFileReader(hdfs_params, path, start_offset);
    return Status::OK();
}

Status HdfsReaderWriter::create_writer(const std::map<std::string, std::string>& properties,
                                       const std::string& path,
                                       std::unique_ptr<FileWriter>& writer) {
    writer.reset(new HDFSWriter(properties, path));
    return Status::OK();
}
} // namespace doris
