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

#include <memory>
#include <string>

#include "common/status.h"
#include "env.h"

namespace doris {

class Env;
class RandomAccessFile;
class WritableFile;
struct WritableFileOptions;

namespace env_util {

Status open_file_for_write(Env* env, const std::string& path, std::shared_ptr<WritableFile>* file);

Status open_file_for_write(const WritableFileOptions& opts, Env* env, const std::string& path,
                           std::shared_ptr<WritableFile>* file);

Status open_file_for_random(Env* env, const std::string& path,
                            std::shared_ptr<RandomAccessFile>* file);

// A utility routine: write "data" to the named file.
Status write_string_to_file(Env* env, const Slice& data, const std::string& fname);
// Like above but also fsyncs the new file.
Status write_string_to_file_sync(Env* env, const Slice& data, const std::string& fname);

// A utility routine: read contents of named file into *data
Status read_file_to_string(Env* env, const std::string& fname, faststring* data);

} // namespace env_util
} // namespace doris
