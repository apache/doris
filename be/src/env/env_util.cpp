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

#include "env/env_util.h"

#include "env/env.h"
#include "util/faststring.h"

using std::shared_ptr;
using std::string;

namespace doris {
namespace env_util {

Status open_file_for_write(Env* env, const string& path, shared_ptr<WritableFile>* file) {
    return open_file_for_write(WritableFileOptions(), env, path, file);
}

Status open_file_for_write(const WritableFileOptions& opts, Env* env, const string& path,
                           shared_ptr<WritableFile>* file) {
    std::unique_ptr<WritableFile> w;
    RETURN_IF_ERROR(env->new_writable_file(opts, path, &w));
    file->reset(w.release());
    return Status::OK();
}

Status open_file_for_random(Env* env, const string& path, shared_ptr<RandomAccessFile>* file) {
    std::unique_ptr<RandomAccessFile> r;
    RETURN_IF_ERROR(env->new_random_access_file(path, &r));
    file->reset(r.release());
    return Status::OK();
}

static Status do_write_string_to_file(Env* env, const Slice& data, const std::string& fname,
                                      bool should_sync) {
    std::unique_ptr<WritableFile> file;
    Status s = env->new_writable_file(fname, &file);
    if (!s.ok()) {
        return s;
    }
    s = file->append(data);
    if (s.ok() && should_sync) {
        s = file->sync();
    }
    if (s.ok()) {
        s = file->close();
    }
    file.reset(); // Will auto-close if we did not close above
    if (!s.ok()) {
        RETURN_NOT_OK_STATUS_WITH_WARN(env->delete_file(fname),
                                       "Failed to delete partially-written file " + fname);
    }
    return s;
}

Status write_string_to_file(Env* env, const Slice& data, const std::string& fname) {
    return do_write_string_to_file(env, data, fname, false);
}

Status write_string_to_file_sync(Env* env, const Slice& data, const std::string& fname) {
    return do_write_string_to_file(env, data, fname, true);
}

Status read_file_to_string(Env* env, const std::string& fname, std::string* data) {
    data->clear();
    std::unique_ptr<RandomAccessFile> file;
    Status s = env->new_random_access_file(fname, &file);
    if (!s.ok()) {
        return s;
    }
    s = file->read_all(data);
    return s;
}

} // namespace env_util
} // namespace doris
