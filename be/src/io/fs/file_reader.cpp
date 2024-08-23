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

#include "io/fs/file_reader.h"

#include <bthread/bthread.h>
#include <glog/logging.h>

#include "io/fs/file_system.h"
#include "runtime/thread_context.h"
#include "runtime/workload_management/io_throttle.h"
#include "util/async_io.h"

namespace doris {
namespace io {

const std::string FileReader::VIRTUAL_REMOTE_DATA_DIR = "virtual_remote_data_dir";

Status FileReader::read_at(size_t offset, Slice result, size_t* bytes_read,
                           const IOContext* io_ctx) {
    DCHECK(bthread_self() == 0);
    std::shared_ptr<IOThrottle> iot = nullptr;
    if (auto* t_ctx = doris::thread_context(true)) {
        iot = t_ctx->io_throttle(get_data_dir_path());
    }
    if (iot) {
        iot->acquire(-1);
    }
    Status st = read_at_impl(offset, result, bytes_read, io_ctx);
    if (iot) {
        iot->update_next_io_time(*bytes_read);
    }
    if (!st) {
        LOG(WARNING) << st;
    }
    return st;
}

} // namespace io
} // namespace doris
