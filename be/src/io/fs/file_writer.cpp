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

#include "io/fs/file_writer.h"

#include <future>

#include "io/fs/file_system.h"
#include "io/io_common.h"
#include "util/async_io.h"

namespace doris {
namespace io {

Status FileWriter::close(FileWriterCloseOptions option) {
    Status st;
    if (!option.nonblock) {
        st = _close(option.flush);
    } else {
        _close_future = _close_promise.get_future();
        auto task = [o = option, this]() mutable {
            _close_promise.set_value(_close(o.flush));
        };
        AsyncIO::run_task(std::move(task), fs()->type());
    }
    if (!st) {
        LOG(WARNING) << st;
    }
    _closed = true;
    return st;
}

Status FileWriter::wait_until_flush() { 
    if (_close_future.valid()) {
        return _close_future.get();
    }
    return Status::OK();
}

} // namespace io
} // namespace doris
