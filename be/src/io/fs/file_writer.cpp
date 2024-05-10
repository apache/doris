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

#include "runtime/exec_env.h"
#include "util/threadpool.h"

// Used for unit test
namespace {
std::once_flag flag;
std::unique_ptr<doris::ThreadPool> non_block_close_thread_pool;
void init_threadpool_for_test() {
    static_cast<void>(doris::ThreadPoolBuilder("NonBlockCloseThreadPool")
                              .set_min_threads(12)
                              .set_max_threads(48)
                              .build(&non_block_close_thread_pool));
}

[[maybe_unused]] doris::ThreadPool* get_non_block_close_thread_pool() {
    std::call_once(flag, init_threadpool_for_test);
    return non_block_close_thread_pool.get();
}
} // namespace

namespace doris::io {

Status FileWriter::close(bool non_block) {
    if (non_block) {
        if (_fut.valid()) {
            return Status::OK();
        }

        _fut = _pro.get_future();
#ifdef BE_TEST
        return get_non_block_close_thread_pool()->submit_func(
                [&]() { _pro.set_value(close_impl()); });
#else
        return ExecEnv::GetInstance()->non_block_close_thread_pool()->submit_func(
                [&]() { _pro.set_value(close_impl()); });
#endif
    }
    if (_fut.valid()) {
        return _fut.get();
    }
    return close_impl();
}

} // namespace doris::io