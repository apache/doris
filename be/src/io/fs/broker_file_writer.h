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

#include <gen_cpp/PaloBrokerService_types.h>
#include <gen_cpp/Types_types.h>

#include <map>
#include <string>

#include "common/status.h"
#include "io/fs/file_writer.h"
#include "util/hash_util.hpp" // IWYU pragma: keep
#include "util/slice.h"

namespace doris {

class ExecEnv;

namespace io {
struct FileCacheAllocatorBuilder;
class BrokerFileWriter final : public FileWriter {
public:
    // Create and open file writer
    static Result<FileWriterPtr> create(ExecEnv* env, const TNetworkAddress& broker_address,
                                        const std::map<std::string, std::string>& properties,
                                        Path path);

    BrokerFileWriter(ExecEnv* env, const TNetworkAddress& broker_address, Path path, TBrokerFD fd);
    ~BrokerFileWriter() override;
    Status close(bool non_block = false) override;

    Status appendv(const Slice* data, size_t data_cnt) override;
    const Path& path() const override { return _path; }
    size_t bytes_appended() const override { return _cur_offset; }
    State state() const override { return _state; }
    FileCacheAllocatorBuilder* cache_builder() const override { return nullptr; }

private:
    Status _write(const uint8_t* buf, size_t buf_len, size_t* written_bytes);
    Status _close_impl();

    ExecEnv* _env = nullptr;
    const TNetworkAddress _address;
    Path _path;
    size_t _cur_offset = 0;
    TBrokerFD _fd;
    State _state {State::OPENED};
};

} // end namespace io
} // end namespace doris
