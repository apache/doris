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
#include <stddef.h>
#include <stdint.h>

#include <map>
#include <string>

#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "util/hash_util.hpp" // IWYU pragma: keep
#include "util/slice.h"

namespace doris {

class ExecEnv;

namespace io {

// Reader of broker file
class BrokerFileWriter : public FileWriter {
public:
    BrokerFileWriter(ExecEnv* env, const TNetworkAddress& broker_address,
                     const std::map<std::string, std::string>& properties, const std::string& path,
                     int64_t start_offset, FileSystemSPtr fs);
    virtual ~BrokerFileWriter();

    Status close() override;
    Status abort() override;
    Status appendv(const Slice* data, size_t data_cnt) override;
    Status finalize() override;
    Status write_at(size_t offset, const Slice& data) override {
        return Status::NotSupported("not support");
    }

private:
    Status _open();
    Status _write(const uint8_t* buf, size_t buf_len, size_t* written_bytes);

private:
    ExecEnv* _env = nullptr;
    const TNetworkAddress _address;
    const std::map<std::string, std::string>& _properties;
    int64_t _cur_offset;
    TBrokerFD _fd;
};

} // end namespace io
} // end namespace doris
