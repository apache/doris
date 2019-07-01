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

#include <stdint.h>

#include <string>
#include <map>

#include "common/status.h"
#include "exec/file_reader.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/PaloBrokerService_types.h"

namespace doris {

class ExecEnv;
class TBrokerRangeDesc;
class TNetworkAddress;
class RuntimeState;

// Reader of broker file
class BrokerReader : public FileReader {
public:
    BrokerReader(ExecEnv* env,
                 const std::vector<TNetworkAddress>& broker_addresses,
                 const std::map<std::string, std::string>& properties,
                 const std::string& path,
                 int64_t start_offset);
    virtual ~BrokerReader();

    virtual Status open() override;

    // Read 
    virtual Status read(uint8_t* buf, size_t* buf_len, bool* eof) override;
    virtual Status readat(int64_t position, int64_t nbytes, int64_t* bytes_read, void* out) override;
    virtual int64_t size () override;
    virtual Status seek(int64_t position) override;
    virtual Status tell(int64_t* position) override;
    virtual void close() override;
    virtual bool closed() override;

private:
    ExecEnv* _env;
    const std::vector<TNetworkAddress>& _addresses;
    const std::map<std::string, std::string>& _properties;
    const std::string& _path;

    int64_t _cur_offset;

    bool _is_fd_valid;
    TBrokerFD _fd;

    int64_t _file_size;
    int _addr_idx;
};

}

