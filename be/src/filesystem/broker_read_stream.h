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

#include "filesystem/read_stream.h"
#include "gen_cpp/PaloBrokerService_types.h"

namespace doris {

class ExecEnv;
class TNetworkAddress;

class BrokerReadStream final : public ReadStream {
public:
    BrokerReadStream(ExecEnv* env, const std::vector<TNetworkAddress>& addrs, TBrokerFD fd,
                     size_t file_size);
    ~BrokerReadStream() override;

    Status read(char* to, size_t req_n, size_t* read_n) override;

    Status read_at(size_t position, char* to, size_t req_n, size_t* read_n) override;

    Status seek(size_t position) override;

    Status tell(size_t* position) const override;

    Status available(size_t* n_bytes) const override;

    Status close() override;

    bool closed() const override { return _closed; }

private:
    ExecEnv* _env;
    const std::vector<TNetworkAddress>& _addrs;
    int _addr_idx = 0;

    TBrokerFD _fd;
    size_t _file_size;
    size_t _offset = 0;

    bool _closed = false;
};

} // namespace doris
