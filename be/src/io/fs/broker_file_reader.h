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

#include <atomic>
#include <memory>

#include "common/status.h"
#include "io/fs/broker_file_system.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/path.h"
#include "runtime/client_cache.h"
#include "util/slice.h"

namespace doris::io {

struct IOContext;

class BrokerFileReader : public FileReader {
public:
    BrokerFileReader(const TNetworkAddress& broker_addr, Path path, size_t file_size, TBrokerFD fd,
                     std::shared_ptr<BrokerFileSystem> fs);

    ~BrokerFileReader() override;

    Status close() override;

    const Path& path() const override { return _path; }

    size_t size() const override { return _file_size; }

    bool closed() const override { return _closed.load(std::memory_order_acquire); }

    FileSystemSPtr fs() const override { return _fs; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* io_ctx) override;

private:
    const Path _path;
    size_t _file_size;

    const TNetworkAddress _broker_addr;
    TBrokerFD _fd;

    std::shared_ptr<BrokerFileSystem> _fs;
    std::atomic<bool> _closed = false;
};
} // namespace doris::io
