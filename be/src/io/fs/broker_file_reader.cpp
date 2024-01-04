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

#include "io/fs/broker_file_reader.h"

#include <gen_cpp/TPaloBrokerService.h>
#include <string.h>
#include <thrift/Thrift.h>
#include <thrift/transport/TTransportException.h>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <ostream>
#include <string>
#include <thread>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/logging.h"
#include "common/status.h"
#include "io/fs/broker_file_system.h"
#include "util/doris_metrics.h"

namespace doris::io {
class IOContext;

BrokerFileReader::BrokerFileReader(const TNetworkAddress& broker_addr, Path path, size_t file_size,
                                   TBrokerFD fd, std::shared_ptr<BrokerFileSystem> fs)
        : _path(std::move(path)),
          _file_size(file_size),
          _broker_addr(broker_addr),
          _fd(fd),
          _fs(std::move(fs)) {
    DorisMetrics::instance()->broker_file_open_reading->increment(1);
    DorisMetrics::instance()->broker_file_reader_total->increment(1);
}

BrokerFileReader::~BrokerFileReader() {
    BrokerFileReader::close();
}

Status BrokerFileReader::close() {
    bool expected = false;
    if (_closed.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        RETURN_IF_ERROR(_fs->close_file(_fd));
        DorisMetrics::instance()->broker_file_open_reading->increment(-1);
    }
    return Status::OK();
}

Status BrokerFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                      const IOContext* /*io_ctx*/) {
    DCHECK(!closed());
    size_t bytes_req = result.size;
    char* to = result.data;
    *bytes_read = 0;
    if (UNLIKELY(bytes_req == 0)) {
        return Status::OK();
    }

    std::string data;
    RETURN_IF_ERROR(_fs->read_file(_fd, offset, bytes_req, &data));

    *bytes_read = data.size();
    memcpy(to, data.data(), *bytes_read);
    return Status::OK();
}

} // namespace doris::io
