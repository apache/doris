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
#include <limits>
#include <ostream>
#include <string>
#include <thread>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "io/fs/broker_file_system.h"
#include "util/doris_metrics.h"

namespace doris::io {
struct IOContext;

BrokerFileReader::BrokerFileReader(const TNetworkAddress& broker_addr, Path path, size_t file_size,
                                   TBrokerFD fd,
                                   std::shared_ptr<BrokerServiceConnection> connection)
        : _path(std::move(path)),
          _file_size(file_size),
          _broker_addr(broker_addr),
          _fd(fd),
          _connection(std::move(connection)) {
    DorisMetrics::instance()->broker_file_open_reading->increment(1);
    DorisMetrics::instance()->broker_file_reader_total->increment(1);
}

BrokerFileReader::~BrokerFileReader() {
    static_cast<void>(BrokerFileReader::close());
}

Status BrokerFileReader::close() {
    bool expected = false;
    if (_closed.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        if (!_connection->is_alive()) {
            return Status::InternalError("connect to broker failed");
        }

        TBrokerCloseReaderRequest request;
        request.__set_version(TBrokerVersion::VERSION_ONE);
        request.__set_fd(_fd);

        TBrokerOperationStatus response;
        try {
            try {
                (*_connection)->closeReader(response, request);
            } catch (apache::thrift::transport::TTransportException&) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
                RETURN_IF_ERROR((*_connection).reopen());
                (*_connection)->closeReader(response, request);
            }
        } catch (apache::thrift::TException& e) {
            std::stringstream ss;
            ss << "close broker file failed, broker:" << _broker_addr << " failed:" << e.what();
            return Status::RpcError(ss.str());
        }

        if (response.statusCode != TBrokerOperationStatusCode::OK) {
            std::stringstream ss;
            ss << "close broker file failed, broker:" << _broker_addr
               << " failed:" << response.message;
            return Status::InternalError(ss.str());
        }

        DorisMetrics::instance()->broker_file_open_reading->increment(-1);
    }
    return Status::OK();
}

Status BrokerFileReader::read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                                      const IOContext* /*io_ctx*/) {
    if (closed()) [[unlikely]] {
        return Status::InternalError("read closed file: ", _path.native());
    }

    size_t bytes_req = result.size;
    char* to = result.data;
    *bytes_read = 0;
    if (UNLIKELY(bytes_req == 0)) {
        return Status::OK();
    }

    if (!_connection->is_alive()) {
        return Status::InternalError("connect to broker failed");
    }

    // If max_chunk_size_for_broker is <= 0, read all at once; otherwise read in chunks
    const size_t MAX_READ_SIZE = (config::max_chunk_size_for_broker <= 0) 
                                  ? std::numeric_limits<size_t>::max() 
                                  : config::max_chunk_size_for_broker;
    size_t remaining = bytes_req;
    size_t current_offset = offset;
    size_t total_bytes_read = 0;

    while (remaining > 0) {
        size_t chunk_size = std::min(remaining, MAX_READ_SIZE);
        TBrokerPReadRequest request;
        request.__set_version(TBrokerVersion::VERSION_ONE);
        request.__set_fd(_fd);
        request.__set_offset(current_offset);
        request.__set_length(chunk_size);

        TBrokerReadResponse response;
        try {
            VLOG_RPC << "send pread request to broker:" << _broker_addr
                     << " position:" << current_offset
                     << ", read bytes length:" << chunk_size;
            try {
                (*_connection)->pread(response, request);
            } catch (apache::thrift::transport::TTransportException& e) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
                RETURN_IF_ERROR((*_connection).reopen());
                LOG(INFO) << "retry reading from broker: " << _broker_addr << ". reason: " << e.what();
                (*_connection)->pread(response, request);
            }
        } catch (apache::thrift::TException& e) {
            std::stringstream ss;
            ss << "read broker file failed, broker:" << _broker_addr << " failed:" << e.what();
            return Status::RpcError(ss.str());
        }

        if (response.opStatus.statusCode == TBrokerOperationStatusCode::END_OF_FILE) {
            // read the end of broker's file
            return Status::OK();
        }
        if (response.opStatus.statusCode != TBrokerOperationStatusCode::OK) {
            std::stringstream ss;
            ss << "Open broker reader failed, broker:" << _broker_addr
               << " failed:" << response.opStatus.message;
            return Status::InternalError(ss.str());
        }

        memcpy(to + total_bytes_read, response.data.data(), response.data.size());
        total_bytes_read += response.data.size();
        current_offset += response.data.size();
        
        if (response.data.size() < chunk_size) {
            break;
        }

        remaining -= response.data.size();
    }

    *bytes_read = total_bytes_read;
    return Status::OK();
}

} // namespace doris::io
