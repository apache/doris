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

namespace doris {
namespace io {
struct IOContext;

BrokerFileReader::BrokerFileReader(const TNetworkAddress& broker_addr, const Path& path,
                                   size_t file_size, TBrokerFD fd,
                                   std::shared_ptr<BrokerFileSystem> fs)
        : _path(path),
          _file_size(file_size),
          _broker_addr(broker_addr),
          _fd(fd),
          _fs(std::move(fs)) {
    static_cast<void>(_fs->get_client(&_client));
    DorisMetrics::instance()->broker_file_open_reading->increment(1);
    DorisMetrics::instance()->broker_file_reader_total->increment(1);
}

BrokerFileReader::~BrokerFileReader() {
    static_cast<void>(BrokerFileReader::close());
}

Status BrokerFileReader::close() {
    bool expected = false;
    if (_closed.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        TBrokerCloseReaderRequest request;
        request.__set_version(TBrokerVersion::VERSION_ONE);
        request.__set_fd(_fd);

        TBrokerOperationStatus response;
        try {
            try {
                (*_client)->closeReader(response, request);
            } catch (apache::thrift::transport::TTransportException&) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
                RETURN_IF_ERROR((*_client).reopen());
                (*_client)->closeReader(response, request);
            }
        } catch (apache::thrift::TException& e) {
            std::stringstream ss;
            ss << "Close broker reader failed, broker:" << _broker_addr << " failed:" << e.what();
            return Status::RpcError(ss.str());
        }

        if (response.statusCode != TBrokerOperationStatusCode::OK) {
            std::stringstream ss;
            ss << "close broker reader failed, broker:" << _broker_addr
               << " failed:" << response.message;
            return Status::InternalError(ss.str());
        }

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

    TBrokerPReadRequest request;
    request.__set_version(TBrokerVersion::VERSION_ONE);
    request.__set_fd(_fd);
    request.__set_offset(offset);
    request.__set_length(bytes_req);

    TBrokerReadResponse response;
    try {
        VLOG_RPC << "send pread request to broker:" << _broker_addr << " position:" << offset
                 << ", read bytes length:" << bytes_req;
        try {
            (*_client)->pread(response, request);
        } catch (apache::thrift::transport::TTransportException& e) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            RETURN_IF_ERROR((*_client).reopen());
            LOG(INFO) << "retry reading from broker: " << _broker_addr << ". reason: " << e.what();
            (*_client)->pread(response, request);
        }
    } catch (apache::thrift::TException& e) {
        std::stringstream ss;
        ss << "Open broker reader failed, broker:" << _broker_addr << " failed:" << e.what();
        return Status::RpcError(ss.str());
    }

    if (response.opStatus.statusCode == TBrokerOperationStatusCode::END_OF_FILE) {
        // read the end of broker's file
        *bytes_read = 0;
        return Status::OK();
    } else if (response.opStatus.statusCode != TBrokerOperationStatusCode::OK) {
        std::stringstream ss;
        ss << "Open broker reader failed, broker:" << _broker_addr
           << " failed:" << response.opStatus.message;
        return Status::InternalError(ss.str());
    }

    *bytes_read = response.data.size();
    memcpy(to, response.data.data(), *bytes_read);
    return Status::OK();
}

} // namespace io
} // namespace doris
