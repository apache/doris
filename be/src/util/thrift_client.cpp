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

#include <util/thrift_client.h>

#include <ostream>

#include <boost/assign.hpp>

namespace doris {

Status ThriftClientImpl::open() {
    try {
        if (!_transport->isOpen()) {
            _transport->open();
        }
    } catch (apache::thrift::transport::TTransportException& e) {
        std::stringstream msg;
        msg << "Couldn't open transport for " << ipaddress() << ":" << port()
            << "(" << e.what() << ")";
        return Status::ThriftRpcError(msg.str());
    }

    return Status::OK();
}

Status ThriftClientImpl::open_with_retry(int num_tries, int wait_ms) {
    DCHECK_GE(wait_ms, 0);
    Status status;
    int try_count = 0L;

    while (num_tries <= 0 || try_count < num_tries) {
        ++try_count;
        status = open();

        if (status.ok()) {
            return status;
        }

        LOG(INFO) << "Unable to connect to " << _ipaddress << ":" << _port;

        if (num_tries < 0) {
            LOG(INFO) << "(Attempt " << try_count << ", will retry indefinitely)";
        } else {
            LOG(INFO) << "(Attempt " << try_count << " of " << num_tries << ")";
        }

        usleep(wait_ms * 1000L);
    }

    return status;
}

Status ThriftClientImpl::close() {
    if (_transport->isOpen()) {
        _transport->close();
    }

    return Status::OK();
}

}
