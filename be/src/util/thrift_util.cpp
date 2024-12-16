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

#include "util/thrift_util.h"

#include <gen_cpp/Types_types.h>
#include <thrift/TOutput.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportException.h>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <string>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/logging.h"
#include "exec/tablet_info.h"
#include "olap/tablet_schema.h"
#include "util/thrift_server.h"

namespace apache::thrift::protocol {
class TProtocol;
} // namespace apache::thrift::protocol

// TCompactProtocol requires some #defines to work right.  They also define UNLIKELY
// so we need to undef this.
// TODO: is there a better include to use?
#ifdef UNLIKELY
#undef UNLIKELY
#endif
#ifndef SIGNED_RIGHT_SHIFT_IS
#define SIGNED_RIGHT_SHIFT_IS 1
#endif

#ifndef ARITHMETIC_RIGHT_SHIFT
#define ARITHMETIC_RIGHT_SHIFT 1
#endif

#include <thrift/protocol/TCompactProtocol.h>

#include <sstream>
#include <thread>

namespace doris {

ThriftSerializer::ThriftSerializer(bool compact, int initial_buffer_size)
        : _mem_buffer(new apache::thrift::transport::TMemoryBuffer(initial_buffer_size)) {
    if (compact) {
        apache::thrift::protocol::TCompactProtocolFactoryT<apache::thrift::transport::TMemoryBuffer>
                factory;
        _protocol = factory.getProtocol(_mem_buffer);
    } else {
        apache::thrift::protocol::TBinaryProtocolFactoryT<apache::thrift::transport::TMemoryBuffer>
                factory;
        _protocol = factory.getProtocol(_mem_buffer);
    }
}

std::shared_ptr<apache::thrift::protocol::TProtocol> create_deserialize_protocol(
        std::shared_ptr<apache::thrift::transport::TMemoryBuffer> mem, bool compact) {
    if (compact) {
        apache::thrift::protocol::TCompactProtocolFactoryT<apache::thrift::transport::TMemoryBuffer>
                tproto_factory;
        return tproto_factory.getProtocol(mem);
    } else {
        apache::thrift::protocol::TBinaryProtocolFactoryT<apache::thrift::transport::TMemoryBuffer>
                tproto_factory;
        return tproto_factory.getProtocol(mem);
    }
}

// Comparator for THostPorts. Thrift declares this (in gen-cpp/Types_types.h) but
// never defines it.
bool TNetworkAddress::operator<(const TNetworkAddress& that) const {
    if (this->hostname < that.hostname) {
        return true;
    } else if ((this->hostname == that.hostname) && (this->port < that.port)) {
        return true;
    }

    return false;
};

static void thrift_output_function(const char* output) {
    VLOG_QUERY << output;
}

void init_thrift_logging() {
    apache::thrift::GlobalOutput.setOutputFunction(thrift_output_function);
}

Status wait_for_local_server(const ThriftServer& server, int num_retries, int retry_interval_ms) {
    return wait_for_server("localhost", server.port(), num_retries, retry_interval_ms);
}

Status wait_for_server(const std::string& host, int port, int num_retries, int retry_interval_ms) {
    int retry_count = 0;

    while (retry_count < num_retries) {
        try {
            apache::thrift::transport::TSocket socket(host, port);
            // Timeout is in ms
            socket.setConnTimeout(500);
            socket.open();
            socket.close();
            return Status::OK();
        } catch (apache::thrift::transport::TTransportException& e) {
            VLOG_QUERY << "Connection failed: " << e.what();
        }

        ++retry_count;
        VLOG_QUERY << "Waiting " << retry_interval_ms << "ms for Thrift server at " << host << ":"
                   << port << " to come up, failed attempt " << retry_count << " of "
                   << num_retries;
        std::this_thread::sleep_for(std::chrono::milliseconds(retry_interval_ms));
    }

    return Status::InternalError("Server did not come up");
}

void t_network_address_to_string(const TNetworkAddress& address, std::string* out) {
    std::stringstream ss;
    ss << address;
    *out = ss.str();
}

bool t_network_address_comparator(const TNetworkAddress& a, const TNetworkAddress& b) {
    int cmp = a.hostname.compare(b.hostname);

    if (cmp < 0) {
        return true;
    }

    if (cmp == 0) {
        return a.port < b.port;
    }

    return false;
}

std::string to_string(const TUniqueId& id) {
    return std::to_string(id.hi).append(std::to_string(id.lo));
}

bool _has_inverted_index_v1_or_partial_update(TOlapTableSink sink) {
    OlapTableSchemaParam schema;
    if (!schema.init(sink.schema).ok()) {
        return false;
    }
    if (schema.is_partial_update()) {
        return true;
    }
    for (const auto& index_schema : schema.indexes()) {
        for (const auto& index : index_schema->indexes) {
            if (index->index_type() == INVERTED) {
                if (sink.schema.inverted_index_file_storage_format ==
                    TInvertedIndexFileStorageFormat::V1) {
                    return true;
                } else {
                    return false;
                }
            }
        }
    }
    return false;
}

} // namespace doris
