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
#include <thrift/protocol/TProtocolDecorator.h>
#include <thrift/protocol/TProtocolException.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportException.h>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <limits>
#include <string>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/logging.h"
#include "runtime/thread_context.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/tablet_info.h"
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
namespace {

constexpr size_t DECODED_THRIFT_STRUCT_RESERVATION_BYTES = 1024;

size_t decoded_thrift_value_reservation(apache::thrift::protocol::TType type) {
    using apache::thrift::protocol::T_BOOL;
    using apache::thrift::protocol::T_BYTE;
    using apache::thrift::protocol::T_DOUBLE;
    using apache::thrift::protocol::T_I16;
    using apache::thrift::protocol::T_I32;
    using apache::thrift::protocol::T_I64;
    using apache::thrift::protocol::T_LIST;
    using apache::thrift::protocol::T_MAP;
    using apache::thrift::protocol::T_SET;
    using apache::thrift::protocol::T_STRING;
    using apache::thrift::protocol::T_STRUCT;
    switch (type) {
    case T_BOOL:
    case T_BYTE:
        return 1;
    case T_I16:
        return sizeof(int16_t);
    case T_I32:
        return sizeof(int32_t);
    case T_I64:
    case T_DOUBLE:
        return sizeof(int64_t);
    case T_STRING:
        return sizeof(std::string);
    case T_LIST:
    case T_SET:
    case T_MAP:
        return sizeof(std::vector<uint8_t>);
    case T_STRUCT:
        // Generated structs vary in size. Reserving a conservative inline object budget keeps
        // their eager vector resize inside task admission; the serialized-size reservation covers
        // their dynamic field payloads.
        return DECODED_THRIFT_STRUCT_RESERVATION_BYTES;
    default:
        return 1;
    }
}

class MemoryBudgetProtocol final : public apache::thrift::protocol::TProtocolDecorator {
public:
    MemoryBudgetProtocol(std::shared_ptr<apache::thrift::protocol::TProtocol> protocol,
                         int32_t serialized_size)
            : TProtocolDecorator(std::move(protocol)),
              _memory_manager(thread_context()->thread_mem_tracker_mgr.get()),
              _prior_reservation(_memory_manager->take_reserved_memory()) {
        reserve_or_throw(static_cast<size_t>(serialized_size), /*restore_prior_on_failure=*/true);
    }

    ~MemoryBudgetProtocol() override {
        _memory_manager->shrink_reserved();
        _memory_manager->adopt_reserved_memory(std::move(_prior_reservation));
    }

    uint32_t readMapBegin_virt(apache::thrift::protocol::TType& key_type,
                               apache::thrift::protocol::TType& value_type,
                               uint32_t& size) override {
        const uint32_t consumed = TProtocolDecorator::readMapBegin_virt(key_type, value_type, size);
        const uint32_t count = size;
        const size_t element_size = decoded_thrift_value_reservation(key_type) +
                                    decoded_thrift_value_reservation(value_type) +
                                    4 * sizeof(void*);
        reserve_container(count, element_size);
        return consumed;
    }

    uint32_t readListBegin_virt(apache::thrift::protocol::TType& element_type,
                                uint32_t& size) override {
        const uint32_t consumed = TProtocolDecorator::readListBegin_virt(element_type, size);
        reserve_container(size, decoded_thrift_value_reservation(element_type));
        return consumed;
    }

    uint32_t readSetBegin_virt(apache::thrift::protocol::TType& element_type,
                               uint32_t& size) override {
        const uint32_t consumed = TProtocolDecorator::readSetBegin_virt(element_type, size);
        reserve_container(size, decoded_thrift_value_reservation(element_type) + 4 * sizeof(void*));
        return consumed;
    }

private:
    void reserve_container(uint32_t count, size_t element_size) {
        if (count > std::numeric_limits<size_t>::max() / element_size) {
            throw apache::thrift::protocol::TProtocolException(
                    apache::thrift::protocol::TProtocolException::SIZE_LIMIT,
                    "Decoded Thrift container size overflows");
        }
        reserve_or_throw(static_cast<size_t>(count) * element_size,
                         /*restore_prior_on_failure=*/false);
    }

    void reserve_or_throw(size_t bytes, bool restore_prior_on_failure) {
        const Status status = _memory_manager->try_reserve(static_cast<int64_t>(bytes));
        if (status.ok()) {
            return;
        }
        if (restore_prior_on_failure) {
            _memory_manager->adopt_reserved_memory(std::move(_prior_reservation));
        }
        throw apache::thrift::protocol::TProtocolException(
                apache::thrift::protocol::TProtocolException::SIZE_LIMIT, status.to_string());
    }

    ThreadMemTrackerMgr* _memory_manager;
    ReservedMemoryToken _prior_reservation;
};

} // namespace

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
        std::shared_ptr<apache::thrift::transport::TMemoryBuffer> mem, bool compact,
        int32_t size_limit) {
    std::shared_ptr<apache::thrift::protocol::TProtocol> protocol;
    if (compact) {
        protocol = std::make_shared<apache::thrift::protocol::TCompactProtocolT<
                apache::thrift::transport::TMemoryBuffer>>(mem, size_limit, size_limit);
    } else {
        protocol = std::make_shared<apache::thrift::protocol::TBinaryProtocolT<
                apache::thrift::transport::TMemoryBuffer>>(mem, size_limit, size_limit,
                                                           /*strict_read=*/false,
                                                           /*strict_write=*/true);
    }
    return std::make_shared<MemoryBudgetProtocol>(std::move(protocol), size_limit);
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

bool _has_row_binlog(const TOlapTableSink& sink) {
    OlapTableSchemaParam schema;
    if (!schema.init(sink.schema).ok()) {
        return false;
    }
    for (const auto* index_schema : schema.indexes()) {
        if (index_schema->row_binlog_id > 0) {
            return true;
        }
    }
    return false;
}

} // namespace doris
