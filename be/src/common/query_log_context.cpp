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

#include "common/query_log_context.h"

#include <bthread/bthread.h>

#include <atomic>
#include <charconv>
#include <mutex>
#include <ostream>

#include "common/config.h"
#include "common/logging.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/types.pb.h"

namespace doris {
namespace {

bthread_key_t query_log_key;
std::atomic<bool> query_log_initialized {false};
thread_local QueryLogIdentity* pthread_query_log_identity = nullptr;

QueryLogIdentity* get_query_log_identity() {
    if (bthread_self() != 0) {
        return static_cast<QueryLogIdentity*>(bthread_getspecific(query_log_key));
    }
    return pthread_query_log_identity;
}

void set_query_log_identity(QueryLogIdentity* identity) {
    if (bthread_self() != 0) {
        CHECK_EQ(0, bthread_setspecific(query_log_key, identity));
    } else {
        pthread_query_log_identity = identity;
    }
}

// The caller provides space for 16 hex digits per half plus the separator.
char* format_log_id(char* buffer, uint64_t hi, uint64_t lo) {
    auto first = std::to_chars(buffer, buffer + 16, hi, 16);
    *first.ptr++ = '-';
    return std::to_chars(first.ptr, buffer + 33, lo, 16).ptr;
}

void write_log_id(std::ostream& stream, uint64_t hi, uint64_t lo) {
    char buffer[33];
    const auto* end = format_log_id(buffer, hi, lo);
    stream.write(buffer, static_cast<std::streamsize>(end - buffer));
}

} // namespace

QueryLogIdentity::QueryLogIdentity(const TUniqueId& query_id)
        : query_hi(static_cast<uint64_t>(query_id.hi)),
          query_lo(static_cast<uint64_t>(query_id.lo)) {}

QueryLogIdentity::QueryLogIdentity(const PUniqueId& query_id)
        : query_hi(static_cast<uint64_t>(query_id.hi())),
          query_lo(static_cast<uint64_t>(query_id.lo())) {}

QueryLogIdentity::QueryLogIdentity(const TUniqueId& query_id, const TUniqueId& instance_id)
        : QueryLogIdentity(query_id) {
    instance_hi = static_cast<uint64_t>(instance_id.hi);
    instance_lo = static_cast<uint64_t>(instance_id.lo);
}

QueryLogIdentity::QueryLogIdentity(const PUniqueId& query_id, const PUniqueId& instance_id)
        : QueryLogIdentity(query_id) {
    instance_hi = static_cast<uint64_t>(instance_id.hi());
    instance_lo = static_cast<uint64_t>(instance_id.lo());
}

void init_query_log_context() {
    if (!config::sys_log_enable_query_id) {
        return;
    }
    static std::once_flag once;
    std::call_once(once, [] {
        CHECK_EQ(0, bthread_key_create(&query_log_key, nullptr));
        query_log_initialized.store(true, std::memory_order_release);
    });
}

QueryLogIdentity current_query_log_identity() {
    if (!query_log_initialized.load(std::memory_order_acquire)) {
        return {};
    }
    auto* identity = get_query_log_identity();
    return identity == nullptr ? QueryLogIdentity {} : *identity;
}

std::string query_id_log_suffix(const TUniqueId& query_id) {
    const QueryLogIdentity query(query_id);
    if (query.query_hi == 0 && query.query_lo == 0) {
        return {};
    }
    const auto current = current_query_log_identity();
    if (config::sys_log_enable_query_id && FLAGS_log_prefix && current.query_hi == query.query_hi &&
        current.query_lo == query.query_lo) {
        return {};
    }
    char buffer[36] = {' ', '['};
    auto* end = format_log_id(buffer + 2, query.query_hi, query.query_lo);
    *end++ = ']';
    return {buffer, static_cast<size_t>(end - buffer)};
}

void ScopedQueryLogContext::reset(QueryLogIdentity identity) {
    if (!query_log_initialized.load(std::memory_order_acquire)) {
        return;
    }
    _identity = identity;
    if (!_installed) {
        _previous = get_query_log_identity();
        set_query_log_identity(&_identity);
        _installed = true;
    }
}

ScopedQueryLogContext::~ScopedQueryLogContext() {
    if (_installed) {
        set_query_log_identity(_previous);
    }
}

void append_query_log_identity(std::ostream& stream) {
    const auto identity = current_query_log_identity();
    const bool has_query = identity.query_hi != 0 || identity.query_lo != 0;
    const bool has_instance = identity.instance_hi != 0 || identity.instance_lo != 0;
    if (has_query) {
        stream << " [";
        write_log_id(stream, identity.query_hi, identity.query_lo);
    }
    if (has_instance) {
        if (has_query && identity.instance_hi == identity.query_hi) {
            // Instance IDs normally share the query's high half. Unsigned subtraction
            // preserves the low-half offset even when ID generation wraps around.
            char buffer[20]; // Maximum decimal length of a uint64_t.
            auto result = std::to_chars(buffer, buffer + sizeof(buffer),
                                        identity.instance_lo - identity.query_lo);
            stream.put('/');
            stream.write(buffer, static_cast<std::streamsize>(result.ptr - buffer));
        } else {
            // Load IDs can give an instance a different high half; retain the full ID.
            stream << (has_query ? " fragment_instance_id=" : " [fragment_instance_id=");
            write_log_id(stream, identity.instance_hi, identity.instance_lo);
        }
    }
    if (has_query || has_instance) {
        stream << ']';
    }
}

} // namespace doris
