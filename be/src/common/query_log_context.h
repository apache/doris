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

#include <cstdint>
#include <iosfwd>
#include <string>

namespace doris {

class TUniqueId;
class PUniqueId;

// A value snapshot. It never retains a query, a resource context, or an RPC request.
struct QueryLogIdentity {
    uint64_t query_hi = 0;
    uint64_t query_lo = 0;
    uint64_t instance_hi = 0;
    uint64_t instance_lo = 0;

    QueryLogIdentity() = default;
    explicit QueryLogIdentity(const TUniqueId& query_id);
    explicit QueryLogIdentity(const PUniqueId& query_id);
    QueryLogIdentity(const TUniqueId& query_id, const TUniqueId& instance_id);
    QueryLogIdentity(const PUniqueId& query_id, const PUniqueId& instance_id);
};

// Called during logging initialization, before worker threads start. The bthread key
// has process lifetime. Reading the identity never creates thread-local resources.
void init_query_log_context();
QueryLogIdentity current_query_log_identity();
void append_query_log_identity(std::ostream& stream);

// A message suffix for an ID not already present in the runtime log prefix. Keeps
// query attribution when prefix logging is disabled or the caller has no context.
// Do not use for audit/protocol fields or IDs describing relationships between queries.
std::string query_id_log_suffix(const TUniqueId& query_id);

// The installed pointer refers to this stack object and is restored before it dies.
// bthreads use their own key, so yielding or migrating cannot expose a pthread's ID.
class ScopedQueryLogContext {
public:
    ScopedQueryLogContext() = default;
    explicit ScopedQueryLogContext(QueryLogIdentity identity) { reset(identity); }
    ~ScopedQueryLogContext();

    ScopedQueryLogContext(const ScopedQueryLogContext&) = delete;
    ScopedQueryLogContext& operator=(const ScopedQueryLogContext&) = delete;
    ScopedQueryLogContext(ScopedQueryLogContext&&) = delete;
    ScopedQueryLogContext& operator=(ScopedQueryLogContext&&) = delete;

    // Replacing the current identity does not replace the saved outer scope.
    void reset(QueryLogIdentity identity);

private:
    QueryLogIdentity _identity;
    QueryLogIdentity* _previous = nullptr;
    bool _installed = false;
};

} // namespace doris
