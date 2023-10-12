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

#include <arrow/status.h>

#include "arrow/flight/server.h"
#include "arrow/flight/server_middleware.h"
#include "arrow/flight/types.h"

namespace doris {
namespace flight {

// Just return default bearer token.
class NoOpHeaderAuthServerMiddleware : public arrow::flight::ServerMiddleware {
public:
    void SendingHeaders(arrow::flight::AddCallHeaders* outgoing_headers) override;

    void CallCompleted(const arrow::Status& status) override {}

    [[nodiscard]] std::string name() const override { return "NoOpHeaderAuthServerMiddleware"; }
};

// Factory for base64 header authentication.
// No actual authentication.
class NoOpHeaderAuthServerMiddlewareFactory : public arrow::flight::ServerMiddlewareFactory {
public:
    NoOpHeaderAuthServerMiddlewareFactory() = default;

    arrow::Status StartCall(const arrow::flight::CallInfo& info,
                            const arrow::flight::ServerCallContext& context,
                            std::shared_ptr<arrow::flight::ServerMiddleware>* middleware) override;
};

// A server middleware for validating incoming bearer header authentication.
// Just compare with default bearer token.
class NoOpBearerAuthServerMiddleware : public arrow::flight::ServerMiddleware {
public:
    explicit NoOpBearerAuthServerMiddleware(const arrow::flight::CallHeaders& incoming_headers,
                                            bool* isValid)
            : _is_valid(isValid) {
        _incoming_headers = incoming_headers;
    }

    void SendingHeaders(arrow::flight::AddCallHeaders* outgoing_headers) override;

    void CallCompleted(const arrow::Status& status) override {}

    [[nodiscard]] std::string name() const override { return "NoOpBearerAuthServerMiddleware"; }

private:
    arrow::flight::CallHeaders _incoming_headers;
    bool* _is_valid;
};

// Factory for base64 header authentication.
// No actual authentication.
class NoOpBearerAuthServerMiddlewareFactory : public arrow::flight::ServerMiddlewareFactory {
public:
    NoOpBearerAuthServerMiddlewareFactory() : _is_valid(false) {}

    arrow::Status StartCall(const arrow::flight::CallInfo& info,
                            const arrow::flight::ServerCallContext& context,
                            std::shared_ptr<arrow::flight::ServerMiddleware>* middleware) override;

    [[nodiscard]] bool GetIsValid() const { return _is_valid; }

private:
    bool _is_valid;
};

} // namespace flight
} // namespace doris
