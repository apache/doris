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

#include "service/arrow_flight/flight_sql_service.h"

#include <absl/strings/str_split.h>
#include <arrow/status.h>
#include <openssl/crypto.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>

#include <charconv>
#include <cstdint>
#include <memory>
#include <string_view>
#include <system_error>
#include <vector>

#include "arrow/flight/sql/server.h"
#include "common/config.h"
#include "format/arrow/arrow_utils.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "service/arrow_flight/arrow_flight_batch_reader.h"
#include "service/arrow_flight/flight_sql_info.h"
#include "service/backend_options.h"
#include "util/time.h"
#include "util/uid_util.h"
#include "util/url_coding.h"

namespace doris::flight {

namespace {
constexpr int64_t kMaxLifetimeMs = 3600000;
constexpr int64_t kClockSkewMs = 30000;
constexpr std::string_view kKeyDomain = "doris/flight-ticket/key/v1";
// Include the terminating NUL in the authenticated prefix, matching the FE signer.
constexpr char kPayloadDomain[] = "doris/flight-ticket/payload/v1";

Status invalid_ticket() {
    // Tickets are credentials; never echo them in errors or logs.
    return Status::NotAuthorized("Invalid or expired Flight result ticket");
}

Status hmac_sha256(std::string_view key, std::string_view message, std::string* result) {
    unsigned char digest[SHA256_DIGEST_LENGTH];
    unsigned int length = 0;
    // An OpenSSL failure must not authenticate two empty MACs.
    if (HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
             reinterpret_cast<const unsigned char*>(message.data()), message.size(), digest,
             &length) == nullptr ||
        length != SHA256_DIGEST_LENGTH) {
        return Status::InternalError("Flight ticket HMAC failed");
    }
    result->assign(reinterpret_cast<const char*>(digest), length);
    return Status::OK();
}

Status verify_flight_ticket(const std::string& ticket, const std::vector<std::string>& fields,
                            const std::vector<std::string>& tokens, int32_t cluster_id,
                            int64_t now_ms) {
    // Keep the original four fields, followed by v1, cluster, issued, expires, base64 MAC.
    // A legacy four-field ticket must fail when verification is enabled.
    if (fields.size() != 9 || fields[4] != "v1") {
        return invalid_ticket();
    }
    auto parse_number = [](const std::string& text, auto* value) {
        auto parsed = std::from_chars(text.data(), text.data() + text.size(), *value);
        return parsed.ec == std::errc() && parsed.ptr == text.data() + text.size();
    };
    int32_t ticket_cluster_id = 0;
    int64_t issued = 0;
    int64_t expires = 0;
    // Order nonnegative timestamps before subtraction to avoid overflow on hostile inputs.
    if (!parse_number(fields[5], &ticket_cluster_id) || ticket_cluster_id != cluster_id ||
        !parse_number(fields[6], &issued) || !parse_number(fields[7], &expires) || now_ms < 0 ||
        issued < 0 || expires <= issued || expires - issued > kMaxLifetimeMs ||
        (issued > now_ms && issued - now_ms > kClockSkewMs) ||
        (now_ms > expires && now_ms - expires > kClockSkewMs)) {
        return invalid_ticket();
    }
    std::string mac;
    if (fields[8].size() != 44 || !base64_decode(fields[8], &mac) ||
        mac.size() != SHA256_DIGEST_LENGTH) {
        return invalid_ticket();
    }
    // Authenticate the exact original ticket and all metadata, excluding only the final MAC.
    std::string message(kPayloadDomain, sizeof(kPayloadDomain));
    message.append(ticket, 0, ticket.rfind('&'));
    // FE and BE may observe different heartbeat generations; try every retained token.
    for (const auto& token : tokens) {
        // Never derive a publicly computable key from an empty token.
        if (token.empty()) {
            continue;
        }
        std::string key;
        RETURN_IF_ERROR(hmac_sha256(token, kKeyDomain, &key));
        std::string expected;
        RETURN_IF_ERROR(hmac_sha256(key, message, &expected));
        if (CRYPTO_memcmp(expected.data(), mac.data(), expected.size()) == 0) {
            return Status::OK();
        }
    }
    return invalid_ticket();
}
} // namespace

class FlightSqlServer::Impl {
private:
    // Create a Ticket that combines a sql and a query ID.
    arrow::Result<arrow::flight::Ticket> encode_ticket(const std::string& sql,
                                                       const std::string& query_id) {
        std::string query = query_id;
        query += ':';
        query += sql;
        ARROW_ASSIGN_OR_RAISE(auto ticket, arrow::flight::sql::CreateStatementQueryTicket(query));
        return arrow::flight::Ticket {std::move(ticket)};
    }

    arrow::Result<std::shared_ptr<QueryStatement>> decode_ticket(const std::string& ticket) {
        std::vector<std::string> fields = absl::StrSplit(ticket, "&");
        // Accept both formats during rolling upgrades when verification is explicitly disabled.
        if (fields.size() != 4 && fields.size() != 9) {
            return arrow::Status::Invalid("Malformed Flight ticket");
        }
        if (config::enable_arrow_flight_ticket_signature_check) {
            // Require authentication even if the client removes the entire signature suffix.
            std::vector<std::string> tokens;
            int32_t cluster_id = -1;
            ExecEnv::GetInstance()->cluster_info()->get_auth_tokens(&tokens, &cluster_id);
            RETURN_ARROW_STATUS_IF_ERROR(
                    verify_flight_ticket(ticket, fields, tokens, cluster_id, UnixMillis()));
        }

        std::vector<std::string> str = absl::StrSplit(fields[0], "-");
        if (str.size() != 2) {
            return arrow::Status::Invalid("Malformed Flight ticket: missing query id");
        }

        TUniqueId queryid;
        from_hex(&queryid.hi, str[0]);
        from_hex(&queryid.lo, str[1]);
        TNetworkAddress result_addr;
        result_addr.hostname = fields[1];
        result_addr.port = std::stoi(fields[2]);
        const std::string& sql_base64 = fields[3];
        std::string sql;
        base64_decode(sql_base64, &sql);
        std::shared_ptr<QueryStatement> statement =
                std::make_shared<QueryStatement>(queryid, result_addr, sql);
        return statement;
    }

public:
    explicit Impl() = default;

    ~Impl() = default;

    arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> DoGetStatement(
            const arrow::flight::ServerCallContext& context,
            const arrow::flight::sql::StatementQueryTicket& command) {
        ARROW_ASSIGN_OR_RAISE(auto statement, decode_ticket(command.statement_handle));
        // if IP:BrpcPort in the Ticket is not current BE node,
        // pulls the query result Block from the BE node specified by IP:BrpcPort,
        // converts it to Arrow Batch and returns it to ADBC client.
        // use brpc to transmit blocks between BEs.
        if (statement->result_addr.hostname == BackendOptions::get_localhost() &&
            statement->result_addr.port == config::brpc_port) {
            std::shared_ptr<ArrowFlightBatchLocalReader> reader;
            ARROW_ASSIGN_OR_RAISE(
                    reader, ArrowFlightBatchLocalReader::Create(
                                    statement, [&context] { return context.is_cancelled(); }));
            return std::make_unique<arrow::flight::RecordBatchStream>(reader);
        } else {
            std::shared_ptr<ArrowFlightBatchRemoteReader> reader;
            ARROW_ASSIGN_OR_RAISE(
                    reader, ArrowFlightBatchRemoteReader::Create(
                                    statement, [&context] { return context.is_cancelled(); }));
            return std::make_unique<arrow::flight::RecordBatchStream>(reader);
        }
    }
};

FlightSqlServer::FlightSqlServer(std::shared_ptr<Impl> impl) : _impl(std::move(impl)) {}

arrow::Result<std::shared_ptr<FlightSqlServer>> FlightSqlServer::create() {
    std::shared_ptr<Impl> impl = std::make_shared<Impl>();

    std::shared_ptr<FlightSqlServer> result(new FlightSqlServer(std::move(impl)));
    for (const auto& id_to_result : GetSqlInfoResultMap()) {
        result->RegisterSqlInfo(id_to_result.first, id_to_result.second);
    }

    return result;
}

FlightSqlServer::~FlightSqlServer() {
    static_cast<void>(join());
}

arrow::Result<std::unique_ptr<arrow::flight::FlightDataStream>> FlightSqlServer::DoGetStatement(
        const arrow::flight::ServerCallContext& context,
        const arrow::flight::sql::StatementQueryTicket& command) {
    return _impl->DoGetStatement(context, command);
}

Status FlightSqlServer::init(int port) {
    if (port == -1) {
        LOG(INFO) << "Arrow Flight Service not start";
        return Status::OK();
    }
    _inited = true;
    arrow::flight::Location bind_location;
    RETURN_DORIS_STATUS_IF_ERROR(
            arrow::flight::Location::ForGrpcTcp(BackendOptions::get_service_bind_address(), port)
                    .Value(&bind_location));
    arrow::flight::FlightServerOptions flight_options(bind_location);

    // The FE authenticates SQL users and issues HMAC-protected result tickets.
    // enable_arrow_flight_ticket_signature_check requires verification before local
    // access or forwarding. Disable only for compatibility with legacy unsigned tickets.
    _header_middleware = std::make_shared<NoOpHeaderAuthServerMiddlewareFactory>();
    _bearer_middleware = std::make_shared<NoOpBearerAuthServerMiddlewareFactory>();
    flight_options.auth_handler = std::make_unique<arrow::flight::NoOpAuthHandler>();
    flight_options.middleware.push_back({"header-auth-server", _header_middleware});
    flight_options.middleware.push_back({"bearer-auth-server", _bearer_middleware});

    RETURN_DORIS_STATUS_IF_ERROR(Init(flight_options));
    LOG(INFO) << "Arrow Flight Service bind to host: " << BackendOptions::get_service_bind_address()
              << ", port: " << port;
    return Status::OK();
}

Status FlightSqlServer::join() {
    if (!_inited) {
        // Flight not inited, not need shutdown
        return Status::OK();
    }
    RETURN_DORIS_STATUS_IF_ERROR(Shutdown());
    return Status::OK();
}

} // namespace doris::flight
