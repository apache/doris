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

#include "format_v2/table/mongodb_reader.h"

#include <mongoc/mongoc.h>

#include <algorithm>
#include <chrono>
#include <iterator>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <utility>

#include "core/block/block.h"
#include "core/data_type_serde/bson_value_view.h"
#include "core/data_type_serde/data_type_serde.h"
#include "format_v2/column_mapper.h"
#include "format_v2/materialized_reader_util.h"
#include "runtime/descriptors.h"
#include "runtime/file_scan_profile.h"
#include "runtime/runtime_state.h"
#include "util/defer_op.h"

namespace doris::format::mongodb {
namespace {
using Clock = std::chrono::steady_clock;
template <auto Destroy>
struct MongoDeleter {
    template <typename T>
    void operator()(T* value) const {
        if (value) {
            Destroy(value);
        }
    }
};
template <typename T, auto Destroy>
using MongoPtr = std::unique_ptr<T, MongoDeleter<Destroy>>;
using UriPtr = MongoPtr<mongoc_uri_t, mongoc_uri_destroy>;
using CollectionPtr = MongoPtr<mongoc_collection_t, mongoc_collection_destroy>;
using CursorPtr = MongoPtr<mongoc_cursor_t, mongoc_cursor_destroy>;
using ReadPrefsPtr = MongoPtr<mongoc_read_prefs_t, mongoc_read_prefs_destroy>;
using ReadConcernPtr = MongoPtr<mongoc_read_concern_t, mongoc_read_concern_destroy>;
using SessionPtr = MongoPtr<mongoc_client_session_t, mongoc_client_session_destroy>;
using SessionOptionsPtr = MongoPtr<mongoc_session_opt_t, mongoc_session_opts_destroy>;
using ServerDescriptionPtr =
        MongoPtr<mongoc_server_description_t, mongoc_server_description_destroy>;

struct ClientPool {
    MongoPtr<mongoc_client_pool_t, mongoc_client_pool_destroy> pool;
};

// Share a pool within this BE; open() separately borrows an exclusive client from that pool.
// The registry holds weak references, so the last stream owner releases the pool and its clients.
Status acquire_pool(const TMongoScanDesc& d, std::shared_ptr<ClientPool>* result) {
    // Driver initialization is process-wide; cleanup while another scanner is alive is unsafe.
    static std::once_flag initialized;
    std::call_once(initialized, [] { mongoc_init(); });
    static std::mutex mutex;
    static std::unordered_map<std::string, std::weak_ptr<ClientPool>> pools;
    // Include exact credentials as well as the FE version: even a malformed descriptor cannot reuse
    // another identity's authenticated client. The key is private memory and is never logged.
    std::string key = std::to_string(d.catalog_id) + ":" + d.configuration_version;
    for (const auto* value : {&d.uri, &d.username, &d.password, &d.auth_database}) {
        key += ":" + std::to_string(value->size()) + ":" + *value;
    }
    const auto connect_timeout = std::min<int64_t>(d.connect_timeout_ms, d.timeout_ms);
    const auto selection_timeout = std::min<int64_t>(d.server_selection_timeout_ms, d.timeout_ms);
    // Snapshot planning consumes different amounts of each scan's deadline. Key the effective
    // transport caps, not the remaining lifetime, or every scan could bypass the shared pool bound.
    key += ":" + std::to_string(connect_timeout) + ":" + std::to_string(selection_timeout) + ":" +
           std::to_string(d.socket_timeout_ms) + ":" + std::to_string(d.pool_max_clients);
    {
        std::lock_guard lock(mutex);
        for (auto it = pools.begin(); it != pools.end();) {
            it = it->second.expired() ? pools.erase(it) : std::next(it);
        }
        if (auto found = pools.find(key); found != pools.end()) {
            if (auto pool = found->second.lock()) {
                *result = std::move(pool);
                return Status::OK();
            }
        }
    }
    bson_error_t error {};
    UriPtr uri(mongoc_uri_new_with_error(d.uri.c_str(), &error));
    if (!uri) {
        return Status::InvalidArgument("Invalid MongoDB connection configuration");
    }
    if (!d.username.empty() && (!mongoc_uri_set_username(uri.get(), d.username.c_str()) ||
                                !mongoc_uri_set_password(uri.get(), d.password.c_str()) ||
                                !mongoc_uri_set_auth_source(uri.get(), d.auth_database.c_str()))) {
        return Status::InvalidArgument("Invalid MongoDB authentication configuration");
    }
    mongoc_uri_set_option_as_int32(uri.get(), MONGOC_URI_MAXPOOLSIZE, d.pool_max_clients);
    mongoc_uri_set_option_as_int32(uri.get(), MONGOC_URI_CONNECTTIMEOUTMS,
                                   static_cast<int32_t>(connect_timeout));
    mongoc_uri_set_option_as_int32(uri.get(), MONGOC_URI_SERVERSELECTIONTIMEOUTMS,
                                   static_cast<int32_t>(selection_timeout));
    mongoc_uri_set_option_as_int32(uri.get(), MONGOC_URI_SOCKETTIMEOUTMS, d.socket_timeout_ms);
    mongoc_uri_set_option_as_bool(uri.get(), MONGOC_URI_RETRYREADS, false);
    auto pool = std::make_shared<ClientPool>();
    pool->pool.reset(mongoc_client_pool_new_with_error(uri.get(), &error));
    if (!pool->pool) {
        return Status::InvalidArgument("MongoDB client pool initialization failed");
    }
    mongoc_client_pool_set_error_api(pool->pool.get(), 2);
    {
        std::lock_guard lock(mutex);
        auto& existing = pools[key];
        if (auto shared = existing.lock()) {
            *result = std::move(shared);
        } else {
            existing = pool;
            *result = std::move(pool);
        }
    }
    return Status::OK();
}

Status validate_pushdown_bson(const TMongoScanDesc& d) {
    for (const auto* bytes : {d.__isset.filter_bson ? &d.filter_bson : nullptr,
                              d.__isset.projection_bson ? &d.projection_bson : nullptr}) {
        if (!bytes) {
            continue;
        }
        bson_t view;
        // A truncated/oversized RPC payload must fail before libmongoc reads it or acquires a lease.
        if (bytes->size() > 1024 * 1024 ||
            !bson_init_static(&view, reinterpret_cast<const uint8_t*>(bytes->data()),
                              bytes->size()) ||
            !bson_validate(&view, BSON_VALIDATE_NONE, nullptr)) {
            return Status::InvalidArgument("Invalid MongoDB pushdown BSON descriptor");
        }
    }
    // A stale or malformed FE plan must not bypass the strict-error or view-collation contracts.
    if (d.__isset.filter_bson && (!d.type_mismatch_as_null || d.object_kind == "view")) {
        return Status::InvalidArgument(
                "MongoDB predicate pushdown is unavailable for strict scans or views");
    }
    return Status::OK();
}

Status mongo_error(const bson_error_t& error) {
    // Driver messages can contain URIs, credentials, commands, or source values.
    return Status::IOError("MongoDB command failed (domain={}, code={})", error.domain, error.code);
}

Status validate_snapshot_descriptor(const TMongoScanDesc& d) {
    if (d.__isset.snapshot != (d.read_concern == "snapshot")) {
        return Status::InvalidArgument(
                "MongoDB snapshot reads require exactly one shared snapshot pin");
    }
    if (!d.__isset.snapshot) {
        return Status::OK();
    }
    const auto& snapshot = d.snapshot;
    if (!snapshot.__isset.timestamp_seconds || !snapshot.__isset.timestamp_increment ||
        !snapshot.__isset.source_kind || !snapshot.__isset.source_id ||
        !snapshot.__isset.topology_version || !snapshot.__isset.max_wire_version ||
        snapshot.timestamp_seconds <= 0 ||
        std::cmp_greater(snapshot.timestamp_seconds, UINT32_MAX) ||
        snapshot.timestamp_increment < 0 ||
        std::cmp_greater(snapshot.timestamp_increment, UINT32_MAX) ||
        (snapshot.source_kind != "replica_set" && snapshot.source_kind != "mongos") ||
        snapshot.source_id.size() != 24 ||
        !std::ranges::all_of(
                snapshot.source_id,
                [](char c) { return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'); }) ||
        (snapshot.max_wire_version != 21 && snapshot.max_wire_version != 25) ||
        (snapshot.source_kind == "replica_set" ? snapshot.topology_version <= 0
                                               : snapshot.topology_version != 0) ||
        d.object_kind == "view" || (d.object_kind == "collection" && d.collection_uuid.empty())) {
        return Status::InvalidArgument("Invalid or unsupported MongoDB shared snapshot descriptor");
    }
    if (d.object_kind == "timeseries") {
        if (!snapshot.__isset.timeseries_options_json || snapshot.timeseries_options_json.empty() ||
            snapshot.timeseries_options_json.size() > 16384) {
            return Status::InvalidArgument(
                    "MongoDB snapshot requires bounded time-series definition metadata");
        }
        auto* definition = bson_new_from_json(
                reinterpret_cast<const uint8_t*>(snapshot.timeseries_options_json.data()),
                snapshot.timeseries_options_json.size(), nullptr);
        if (!definition) {
            return Status::InvalidArgument("Invalid MongoDB time-series snapshot definition");
        }
        const auto view = BsonValueView::document(*definition).at_path({"timeseries"});
        const bool valid = !view.missing && view.value.value_type == BSON_TYPE_DOCUMENT;
        bson_destroy(definition);
        if (!valid) {
            return Status::InvalidArgument("MongoDB snapshot requires public time-series options");
        }
    } else if (snapshot.__isset.timeseries_options_json) {
        return Status::InvalidArgument(
                "MongoDB collection snapshot cannot contain time-series options");
    }
    return Status::OK();
}

// Own the native I/O lifetime for one scan. next() exposes a borrowed document from libmongoc's
// current batch; MongoFileReader copies its values before another next() or close() can invalidate it.
class NativeMongoStream final : public MongoCursorStream {
public:
    ~NativeMongoStream() override { close(); }
    Status open(const TMongoScanDesc& d, std::function<bool()> cancelled) override {
        _cancelled = std::move(cancelled);
        _deadline = Clock::now() + std::chrono::milliseconds(d.timeout_ms);
        RETURN_IF_ERROR(check_deadline());
        RETURN_IF_ERROR(validate_pushdown_bson(d));
        RETURN_IF_ERROR(validate_snapshot_descriptor(d));
        RETURN_IF_ERROR(acquire_pool(d, &_pool));
        while (!(_client = mongoc_client_pool_try_pop(_pool->pool.get()))) {
            RETURN_IF_ERROR(check_deadline());
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        mongoc_client_set_sockettimeoutms(_client, static_cast<int32_t>(std::min<int64_t>(
                                                           d.socket_timeout_ms, remaining_ms())));
        if (d.__isset.snapshot) {
            RETURN_IF_ERROR(verify_snapshot_source(d.snapshot));
            _identity.__set_database(d.database);
            _identity.__set_collection(d.collection);
            _identity.__set_collection_uuid(d.collection_uuid);
            _identity.__set_object_kind(d.object_kind);
            _identity.__set_snapshot(d.snapshot);
        }
        RETURN_IF_ERROR(verify_collection_identity(d));
        RETURN_IF_ERROR(check_deadline());
        _collection.reset(
                mongoc_client_get_collection(_client, d.database.c_str(), d.collection.c_str()));
        if (!_collection) {
            return Status::InvalidArgument("Invalid MongoDB namespace");
        }
        // MongoDB enforces visibility for each read. Ordinary levels carry only a policy name;
        // snapshot reads additionally need the common atClusterTime encoded below.
        ReadConcernPtr concern(mongoc_read_concern_new());
        if (!mongoc_read_concern_set_level(concern.get(), d.read_concern.c_str())) {
            return Status::InvalidArgument("Invalid MongoDB read concern");
        }
        mongoc_collection_set_read_concern(_collection.get(), concern.get());
        RETURN_IF_ERROR(configure_mongodb_read_preference(_collection.get(), d.read_preference));
        // Typed source candidates; Doris still evaluates residuals.
        bson_t filter = BSON_INITIALIZER;
        // Projection, collation, read concern and execution limits.
        bson_t options = BSON_INITIALIZER;
        Defer release_bson([&] {
            bson_destroy(&options);
            bson_destroy(&filter);
        });
        RETURN_IF_ERROR(build_mongodb_find_options(d, &filter, &options));
        // Requested document count per wire batch; MongoDB's byte cap can return fewer documents.
        BSON_APPEND_INT32(&options, "batchSize", d.batch_size);
        // Server execution budget for find; socket timeouts and the local deadline also bound waits.
        BSON_APPEND_INT64(&options, "maxTimeMS", remaining_ms());
        if (d.__isset.snapshot) {
            RETURN_IF_ERROR(open_snapshot_cursor(d, filter, options));
            return check_deadline();
        }
        // Always query the public logical namespace: a view may change rows, names, and _id,
        // and time-series buckets are an internal storage representation.
        _cursor.reset(
                mongoc_collection_find_with_opts(_collection.get(), &filter, &options, nullptr));
        if (!_cursor) {
            return Status::IOError("MongoDB cursor initialization failed");
        }
        return check_deadline();
    }
    Status next(const bson_t** document, bool* eof) override {
        RETURN_IF_ERROR(check_deadline());
        if (mongoc_cursor_next(_cursor.get(), document)) {
            *eof = false;
            return check_deadline();
        }
        bson_error_t error {};
        if (mongoc_cursor_error(_cursor.get(), &error)) {
            return mongo_error(error);
        }
        if (_identity.__isset.snapshot) {
            RETURN_IF_ERROR(verify_snapshot_source(_identity.snapshot));
            RETURN_IF_ERROR(verify_collection_identity(_identity));
        }
        *eof = true;
        return check_deadline();
    }
    void close() override {
        // Destroy the cursor (killCursors) before releasing its collection and exclusive pool lease.
        _cursor.reset();
        _collection.reset();
        _session.reset();
        _identity = TMongoScanDesc();
        if (_client) {
            mongoc_client_pool_push(_pool->pool.get(), _client);
            _client = nullptr;
        }
        _pool.reset();
    }

private:
    Status open_snapshot_cursor(const TMongoScanDesc& d, const bson_t& filter,
                                const bson_t& options) {
        SessionOptionsPtr session_options(mongoc_session_opts_new());
        // Each stream uses its own session. Causal afterClusterTime must not replace the fixed T.
        mongoc_session_opts_set_causal_consistency(session_options.get(), false);
        bson_error_t error {};
        _session.reset(mongoc_client_start_session(_client, session_options.get(), &error));
        if (!_session) {
            return mongo_error(error);
        }
        const auto* preferences = mongoc_collection_get_read_prefs(_collection.get());
        ServerDescriptionPtr server(
                mongoc_client_select_server(_client, false, preferences, &error));
        if (!server) {
            return mongo_error(error);
        }
        // MongoDB find body sent to d.database.
        bson_t command = BSON_INITIALIZER;
        // libmongoc routing/session context for this cursor.
        bson_t cursor_options = BSON_INITIALIZER;
        Defer release([&] {
            bson_destroy(&command);
            bson_destroy(&cursor_options);
        });
        // find names the public logical namespace; filter preserves BSON numeric/date tags.
        BSON_APPEND_UTF8(&command, "find", d.collection.c_str());
        BSON_APPEND_DOCUMENT(&command, "filter", &filter);
        // Add projection/collation, the shared readConcern, batchSize and maxTimeMS to the wire body.
        if (!bson_concat(&command, &options)) {
            return Status::InvalidArgument("MongoDB snapshot command exceeds BSON bounds");
        }
        // serverId is a driver-local topology handle, not the cluster identity from TMongoSnapshot.
        // Keeping it binds getMore/killCursors to the server/router that created the cursor.
        BSON_APPEND_INT32(&cursor_options, "serverId", mongoc_server_description_id(server.get()));
        // Append the driver-local sessionId; libmongoc emits its wire lsid and retains it for getMore.
        if (!mongoc_client_session_append(_session.get(), &cursor_options, &error)) {
            return mongo_error(error);
        }
        RETURN_IF_ERROR(check_deadline());
        // find_with_opts normalizes readConcern to its level and drops atClusterTime. Send the
        // explicit command, then retain its selected server/session for getMore and killCursors.
        bson_t reply;
        if (!mongoc_client_read_command_with_opts(_client, d.database.c_str(), &command,
                                                  preferences, &cursor_options, &reply, &error)) {
            bson_destroy(&reply);
            return mongo_error(error);
        }
        // The cursor factory needs this option to size subsequent getMore requests as well.
        BSON_APPEND_INT32(&cursor_options, "batchSize", d.batch_size);
        // The cursor factory consumes and destroys reply. Stack storage avoids leaking a bson_new
        // wrapper when the command helper initializes its overwritable reply storage as static.
        _cursor.reset(
                mongoc_cursor_new_from_command_reply_with_opts(_client, &reply, &cursor_options));
        return Status::OK();
    }

    Status verify_snapshot_source(const TMongoSnapshot& snapshot) {
        bson_t hello_command = BSON_INITIALIZER;
        bson_t hello_reply = BSON_INITIALIZER;
        bson_t identity = BSON_INITIALIZER;
        bson_t identity_command = BSON_INITIALIZER;
        Defer release([&] {
            bson_destroy(&hello_command);
            bson_destroy(&hello_reply);
            bson_destroy(&identity);
            bson_destroy(&identity_command);
        });
        ReadPrefsPtr primary(mongoc_read_prefs_new(MONGOC_READ_PRIMARY));
        BSON_APPEND_INT32(&hello_command, "hello", 1);
        bson_error_t error {};
        if (!mongoc_client_command_simple(_client, "admin", &hello_command, primary.get(),
                                          &hello_reply, &error)) {
            return mongo_error(error);
        }
        RETURN_IF_ERROR(check_deadline());
        if (snapshot.source_kind == "replica_set") {
            BSON_APPEND_INT32(&identity_command, "replSetGetConfig", 1);
            if (!mongoc_client_command_simple(_client, "admin", &identity_command, primary.get(),
                                              &identity, &error)) {
                return mongo_error(error);
            }
        } else {
            CollectionPtr version(mongoc_client_get_collection(_client, "config", "version"));
            ReadConcernPtr metadata(mongoc_read_concern_new());
            mongoc_collection_set_read_concern(version.get(), metadata.get());
            bson_t filter = BSON_INITIALIZER;
            bson_t options = BSON_INITIALIZER;
            Defer release_options([&] {
                bson_destroy(&filter);
                bson_destroy(&options);
            });
            BSON_APPEND_INT32(&filter, "_id", 1);
            BSON_APPEND_INT32(&options, "limit", 1);
            BSON_APPEND_INT32(&options, "batchSize", 1);
            BSON_APPEND_INT64(&options, "maxTimeMS", remaining_ms());
            CursorPtr cursor(mongoc_collection_find_with_opts(version.get(), &filter, &options,
                                                              primary.get()));
            const bson_t* document = nullptr;
            if (mongoc_cursor_next(cursor.get(), &document)) {
                bson_copy_to(document, &identity);
            }
            if (mongoc_cursor_error(cursor.get(), &error)) {
                return mongo_error(error);
            }
        }
        RETURN_IF_ERROR(check_deadline());
        return validate_mongodb_snapshot_source(snapshot, hello_reply, identity);
    }

    Status verify_collection_identity(const TMongoScanDesc& d) {
        const bool timeseries_snapshot = d.__isset.snapshot && d.object_kind == "timeseries";
        if (d.collection_uuid.empty() && !timeseries_snapshot) {
            return Status::OK();
        }
        MongoPtr<mongoc_database_t, mongoc_database_destroy> database(
                mongoc_client_get_database(_client, d.database.c_str()));
        ReadPrefsPtr primary(mongoc_read_prefs_new(MONGOC_READ_PRIMARY));
        mongoc_database_set_read_prefs(database.get(), primary.get());
        ReadConcernPtr metadata_concern(mongoc_read_concern_new());
        mongoc_database_set_read_concern(database.get(), metadata_concern.get());
        bson_t options = BSON_INITIALIZER;
        BSON_APPEND_INT64(&options, "maxTimeMS", remaining_ms());
        bson_t filter;
        BSON_APPEND_DOCUMENT_BEGIN(&options, "filter", &filter);
        BSON_APPEND_UTF8(&filter, "name", d.collection.c_str());
        bson_append_document_end(&options, &filter);
        BSON_APPEND_INT32(&options, "batchSize", 1);
        CursorPtr cursor(mongoc_database_find_collections_with_opts(database.get(), &options));
        bson_destroy(&options);
        const bson_t* document = nullptr;
        std::string actual;
        bool definition_matches = !timeseries_snapshot;
        if (cursor && mongoc_cursor_next(cursor.get(), &document)) {
            if (timeseries_snapshot) {
                auto options_view = BsonValueView::document(*document).at_path({"options"});
                auto kind = BsonValueView::document(*document).at_path({"type"});
                bson_t actual_options;
                auto* expected = bson_new_from_json(
                        reinterpret_cast<const uint8_t*>(d.snapshot.timeseries_options_json.data()),
                        d.snapshot.timeseries_options_json.size(), nullptr);
                if (!options_view.missing && options_view.value.value_type == BSON_TYPE_DOCUMENT &&
                    !kind.missing && kind.value.value_type == BSON_TYPE_UTF8 &&
                    std::string_view(kind.value.value.v_utf8.str, kind.value.value.v_utf8.len) ==
                            "timeseries") {
                    bson_init_static(&actual_options, options_view.value.value.v_doc.data,
                                     options_view.value.value.v_doc.data_len);
                    definition_matches = bson_equal(&actual_options, expected);
                }
                bson_destroy(expected);
            }
            auto identity = BsonValueView::document(*document).at_path({"info", "uuid"});
            if (!identity.missing && identity.value.value_type == BSON_TYPE_BINARY &&
                identity.value.value.v_binary.subtype == BSON_SUBTYPE_UUID) {
                const auto& bytes = identity.value.value.v_binary;
                static constexpr char digits[] = "0123456789abcdef";
                for (uint32_t i = 0; i < bytes.data_len; ++i) {
                    actual += digits[bytes.data[i] >> 4];
                    actual += digits[bytes.data[i] & 15];
                }
            }
        }
        bson_error_t error {};
        if (!cursor || mongoc_cursor_error(cursor.get(), &error)) {
            return mongo_error(error);
        }
        // Namespace reuse after DROP/CREATE must fail instead of scanning an unrelated collection.
        if (actual != d.collection_uuid || !definition_matches) {
            return Status::InvalidArgument(
                    "MongoDB collection identity changed; refresh the catalog and retry");
        }
        return check_deadline();
    }
    int64_t remaining_ms() const {
        return std::max<int64_t>(
                1, std::chrono::duration_cast<std::chrono::milliseconds>(_deadline - Clock::now())
                           .count());
    }
    Status check_deadline() const {
        if (_cancelled && _cancelled()) {
            return Status::Cancelled("MongoDB scan cancelled");
        }
        if (Clock::now() >= _deadline) {
            return Status::TimedOut("MongoDB scan deadline exceeded");
        }
        return Status::OK();
    }
    SessionPtr _session;
    TMongoScanDesc _identity;
    std::function<bool()> _cancelled;
    Clock::time_point _deadline;
    std::shared_ptr<ClientPool> _pool;
    mongoc_client_t* _client = nullptr;
    CollectionPtr _collection;
    CursorPtr _cursor;
};
} // namespace

std::unique_ptr<MongoCursorStream> make_native_mongo_stream() {
    return std::make_unique<NativeMongoStream>();
}

Status validate_mongodb_snapshot_source(const TMongoSnapshot& snapshot, const bson_t& hello,
                                        const bson_t& identity) {
    const auto hello_view = BsonValueView::document(hello);
    const auto number = [](const BsonValueView& value) -> std::optional<int64_t> {
        if (!value.missing && value.value.value_type == BSON_TYPE_INT32) {
            return value.value.value.v_int32;
        }
        if (!value.missing && value.value.value_type == BSON_TYPE_INT64) {
            return value.value.value.v_int64;
        }
        return std::nullopt;
    };
    const auto wire = number(hello_view.at_path({"maxWireVersion"}));
    const auto message = hello_view.at_path({"msg"});
    const bool router = !message.missing && message.value.value_type == BSON_TYPE_UTF8 &&
                        std::string_view(message.value.value.v_utf8.str,
                                         message.value.value.v_utf8.len) == "isdbgrid";
    const auto set_name = hello_view.at_path({"setName"});
    if (!wire || *wire != snapshot.max_wire_version ||
        router != (snapshot.source_kind == "mongos") ||
        (!router && (set_name.missing || set_name.value.value_type != BSON_TYPE_UTF8))) {
        return Status::InvalidArgument(
                "MongoDB snapshot source kind or qualified server version changed");
    }
    const auto identity_view = BsonValueView::document(identity);
    const auto id = router ? identity_view.at_path({"clusterId"})
                           : identity_view.at_path({"config", "settings", "replicaSetId"});
    if (id.missing || id.value.value_type != BSON_TYPE_OID) {
        return Status::InvalidArgument("MongoDB snapshot source identity metadata is unavailable");
    }
    char encoded[25];
    bson_oid_to_string(&id.value.value.v_oid, encoded);
    if (snapshot.source_id != encoded) {
        return Status::InvalidArgument("MongoDB snapshot source identity changed");
    }
    if (!router) {
        const auto version = number(identity_view.at_path({"config", "version"}));
        if (!version || *version != snapshot.topology_version) {
            return Status::InvalidArgument("MongoDB snapshot source topology changed");
        }
    }
    return Status::OK();
}

Status configure_mongodb_read_preference(_mongoc_collection_t* collection,
                                         const std::string& preference) {
    mongoc_read_mode_t mode = MONGOC_READ_PRIMARY;
    if (preference == "primaryPreferred") {
        mode = MONGOC_READ_PRIMARY_PREFERRED;
    } else if (preference == "secondary") {
        mode = MONGOC_READ_SECONDARY;
    } else if (preference == "secondaryPreferred") {
        mode = MONGOC_READ_SECONDARY_PREFERRED;
    } else if (preference == "nearest") {
        mode = MONGOC_READ_NEAREST;
    } else if (preference != "primary") {
        return Status::InvalidArgument("Invalid MongoDB read preference");
    }
    // The collection inherited the complete URI preference. A new mode-only preference loses
    // tag sets and max staleness; copy it before applying FE's effective mode instead.
    ReadPrefsPtr prefs(mongoc_read_prefs_copy(mongoc_collection_get_read_prefs(collection)));
    mongoc_read_prefs_set_mode(prefs.get(), mode);
    // For example, a malformed descriptor must not apply primary together with secondary tags.
    if (!mongoc_read_prefs_is_valid(prefs.get())) {
        return Status::InvalidArgument("Incompatible MongoDB read preference criteria");
    }
    mongoc_collection_set_read_prefs(collection, prefs.get());
    return Status::OK();
}

Status build_mongodb_find_options(const TMongoScanDesc& d, bson_t* filter, bson_t* options) {
    RETURN_IF_ERROR(validate_pushdown_bson(d));
    RETURN_IF_ERROR(validate_snapshot_descriptor(d));
    if (d.__isset.snapshot) {
        bson_t concern;
        // level selects MongoDB snapshot visibility; atClusterTime makes every scan use the FE's T.
        // Timestamp(seconds, increment) is an ordering token, not a millisecond Date value.
        BSON_APPEND_DOCUMENT_BEGIN(options, "readConcern", &concern);
        BSON_APPEND_UTF8(&concern, "level", "snapshot");
        BSON_APPEND_TIMESTAMP(&concern, "atClusterTime",
                              static_cast<uint32_t>(d.snapshot.timestamp_seconds),
                              static_cast<uint32_t>(d.snapshot.timestamp_increment));
        bson_append_document_end(options, &concern);
    }
    if (d.__isset.filter_bson) {
        bson_t view;
        bson_init_static(&view, reinterpret_cast<const uint8_t*>(d.filter_bson.data()),
                         d.filter_bson.size());
        if (!bson_concat(filter, &view)) {
            return Status::InvalidArgument("MongoDB candidate filter exceeds BSON limits");
        }
        // Views cannot override their own collation and are rejected above. For example, the
        // ordinary collection's case-insensitive default must not change SQL text = 'alpha'.
        bson_t collation;
        BSON_APPEND_DOCUMENT_BEGIN(options, "collation", &collation);
        BSON_APPEND_UTF8(&collation, "locale", "simple");
        bson_append_document_end(options, &collation);
    }
    if (d.__isset.projection_bson) {
        bson_t view;
        bson_init_static(&view, reinterpret_cast<const uint8_t*>(d.projection_bson.data()),
                         d.projection_bson.size());
        if (!BSON_APPEND_DOCUMENT(options, "projection", &view)) {
            return Status::InvalidArgument("MongoDB projection exceeds BSON limits");
        }
    }
    return Status::OK();
}

Status validate_mongodb_range(const TFileRangeDesc& range) {
    if (!range.__isset.table_format_params ||
        range.table_format_params.table_format_type != "mongodb" ||
        !range.table_format_params.__isset.mongodb_params) {
        return Status::InvalidArgument("MongoDB native reader requires mongodb_params");
    }
    const auto& d = range.table_format_params.mongodb_params;
    if (!d.__isset.version || d.version != 1 || d.database.empty() || d.collection.empty() ||
        d.collection.starts_with("system.") || d.database.find('\0') != std::string::npos ||
        d.collection.find('\0') != std::string::npos || d.uri.empty() || d.schema_version.empty() ||
        d.configuration_version.empty() || d.bindings_json.empty() ||
        (d.object_kind != "collection" && d.object_kind != "view" &&
         d.object_kind != "timeseries") ||
        d.batch_size < 1 || d.batch_size > 65536 || d.timeout_ms < 1 || d.connect_timeout_ms < 1 ||
        d.connect_timeout_ms > 300000 || d.server_selection_timeout_ms < 1 ||
        d.server_selection_timeout_ms > 300000 || d.socket_timeout_ms < 1 ||
        d.socket_timeout_ms > 300000 || d.pool_max_clients < 1 || d.pool_max_clients > 1024 ||
        (d.read_concern != "local" && d.read_concern != "majority" &&
         d.read_concern != "available" && d.read_concern != "snapshot")) {
        return Status::InvalidArgument("Invalid or unsupported MongoDB native scan descriptor");
    }
    RETURN_IF_ERROR(validate_snapshot_descriptor(d));
    return validate_pushdown_bson(d);
}

MongoFileReader::MongoFileReader(std::shared_ptr<io::FileSystemProperties>& properties,
                                 std::unique_ptr<io::FileDescription>& description,
                                 std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
                                 TFileRangeDesc range, std::vector<SlotDescriptor*> slots,
                                 MongoStreamFactory factory)
        : FileReader(properties, description, std::move(io_ctx), profile),
          _range(std::move(range)),
          _slots(std::move(slots)),
          _factory(std::move(factory)) {}
MongoFileReader::~MongoFileReader() {
    (void)close();
}
void MongoFileReader::_init_profile() {
    if (!_profile) {
        return;
    }
    _io_time = file_scan_profile::ensure_hierarchy(_profile).io;
    const char* parent = "MongoFileReader";
    _total_time = ADD_CHILD_TIMER_WITH_LEVEL(_profile, parent, file_scan_profile::FILE_READER, 1);
    _read_time = ADD_CHILD_TIMER_WITH_LEVEL(_profile, "MongoReadTime", parent, 1);
    _decode_time = ADD_CHILD_TIMER_WITH_LEVEL(_profile, "MongoDecodeTime", parent, 1);
    _filter_time = ADD_CHILD_TIMER_WITH_LEVEL(_profile, "MongoFilterTime", parent, 1);
    _rows_read = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "MongoRowsRead", TUnit::UNIT, parent, 1);
    _bytes_read = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "MongoBsonBytes", TUnit::BYTES, parent, 1);
}
Status MongoFileReader::init(RuntimeState* state) {
    _state = state;
    RETURN_IF_ERROR(validate_mongodb_range(_range));
    RETURN_IF_ERROR(parse_bson_bindings(_range.table_format_params.mongodb_params.bindings_json,
                                        &_bindings));
    _batch_size = state->batch_size();
    _init_profile();
    if (_profile) {
        const auto& descriptor = _range.table_format_params.mongodb_params;
        _profile->add_info_string("MongoReadConcern", descriptor.read_concern);
        if (descriptor.__isset.snapshot) {
            _profile->add_info_string("MongoSnapshotTimestamp",
                                      fmt::format("{}:{}", descriptor.snapshot.timestamp_seconds,
                                                  descriptor.snapshot.timestamp_increment));
        }
    }
    _eof = false;
    return Status::OK();
}
Status MongoFileReader::get_schema(std::vector<ColumnDefinition>* schema) const {
    schema->clear();
    for (size_t i = 0; i < _slots.size(); ++i) {
        const auto* slot = _slots[i];
        schema->push_back({.identifier = Field::create_field<TYPE_INT>(cast_set<int32_t>(i)),
                           .local_id = cast_set<int32_t>(i),
                           .name = slot->col_name(),
                           .type = slot->type(),
                           .children = materialized_schema_children(slot->type())});
    }
    return Status::OK();
}
std::unique_ptr<TableColumnMapper> MongoFileReader::create_column_mapper(
        TableColumnMapperOptions options) const {
    return std::make_unique<MaterializedColumnMapper>(options);
}
void MongoFileReader::set_batch_size(size_t size) {
    DORIS_CHECK(size > 0);
    _batch_size = size;
}
Status MongoFileReader::open(std::shared_ptr<FileScanRequest> request) {
    SCOPED_TIMER(_total_time);
    if (_stream) {
        return Status::InvalidArgument("MongoDB reader already has an open cursor");
    }
    _read_bindings.clear();
    _positions.clear();
    _serdes.clear();
    _eof = false;
    RETURN_IF_ERROR(FileReader::open(std::move(request)));
    // Request iteration order need not match slot order. Keep binding/SerDe entries aligned with
    // their file-local block positions, including reordered or pruned projections.
    for (const auto& [id, position] : _request->local_positions) {
        if (id.value() < 0 || static_cast<size_t>(id.value()) >= _slots.size()) {
            return Status::InvalidArgument("Invalid MongoDB projected slot");
        }
        const auto* slot = _slots[id.value()];
        auto binding = std::ranges::find_if(
                _bindings, [&](const auto& b) { return b.name == slot->col_name(); });
        if (binding == _bindings.end()) {
            return Status::InvalidArgument(
                    "MongoDB projected column differs from the pinned schema");
        }
        _read_bindings.push_back(&*binding);
        _positions.push_back(position.value());
        _serdes.push_back(slot->type()->get_serde());
    }
    _stream = _factory ? _factory() : make_native_mongo_stream();
    SCOPED_TIMER(_io_time);
    SCOPED_TIMER(_read_time);
    Status status = _stream->open(_range.table_format_params.mongodb_params, [this] {
        return _state->is_cancelled() || (_io_ctx && _io_ctx->should_stop);
    });
    if (!status.ok()) {
        (void)close();
    }
    return status;
}
Status MongoFileReader::get_block(Block* block, size_t* rows, bool* eof) {
    SCOPED_TIMER(_total_time);
    *rows = 0;
    *eof = _eof;
    if (_eof) {
        return Status::OK();
    }
    Status status = _read_block(block, rows, eof);
    if (!status.ok() || *eof) {
        (void)close();
    }
    return status;
}
Status MongoFileReader::_read_block(Block* block, size_t* rows, bool* eof) {
    {
        auto guard = block->mutate_columns_scoped();
        auto& columns = guard.mutable_columns();
        std::vector<size_t> sizes(_positions.size());
        size_t bson_bytes = 0;
        while (*rows < _batch_size) {
            RETURN_IF_CANCELLED(_state);
            if (_io_ctx && _io_ctx->should_stop) {
                *eof = true;
                break;
            }
            const bson_t* document = nullptr;
            bool source_eof = false;
            {
                SCOPED_TIMER(_io_time);
                SCOPED_TIMER(_read_time);
                RETURN_IF_ERROR(_stream->next(&document, &source_eof));
            }
            if (source_eof) {
                *eof = true;
                break;
            }
            if (!document || document->len > BsonDecodeContext::MAX_DOCUMENT_BYTES ||
                !bson_validate(document, BSON_VALIDATE_NONE, nullptr)) {
                return Status::Corruption("MongoDB cursor returned invalid BSON");
            }
            SCOPED_TIMER(_decode_time);
            BsonValueView root = BsonValueView::document(*document);
            // Validate every returned subtree before conversion. Resource/structural failures
            // must not be swallowed by the per-field mismatch-as-NULL policy.
            RETURN_IF_ERROR(validate_bson_tree(root));
            // A later field can fail after earlier columns appended. Snapshot every column before
            // the first append so the failing source document contributes no partial Doris row.
            for (size_t i = 0; i < _positions.size(); ++i) {
                sizes[i] = columns[_positions[i]]->size();
            }
            for (size_t i = 0; i < _positions.size(); ++i) {
                const auto* binding = _read_bindings[i];
                BsonValueView value = binding->special.empty() ? root.at_path(binding->path) : root;
                BsonDecodeContext context {
                        .binding = binding,
                        .mismatch_as_null =
                                _range.table_format_params.mongodb_params.type_mismatch_as_null};
                Status status = _serdes[i]->read_one_cell_from_bson(*columns[_positions[i]], value,
                                                                    context);
                if (!status.ok()) {
                    for (size_t j = 0; j < _positions.size(); ++j) {
                        auto& column = *columns[_positions[j]];
                        column.pop_back(column.size() - sizes[j]);
                    }
                    return status;
                }
            }
            // All SerDes have copied borrowed payloads; the next stream advance may release them.
            ++*rows;
            if (_bytes_read) {
                COUNTER_UPDATE(_bytes_read, document->len);
            }
            bson_bytes += document->len;
            // A row-count limit alone can create a multi-GB block for large documents. Allow one
            // oversized document, then yield; the source itself bounds each BSON document to 16 MiB.
            if (bson_bytes >= 8 * 1024 * 1024) {
                break;
            }
        }
    }
    _record_scan_rows(cast_set<int64_t>(*rows));
    if (_rows_read) {
        COUNTER_UPDATE(_rows_read, *rows);
    }
    SCOPED_TIMER(_filter_time);
    return apply_materialized_reader_filters(_request.get(), _io_ctx.get(), block, rows);
}
Status MongoFileReader::close() {
    auto stream = std::move(_stream);
    if (stream) {
        stream->close();
    }
    _request.reset();
    _eof = true;
    return Status::OK();
}
MongoReader::MongoReader(MongoStreamFactory factory) : _factory(std::move(factory)) {}
Status MongoReader::prepare_split(const SplitReadOptions& options) {
    RETURN_IF_ERROR(validate_mongodb_range(options.current_range));
    return TableReader::prepare_split(options);
}
Status MongoReader::create_file_reader(std::unique_ptr<FileReader>* reader) {
    *reader = std::make_unique<MongoFileReader>(_system_properties, _current_task->data_file,
                                                _io_ctx, _scanner_profile, _current_file_range_desc,
                                                *_file_slot_descs, _factory);
    return Status::OK();
}

} // namespace doris::format::mongodb
