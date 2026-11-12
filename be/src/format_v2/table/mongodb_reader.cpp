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
#include <thread>
#include <unordered_map>

#include "core/block/block.h"
#include "core/data_type_serde/bson_value_view.h"
#include "core/data_type_serde/data_type_serde.h"
#include "format_v2/column_mapper.h"
#include "format_v2/materialized_reader_util.h"
#include "runtime/descriptors.h"
#include "runtime/file_scan_profile.h"
#include "runtime/runtime_state.h"

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

struct ClientPool {
    MongoPtr<mongoc_client_pool_t, mongoc_client_pool_destroy> pool;
};

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
    key += ":" + std::to_string(d.timeout_ms) + ":" + std::to_string(d.connect_timeout_ms) + ":" +
           std::to_string(d.server_selection_timeout_ms) + ":" +
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
    mongoc_uri_set_option_as_int32(
            uri.get(), MONGOC_URI_CONNECTTIMEOUTMS,
            static_cast<int32_t>(std::min<int64_t>(d.connect_timeout_ms, d.timeout_ms)));
    mongoc_uri_set_option_as_int32(
            uri.get(), MONGOC_URI_SERVERSELECTIONTIMEOUTMS,
            static_cast<int32_t>(std::min<int64_t>(d.server_selection_timeout_ms, d.timeout_ms)));
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

Status mongo_error(const bson_error_t& error) {
    // Driver messages can contain URIs, credentials, commands, or source values.
    return Status::IOError("MongoDB command failed (domain={}, code={})", error.domain, error.code);
}

class NativeMongoStream final : public MongoCursorStream {
public:
    ~NativeMongoStream() override { close(); }
    Status open(const TMongoScanDesc& d, std::function<bool()> cancelled) override {
        _cancelled = std::move(cancelled);
        _deadline = Clock::now() + std::chrono::milliseconds(d.timeout_ms);
        RETURN_IF_ERROR(check_deadline());
        RETURN_IF_ERROR(acquire_pool(d, &_pool));
        while (!(_client = mongoc_client_pool_try_pop(_pool->pool.get()))) {
            RETURN_IF_ERROR(check_deadline());
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        mongoc_client_set_sockettimeoutms(_client, static_cast<int32_t>(std::min<int64_t>(
                                                           d.socket_timeout_ms, remaining_ms())));
        RETURN_IF_ERROR(verify_collection_identity(d));
        _collection.reset(
                mongoc_client_get_collection(_client, d.database.c_str(), d.collection.c_str()));
        if (!_collection) {
            return Status::InvalidArgument("Invalid MongoDB namespace");
        }
        ReadConcernPtr concern(mongoc_read_concern_new());
        if (!mongoc_read_concern_set_level(concern.get(), d.read_concern.c_str())) {
            return Status::InvalidArgument("Invalid MongoDB read concern");
        }
        mongoc_collection_set_read_concern(_collection.get(), concern.get());
        RETURN_IF_ERROR(configure_mongodb_read_preference(_collection.get(), d.read_preference));
        bson_t filter = BSON_INITIALIZER;
        bson_t options = BSON_INITIALIZER;
        BSON_APPEND_INT32(&options, "batchSize", d.batch_size);
        BSON_APPEND_INT64(&options, "maxTimeMS", remaining_ms());
        // Always query the public logical namespace: a view may change rows, names, and _id,
        // and time-series buckets are an internal storage representation.
        _cursor.reset(
                mongoc_collection_find_with_opts(_collection.get(), &filter, &options, nullptr));
        bson_destroy(&options);
        bson_destroy(&filter);
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
        *eof = true;
        return Status::OK();
    }
    void close() override {
        // Destroy the cursor (killCursors) before releasing its collection and exclusive pool lease.
        _cursor.reset();
        _collection.reset();
        if (_client) {
            mongoc_client_pool_push(_pool->pool.get(), _client);
            _client = nullptr;
        }
        _pool.reset();
    }

private:
    Status verify_collection_identity(const TMongoScanDesc& d) {
        if (d.collection_uuid.empty()) {
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
        if (cursor && mongoc_cursor_next(cursor.get(), &document)) {
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
        if (actual != d.collection_uuid) {
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
         d.read_concern != "available")) {
        return Status::InvalidArgument("Invalid or unsupported MongoDB native scan descriptor");
    }
    return Status::OK();
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
            // Resource/structural failures apply to the entire document, outside the per-field
            // mismatch-as-NULL policy, so projection cannot bypass the decoding limits.
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
