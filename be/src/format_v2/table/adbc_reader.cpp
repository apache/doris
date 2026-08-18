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

#include "format_v2/table/adbc_reader.h"

#include <arrow-adbc/adbc.h>
#include <arrow-adbc/adbc_driver_manager.h>
#include <arrow/array/array_base.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "common/check.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type_serde/data_type_serde.h"
#include "format/arrow/arrow_array_normalizer.h"
#include "format/parquet/arrow_memory_pool.h"
#include "format_v2/materialized_reader_util.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/file_scan_profile.h"
#include "runtime/runtime_state.h"
#include "util/adbc_driver_registry.h"
#include "util/timezone_utils.h"
#include "util/url_coding.h"

namespace doris::format::adbc {
namespace {

// Keys of TTableFormatFileDesc.adbc_params. Kept next to the code that reads them so the FE-side
// producer and this consumer stay diffable.
constexpr const char* kParamDriverPath = "driver_path";
constexpr const char* kParamDriverEntrypoint = "driver_entrypoint";
constexpr const char* kParamUri = "uri";
constexpr const char* kParamUsername = "username";
constexpr const char* kParamPassword = "password";
constexpr const char* kParamQuerySql = "query_sql";
// Base64 of one opaque partition descriptor the driver produced on FE. Mutually exclusive with
// kParamQuerySql: a range either runs a statement here or reads one partition of a statement the
// source has already run.
constexpr const char* kParamPartitionDescriptor = "partition_descriptor";
// Anything under this prefix is an ADBC option name in full (the prefix is part of the option name,
// e.g. "adbc.connection.autocommit") and is handed to the driver untouched.
constexpr std::string_view kAdbcOptionPrefix = "adbc.";

const std::string* find_param(const std::map<std::string, std::string>& params,
                              const std::string& key) {
    const auto it = params.find(key);
    return it == params.end() ? nullptr : &it->second;
}

Status validate_adbc_range(const TFileRangeDesc& range) {
    if (!range.__isset.table_format_params ||
        range.table_format_params.table_format_type != "adbc") {
        return Status::InvalidArgument("ADBC reader requires the adbc table format");
    }
    if (!range.table_format_params.__isset.adbc_params) {
        return Status::InvalidArgument("ADBC reader requires adbc_params");
    }
    const auto& params = range.table_format_params.adbc_params;
    for (const auto* key : {kParamDriverPath, kParamUri}) {
        const auto* value = find_param(params, key);
        if (value == nullptr || value->empty()) {
            return Status::InvalidArgument("ADBC reader requires a non-empty '{}' parameter", key);
        }
    }
    const auto* query_sql = find_param(params, kParamQuerySql);
    const auto* partition = find_param(params, kParamPartitionDescriptor);
    const bool has_query = query_sql != nullptr && !query_sql->empty();
    const bool has_partition = partition != nullptr && !partition->empty();
    // Not a defensive nicety: reading a partition means the source has ALREADY run the statement, so
    // a range carrying both would let this reader run it a second time depending on which branch it
    // happened to take. FE refuses to build such a range; this refuses to act on one.
    if (has_query == has_partition) {
        return Status::InvalidArgument(
                "ADBC reader requires exactly one of '{}' and '{}', but the range carries {}",
                kParamQuerySql, kParamPartitionDescriptor, has_query ? "both" : "neither");
    }
    return Status::OK();
}

// Drivers allocate the strings inside AdbcError, so every populated error has to be released.
class AdbcErrorGuard {
public:
    AdbcErrorGuard() = default;
    ~AdbcErrorGuard() { reset(); }
    AdbcErrorGuard(const AdbcErrorGuard&) = delete;
    AdbcErrorGuard& operator=(const AdbcErrorGuard&) = delete;

    AdbcError* get() { return &_error; }

    std::string take_message() {
        std::string message = _error.message != nullptr ? _error.message : "";
        reset();
        return message;
    }

    void reset() {
        if (_error.release != nullptr) {
            _error.release(&_error);
        }
        _error = ADBC_ERROR_INIT;
    }

private:
    AdbcError _error = ADBC_ERROR_INIT;
};

Status adbc_call_status(const char* what, AdbcStatusCode code, AdbcErrorGuard& error) {
    const std::string message = error.take_message();
    // AdbcStatusCode is a uint8_t, so spell out the name as well as the number.
    return Status::InternalError("ADBC: {} failed ({}, code {}): {}", what,
                                 AdbcStatusCodeMessage(code), static_cast<int>(code),
                                 message.empty() ? "driver reported no message" : message);
}

#define RETURN_IF_ADBC_ERROR(expr, what, error)                       \
    do {                                                              \
        const AdbcStatusCode adbc_call_code = (expr);                 \
        if (adbc_call_code != ADBC_STATUS_OK) {                       \
            return adbc_call_status((what), adbc_call_code, (error)); \
        }                                                             \
        (error).reset();                                              \
    } while (0)

// The production stream: one ADBC database/connection/statement per scan range.
//
// P0 keeps them un-pooled on purpose. Reusing databases across ranges is a throughput optimization
// that only pays off once multiple partitions run concurrently, and an unverifiable caching layer
// added now would only obscure the functional path.
class RealAdbcStream final : public AdbcStream {
public:
    explicit RealAdbcStream(const TFileRangeDesc& range) : _range(range) {}
    ~RealAdbcStream() override { static_cast<void>(close()); }

    Status open() {
        RETURN_IF_ERROR(validate_adbc_range(_range));
        const auto& params = _range.table_format_params.adbc_params;
        const std::string& driver_path = *find_param(params, kParamDriverPath);
        const std::string& uri = *find_param(params, kParamUri);
        const auto* entrypoint = find_param(params, kParamDriverEntrypoint);
        // validate_adbc_range has established that exactly one of these is present.
        const auto* partition = find_param(params, kParamPartitionDescriptor);

        RETURN_IF_ERROR(AdbcDriverRegistry::instance().get_or_load(
                driver_path, entrypoint != nullptr ? *entrypoint : std::string(), &_driver));
        DORIS_CHECK(_driver != nullptr);

        AdbcErrorGuard error;
        RETURN_IF_ADBC_ERROR(_driver->DatabaseNew(&_database, error.get()), "DatabaseNew", error);
        _database_created = true;
        RETURN_IF_ERROR(_set_database_options(params, uri, error));
        RETURN_IF_ADBC_ERROR(_driver->DatabaseInit(&_database, error.get()), "DatabaseInit", error);

        RETURN_IF_ADBC_ERROR(_driver->ConnectionNew(&_connection, error.get()), "ConnectionNew",
                             error);
        _connection_created = true;
        RETURN_IF_ADBC_ERROR(_driver->ConnectionInit(&_connection, &_database, error.get()),
                             "ConnectionInit", error);

        if (partition != nullptr && !partition->empty()) {
            RETURN_IF_ERROR(_read_partition(*partition, error));
        } else {
            RETURN_IF_ERROR(_execute_query(*find_param(params, kParamQuerySql), error));
        }

        // Before Arrow ever sees it: the driver's stream may not clear its release callback, and
        // Arrow aborts the process when that happens. Both branches need it -- the Flight SQL
        // driver's ReadPartition stream breaks the contract exactly like its ExecuteQuery one.
        enforce_stream_release_contract(&_c_stream);

        auto reader = arrow::ImportRecordBatchReader(&_c_stream);
        if (!reader.ok()) {
            return Status::InternalError("ADBC: failed to import the result stream: {}",
                                         reader.status().ToString());
        }
        // ImportRecordBatchReader moves the stream's contents; the reader owns it from here.
        _reader = reader.MoveValueUnsafe();
        return Status::OK();
    }

    Status next(std::shared_ptr<arrow::RecordBatch>* batch) override {
        DORIS_CHECK(batch != nullptr);
        if (_reader == nullptr) {
            return Status::InternalError("ADBC: result stream is not open");
        }
        std::shared_ptr<arrow::RecordBatch> next_batch;
        const auto status = _reader->ReadNext(&next_batch);
        if (!status.ok()) {
            return Status::InternalError("ADBC: failed to read the next batch: {}",
                                         status.ToString());
        }
        *batch = std::move(next_batch);
        return Status::OK();
    }

    Status close() override {
        Status result = Status::OK();
        // Release in reverse order of creation. The reader owns the imported stream, so it has to
        // go before the statement that produced it.
        _reader.reset();
        if (_c_stream.release != nullptr) {
            // Only reachable when the import itself failed; nothing else owns the stream then.
            _c_stream.release(&_c_stream);
            _c_stream = {};
        }
        AdbcErrorGuard error;
        if (_statement_created) {
            const auto code = _driver->StatementRelease(&_statement, error.get());
            if (code != ADBC_STATUS_OK && result.ok()) {
                result = adbc_call_status("StatementRelease", code, error);
            }
            error.reset();
            _statement_created = false;
        }
        if (_connection_created) {
            const auto code = _driver->ConnectionRelease(&_connection, error.get());
            if (code != ADBC_STATUS_OK && result.ok()) {
                result = adbc_call_status("ConnectionRelease", code, error);
            }
            error.reset();
            _connection_created = false;
        }
        if (_database_created) {
            const auto code = _driver->DatabaseRelease(&_database, error.get());
            if (code != ADBC_STATUS_OK && result.ok()) {
                result = adbc_call_status("DatabaseRelease", code, error);
            }
            error.reset();
            _database_created = false;
        }
        // _driver itself is owned by AdbcDriverRegistry and is never released.
        return result;
    }

private:
    // Runs the statement FE generated. One statement per range, so this range is the whole query.
    Status _execute_query(const std::string& query_sql, AdbcErrorGuard& error) {
        RETURN_IF_ADBC_ERROR(_driver->StatementNew(&_connection, &_statement, error.get()),
                             "StatementNew", error);
        _statement_created = true;
        RETURN_IF_ADBC_ERROR(
                _driver->StatementSetSqlQuery(&_statement, query_sql.c_str(), error.get()),
                "StatementSetSqlQuery", error);
        int64_t rows_affected = -1;
        RETURN_IF_ADBC_ERROR(_driver->StatementExecuteQuery(&_statement, &_c_stream, &rows_affected,
                                                            error.get()),
                             "StatementExecuteQuery", error);
        return Status::OK();
    }

    // Reads one partition of a query FE already had the source execute. No statement is created:
    // ADBC reads a partition off a connection, and the whole point is that this can happen on a
    // different machine from the one that planned it.
    Status _read_partition(const std::string& base64_descriptor, AdbcErrorGuard& error) {
        std::string descriptor;
        if (!base64_decode(base64_descriptor, &descriptor)) {
            return Status::InvalidArgument("ADBC: the '{}' parameter is not valid base64",
                                           kParamPartitionDescriptor);
        }
        RETURN_IF_ADBC_ERROR(
                _driver->ConnectionReadPartition(
                        &_connection, reinterpret_cast<const uint8_t*>(descriptor.data()),
                        descriptor.size(), &_c_stream, error.get()),
                "ConnectionReadPartition", error);
        return Status::OK();
    }

    Status _set_database_options(const std::map<std::string, std::string>& params,
                                 const std::string& uri, AdbcErrorGuard& error) {
        RETURN_IF_ADBC_ERROR(
                _driver->DatabaseSetOption(&_database, ADBC_OPTION_URI, uri.c_str(), error.get()),
                "DatabaseSetOption(uri)", error);
        for (const auto* key : {kParamUsername, kParamPassword}) {
            const auto* value = find_param(params, key);
            if (value == nullptr || value->empty()) {
                continue;
            }
            RETURN_IF_ADBC_ERROR(
                    _driver->DatabaseSetOption(&_database, key, value->c_str(), error.get()),
                    "DatabaseSetOption(credentials)", error);
        }
        for (const auto& [key, value] : params) {
            if (!key.starts_with(kAdbcOptionPrefix)) {
                continue;
            }
            RETURN_IF_ADBC_ERROR(
                    _driver->DatabaseSetOption(&_database, key.c_str(), value.c_str(), error.get()),
                    "DatabaseSetOption(passthrough)", error);
        }
        return Status::OK();
    }

    const TFileRangeDesc _range;
    const AdbcDriver* _driver = nullptr;
    AdbcDatabase _database {};
    AdbcConnection _connection {};
    AdbcStatement _statement {};
    ArrowArrayStream _c_stream {};
    std::shared_ptr<arrow::RecordBatchReader> _reader;
    bool _database_created = false;
    bool _connection_created = false;
    bool _statement_created = false;
};

Status create_real_adbc_stream(const TFileRangeDesc& range, std::unique_ptr<AdbcStream>* out) {
    DORIS_CHECK(out != nullptr);
    auto stream = std::make_unique<RealAdbcStream>(range);
    RETURN_IF_ERROR(stream->open());
    *out = std::move(stream);
    return Status::OK();
}

ColumnDefinition adbc_child_definition(const std::string& name, DataTypePtr type, int32_t local_id);

// Mirrors synthesize_remote_doris_children in remote_doris_reader.cpp. Both readers expose table
// slots as file columns, so complex columns still need structural children for TableColumnMapper.
// Kept separate rather than shared to avoid reshaping the already-shipped remote_doris reader.
std::vector<ColumnDefinition> synthesize_adbc_children(const DataTypePtr& type) {
    std::vector<ColumnDefinition> children;
    DORIS_CHECK(type != nullptr);
    const auto nested_type = remove_nullable(type);
    switch (nested_type->get_primitive_type()) {
    case TYPE_ARRAY: {
        const auto* array_type = assert_cast<const DataTypeArray*>(nested_type.get());
        children.push_back(adbc_child_definition("element", array_type->get_nested_type(), 0));
        break;
    }
    case TYPE_MAP: {
        const auto* map_type = assert_cast<const DataTypeMap*>(nested_type.get());
        children.push_back(adbc_child_definition("key", map_type->get_key_type(), 0));
        children.push_back(adbc_child_definition("value", map_type->get_value_type(), 1));
        break;
    }
    case TYPE_STRUCT: {
        const auto* struct_type = assert_cast<const DataTypeStruct*>(nested_type.get());
        children.reserve(struct_type->get_elements().size());
        for (size_t idx = 0; idx < struct_type->get_elements().size(); ++idx) {
            children.push_back(adbc_child_definition(struct_type->get_element_name(idx),
                                                     struct_type->get_element(idx),
                                                     cast_set<int32_t>(idx)));
        }
        break;
    }
    default:
        break;
    }
    return children;
}

ColumnDefinition adbc_child_definition(const std::string& name, DataTypePtr type,
                                       int32_t local_id) {
    ColumnDefinition child;
    child.identifier = Field::create_field<TYPE_STRING>(name);
    child.local_id = local_id;
    child.name = name;
    child.type = std::move(type);
    child.children = synthesize_adbc_children(child.type);
    return child;
}

// A stream that forwards everything to the driver's and, on release, does the one thing some
// drivers forget: clear its own release callback. Heap-allocated because Arrow keeps only the
// ArrowArrayStream it was handed, and the delegate has to outlive this function.
struct DelegatingStream {
    ArrowArrayStream inner;
};

int delegating_get_schema(ArrowArrayStream* self, ArrowSchema* out) {
    auto& inner = static_cast<DelegatingStream*>(self->private_data)->inner;
    return inner.get_schema(&inner, out);
}

int delegating_get_next(ArrowArrayStream* self, ArrowArray* out) {
    auto& inner = static_cast<DelegatingStream*>(self->private_data)->inner;
    return inner.get_next(&inner, out);
}

const char* delegating_get_last_error(ArrowArrayStream* self) {
    auto& inner = static_cast<DelegatingStream*>(self->private_data)->inner;
    return inner.get_last_error != nullptr ? inner.get_last_error(&inner) : nullptr;
}

void delegating_release(ArrowArrayStream* self) {
    auto* delegate = static_cast<DelegatingStream*>(self->private_data);
    if (delegate->inner.release != nullptr) {
        delegate->inner.release(&delegate->inner);
    }
    delete delegate;
    self->private_data = nullptr;
    // What the driver failed to do, and what Arrow aborts the process over.
    self->release = nullptr;
}

} // namespace

void enforce_stream_release_contract(ArrowArrayStream* stream) {
    DORIS_CHECK(stream != nullptr);
    if (stream->release == nullptr) {
        // Already released; nothing to delegate to, and wrapping it would hand Arrow a stream
        // whose callbacks dereference a released delegate.
        return;
    }
    auto* delegate = new DelegatingStream {.inner = *stream};
    *stream = ArrowArrayStream {.get_schema = delegating_get_schema,
                                .get_next = delegating_get_next,
                                .get_last_error = delegating_get_last_error,
                                .release = delegating_release,
                                .private_data = delegate};
}

AdbcFileReader::AdbcFileReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
                               std::unique_ptr<io::FileDescription>& file_description,
                               std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
                               const TFileRangeDesc& range,
                               const std::vector<SlotDescriptor*>& file_slot_descs,
                               AdbcStreamFactory stream_factory)
        : FileReader(system_properties, file_description, std::move(io_ctx), profile),
          _range(range),
          _file_slot_descs(file_slot_descs),
          _stream_factory(std::move(stream_factory)) {
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, _ctz);
}

AdbcFileReader::~AdbcFileReader() {
    static_cast<void>(close());
}

void AdbcFileReader::_init_profile() {
    if (_profile == nullptr) {
        return;
    }
    const auto hierarchy = file_scan_profile::ensure_hierarchy(_profile);
    _io_time = hierarchy.io;
    static const char* adbc_profile = "AdbcFileReader";
    _total_time =
            ADD_CHILD_TIMER_WITH_LEVEL(_profile, adbc_profile, file_scan_profile::FILE_READER, 1);
    _open_stream_time = ADD_CHILD_TIMER_WITH_LEVEL(_profile, "AdbcOpenStreamTime", adbc_profile, 1);
    _next_batch_time = ADD_CHILD_TIMER_WITH_LEVEL(_profile, "AdbcNextBatchTime", adbc_profile, 1);
    _normalize_time = ADD_CHILD_TIMER_WITH_LEVEL(_profile, "AdbcNormalizeTime", adbc_profile, 1);
    _materialize_time =
            ADD_CHILD_TIMER_WITH_LEVEL(_profile, "AdbcMaterializeTime", adbc_profile, 1);
    _filter_time = ADD_CHILD_TIMER_WITH_LEVEL(_profile, "AdbcFilterTime", adbc_profile, 1);
}

Status AdbcFileReader::init(RuntimeState* state) {
    _init_profile();
    SCOPED_TIMER(_total_time);
    _runtime_state = state;
    RETURN_IF_ERROR(validate_adbc_range(_range));
    RETURN_IF_ERROR(_build_col_name_to_file_id());
    _eof = false;
    return Status::OK();
}

Status AdbcFileReader::get_schema(std::vector<ColumnDefinition>* file_schema) const {
    SCOPED_TIMER(_total_time);
    DORIS_CHECK(file_schema != nullptr);
    file_schema->clear();
    file_schema->reserve(_file_slot_descs.size());
    for (size_t idx = 0; idx < _file_slot_descs.size(); ++idx) {
        const auto* slot = _file_slot_descs[idx];
        DORIS_CHECK(slot != nullptr);
        file_schema->push_back({
                .identifier = Field::create_field<TYPE_INT>(cast_set<int32_t>(idx)),
                .local_id = cast_set<int32_t>(idx),
                .name = slot->col_name(),
                .type = slot->type(),
                .children = synthesize_adbc_children(slot->type()),
        });
    }
    return Status::OK();
}

Status AdbcFileReader::open(std::shared_ptr<FileScanRequest> request) {
    SCOPED_TIMER(_total_time);
    SCOPED_TIMER(_open_stream_time);
    RETURN_IF_ERROR(FileReader::open(std::move(request)));
    RETURN_IF_ERROR(_open_stream());
    _eof = false;
    return Status::OK();
}

Status AdbcFileReader::get_block(Block* file_block, size_t* rows, bool* eof) {
    SCOPED_TIMER(_total_time);
    DORIS_CHECK(file_block != nullptr);
    DORIS_CHECK(rows != nullptr);
    DORIS_CHECK(eof != nullptr);
    if (_stream == nullptr) {
        return Status::InternalError("ADBC reader is not open");
    }
    if (_io_ctx != nullptr && _io_ctx->should_stop) {
        // Observe cancellation before entering a potentially blocking driver read.
        RETURN_IF_ERROR(close());
        *rows = 0;
        *eof = true;
        return Status::OK();
    }

    *rows = 0;
    *eof = false;
    std::shared_ptr<arrow::RecordBatch> batch;
    {
        SCOPED_TIMER(_io_time);
        SCOPED_TIMER(_next_batch_time);
        RETURN_IF_ERROR(_stream->next(&batch));
    }
    if (batch == nullptr) {
        *eof = true;
        _eof = true;
        return Status::OK();
    }

    {
        SCOPED_TIMER(_materialize_time);
        RETURN_IF_ERROR(_materialize_record_batch(*batch, file_block, rows));
    }
    _record_scan_rows(cast_set<int64_t>(*rows));
    {
        SCOPED_TIMER(_filter_time);
        RETURN_IF_ERROR(
                apply_materialized_reader_filters(_request.get(), _io_ctx.get(), file_block, rows));
    }
    return Status::OK();
}

Status AdbcFileReader::close() {
    SCOPED_TIMER(_total_time);
    if (_stream != nullptr) {
        RETURN_IF_ERROR(_stream->close());
        _stream.reset();
    }
    _request.reset();
    _eof = true;
    return Status::OK();
}

Status AdbcFileReader::_open_stream() {
    DORIS_CHECK(_stream == nullptr);
    if (_stream_factory) {
        RETURN_IF_ERROR(_stream_factory(_range, &_stream));
    } else {
        RETURN_IF_ERROR(create_real_adbc_stream(_range, &_stream));
    }
    DORIS_CHECK(_stream != nullptr);
    return Status::OK();
}

Status AdbcFileReader::_materialize_record_batch(const arrow::RecordBatch& batch, Block* file_block,
                                                 size_t* rows) const {
    DORIS_CHECK(file_block != nullptr);
    DORIS_CHECK(rows != nullptr);
    if (_request == nullptr) {
        return Status::InternalError("ADBC reader is not open");
    }

    if (_col_name_to_file_id.empty()) {
        // A pushed-down COUNT(*) projects no columns at all: the scan wants rows counted, no values.
        // Counting here rather than falling into the loop below is not an optimization -- every column
        // the source returns is unrequested by definition in this state, so the loop's unknown-column
        // check would reject the first one and fail a query that asked for nothing but a number.
        // FE sends a one-constant-column statement for this case, so the batch is narrow.
        //
        // Only the empty case is special-cased. An unrequested column arriving alongside requested ones
        // still fails: that means FE and this reader disagree about the projection, and it is the one
        // signal that the disagreement exists.
        *rows = cast_set<size_t>(batch.num_rows());
        return Status::OK();
    }

    ArrowMemoryPool<> local_arrow_pool;
    arrow::MemoryPool* arrow_pool = ExecEnv::GetInstance()->arrow_memory_pool();
    if (arrow_pool == nullptr) {
        // Embedded and unit-test runtimes may omit ExecEnv memory initialization; keep conversions
        // on Doris' tracked allocator instead of falling back to Arrow's untracked default pool.
        arrow_pool = &local_arrow_pool;
    }
    std::vector<bool> materialized_columns(file_block->columns(), false);
    for (int arrow_idx = 0; arrow_idx < batch.num_columns(); ++arrow_idx) {
        const std::string& column_name = batch.schema()->field(arrow_idx)->name();
        const auto file_id_it = _col_name_to_file_id.find(column_name);
        if (file_id_it == _col_name_to_file_id.end()) {
            return Status::InternalError("ADBC source returned unknown column {}", column_name);
        }
        const auto block_position_it = _request->local_positions.find(file_id_it->second);
        if (block_position_it == _request->local_positions.end()) {
            continue;
        }
        std::shared_ptr<arrow::Array> array;
        {
            SCOPED_TIMER(_normalize_time);
            RETURN_IF_ERROR(normalize_arrow_array(batch.column(arrow_idx), arrow_pool, &array));
        }
        RETURN_IF_ERROR(_materialize_arrow_column(column_name, array, batch.num_rows(),
                                                  file_id_it->second, block_position_it->second,
                                                  file_block));
        materialized_columns[block_position_it->second.value()] = true;
    }

    for (const auto& [file_column_id, block_position] : _request->local_positions) {
        if (block_position.value() >= materialized_columns.size()) {
            return Status::InternalError(
                    "ADBC requested block position {} out of range, block columns {}",
                    block_position.value(), materialized_columns.size());
        }
        if (!materialized_columns[block_position.value()]) {
            return Status::InternalError("ADBC source did not return requested file column id {}",
                                         file_column_id.value());
        }
    }

    *rows = cast_set<size_t>(batch.num_rows());
    return Status::OK();
}

Status AdbcFileReader::_materialize_arrow_column(const std::string& column_name,
                                                 const std::shared_ptr<arrow::Array>& array,
                                                 int64_t num_rows, LocalColumnId file_column_id,
                                                 const LocalIndex& block_position,
                                                 Block* file_block) const {
    DORIS_CHECK(file_block != nullptr);
    DORIS_CHECK(array != nullptr);
    if (block_position.value() >= file_block->columns()) {
        return Status::InternalError("ADBC block position {} out of range, block columns {}",
                                     block_position.value(), file_block->columns());
    }
    auto columns_guard = file_block->mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    const auto& target_type = columns_guard.get_datatype_by_position(block_position.value());

    // The cached FE target remains authoritative when a source schema changes mid-cache-window;
    // otherwise SerDes without a null map can silently turn source nulls into default values.
    if (array->null_count() > 0 && !target_type->is_nullable()) {
        return Status::InternalError(
                "ADBC Arrow column '{}' contains {} null rows for non-nullable Doris type {}",
                column_name, array->null_count(), target_type->get_name());
    }

    // An all-null column arrives with a type that says nothing about the column.
    //
    // A source that infers Arrow types from the VALUES it returns -- rather than from the declared
    // column type -- has nothing to infer from when every value in the result is null, and picks
    // whatever its default is. Measured against the SQLite driver: the same TEXT column comes back
    // as utf8 for `SELECT id, name FROM t1` and as int64 for
    // `SELECT id, name FROM t1 WHERE name IS NULL`, purely because the filter left only nulls.
    // Handing that to the serde fails with "Unsupported arrow type for string column: 9", and no
    // amount of care on the FE side avoids it: FE cannot know in advance which rows will survive.
    //
    // N nulls are what this array means whatever type it claims, so materialize them directly. The
    // check is narrow on purpose -- a column with even one non-null value keeps its real type and
    // still fails loudly on a genuine mismatch, which is the signal that FE and the source disagree
    // about the schema rather than about one result set.
    //
    // Only for a nullable target: substituting defaults into a NOT NULL column would turn a source
    // that wrongly sent nulls into silently wrong data, so that keeps failing in the serde.
    if (array->null_count() == array->length() && target_type->is_nullable()) {
        columns[block_position.value()]->insert_many_defaults(cast_set<size_t>(num_rows));
        return Status::OK();
    }

    try {
        RETURN_IF_ERROR(target_type->get_serde()->read_column_from_arrow(
                *columns[block_position.value()], array.get(), 0, num_rows, _ctz));
    } catch (const Exception& e) {
        return Status::InternalError(
                "Failed to convert ADBC Arrow column '{}' (file_column_id={}, arrow type={}) to "
                "Doris block: {}",
                column_name, file_column_id.value(), array->type()->ToString(), e.what());
    }
    return Status::OK();
}

Status AdbcFileReader::_build_col_name_to_file_id() {
    _col_name_to_file_id.clear();
    _col_name_to_file_id.reserve(_file_slot_descs.size());
    for (size_t idx = 0; idx < _file_slot_descs.size(); ++idx) {
        const auto* slot = _file_slot_descs[idx];
        DORIS_CHECK(slot != nullptr);
        _col_name_to_file_id.emplace(slot->col_name(), LocalColumnId(cast_set<int32_t>(idx)));
    }
    return Status::OK();
}

AdbcReader::AdbcReader(AdbcStreamFactory stream_factory)
        : _stream_factory(std::move(stream_factory)) {}

Status AdbcReader::init(TableReadOptions&& options) {
    if (options.file_slot_descs == nullptr) {
        return Status::InvalidArgument("ADBC reader requires file slot descriptors");
    }
    return TableReader::init(std::move(options));
}

Status AdbcReader::prepare_split(const SplitReadOptions& options) {
    {
        // Keep protocol validation visible while avoiding overlap with TableReader's own scopes.
        SCOPED_TIMER(_profile.total_timer);
        SCOPED_TIMER(_profile.prepare_split_timer);
        RETURN_IF_ERROR(validate_adbc_range(options.current_range));
    }
    return TableReader::prepare_split(options);
}

Status AdbcReader::create_file_reader(std::unique_ptr<FileReader>* reader) {
    DORIS_CHECK(reader != nullptr);
    DORIS_CHECK(_file_slot_descs != nullptr);
    *reader = std::make_unique<AdbcFileReader>(_system_properties, _current_task->data_file,
                                               _io_ctx, _scanner_profile, _current_file_range_desc,
                                               *_file_slot_descs, _stream_factory);
    return Status::OK();
}

} // namespace doris::format::adbc
