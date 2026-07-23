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

#include "format_v2/table/remote_doris_reader.h"

#include <arrow/flight/client.h>
#include <arrow/flight/types.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type_serde/data_type_serde.h"
#include "format/arrow/arrow_utils.h"
#include "format_v2/materialized_reader_util.h"
#include "runtime/descriptors.h"
#include "runtime/file_scan_profile.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/timezone_utils.h"

namespace doris::format::remote_doris {
namespace {

Status validate_remote_doris_range(const TFileRangeDesc& range) {
    if (!range.__isset.table_format_params ||
        range.table_format_params.table_format_type != "remote_doris") {
        return Status::InvalidArgument("Remote Doris v2 reader requires remote_doris table format");
    }
    if (!range.table_format_params.__isset.remote_doris_params) {
        return Status::InvalidArgument("Remote Doris v2 reader requires remote_doris_params");
    }
    const auto& params = range.table_format_params.remote_doris_params;
    if (!params.__isset.location_uri || params.location_uri.empty()) {
        return Status::InvalidArgument("Remote Doris v2 reader requires location_uri");
    }
    if (!params.__isset.ticket || params.ticket.empty()) {
        return Status::InvalidArgument("Remote Doris v2 reader requires ticket");
    }
    return Status::OK();
}

class FlightRemoteDorisStream final : public RemoteDorisStream {
public:
    FlightRemoteDorisStream(const TFileRangeDesc& range, std::shared_ptr<io::IOContext> io_ctx,
                            RuntimeState* runtime_state, int timeout_seconds)
            : _range(range),
              _io_ctx(std::move(io_ctx)),
              _runtime_state(runtime_state),
              _timeout_seconds(std::max(1, timeout_seconds)) {}

    Status open() {
        RETURN_IF_ERROR(validate_remote_doris_range(_range));
        const auto& params = _range.table_format_params.remote_doris_params;
        arrow::flight::Location location;
        RETURN_DORIS_STATUS_IF_ERROR(
                arrow::flight::Location::Parse(params.location_uri).Value(&location));
        arrow::flight::Ticket ticket;
        RETURN_DORIS_STATUS_IF_ERROR(
                arrow::flight::Ticket::Deserialize(params.ticket).Value(&ticket));
        struct PendingOpen {
            std::mutex mutex;
            std::condition_variable cv;
            bool done = false;
            bool abandoned = false;
            arrow::Status status = arrow::Status::OK();
            std::unique_ptr<arrow::flight::FlightClient> client;
            std::unique_ptr<arrow::flight::FlightStreamReader> stream;
        };
        auto pending = std::make_shared<PendingOpen>();
        std::unique_ptr<arrow::flight::FlightClient> flight_client;
        RETURN_DORIS_STATUS_IF_ERROR(
                arrow::flight::FlightClient::Connect(location).Value(&flight_client));
        arrow::flight::FlightCallOptions options;
        // A Flight deadline covers streaming reads as well as DoGet setup, so a stalled Next()
        // cannot outlive the query execution timeout indefinitely.
        options.timeout = std::chrono::seconds(_timeout_seconds);
        // Start before DoGet because endpoint setup is itself a blocking RPC covered by the same
        // query/scanner cancellation contract as streaming Next().
        _cancellation_watcher = std::jthread(
                [this](std::stop_token stop_token) { _watch_cancellation(stop_token); });

        std::shared_ptr<ResourceContext> resource_ctx;
        if (_runtime_state != nullptr && _runtime_state->get_query_ctx() != nullptr) {
            resource_ctx = _runtime_state->get_query_ctx()->resource_ctx();
        }
        std::thread do_get_thread([pending, options, ticket, resource_ctx,
                                   client = std::move(flight_client)]() mutable {
            const auto do_get = [&] {
                std::unique_ptr<arrow::flight::FlightStreamReader> stream;
                auto status = client->DoGet(options, ticket).Value(&stream);
                {
                    std::lock_guard lock(pending->mutex);
                    if (!pending->abandoned) {
                        pending->status = std::move(status);
                        pending->client = std::move(client);
                        pending->stream = std::move(stream);
                    } else {
                        // A detached worker must release its query-owned Flight client
                        // before leaving the task attachment that accounts for it.
                        client.reset();
                    }
                    pending->done = true;
                }
                pending->cv.notify_all();
            };
            if (resource_ctx != nullptr) {
                SCOPED_ATTACH_TASK(resource_ctx);
                do_get();
            } else {
                SCOPED_INIT_THREAD_CONTEXT();
                do_get();
            }
        });
        bool cancelled_during_open = false;
        {
            std::unique_lock lock(pending->mutex);
            while (!pending->done && !_is_cancelled()) {
                pending->cv.wait_for(lock, std::chrono::milliseconds(25));
            }
            if (!pending->done) {
                pending->abandoned = true;
                cancelled_during_open = true;
            }
        }
        if (cancelled_during_open) {
            // Arrow 17 exposes no cancellable handle until DoGet returns. Detaching the bounded RPC
            // keeps query/scanner shutdown prompt while the call is still capped by its deadline;
            // the shared state owns all Arrow objects until that worker exits.
            do_get_thread.detach();
            _stop_cancellation_watcher();
            return Status::Cancelled("Remote Doris Flight open was cancelled");
        }
        do_get_thread.join();
        if (!pending->status.ok()) {
            _stop_cancellation_watcher();
            RETURN_DORIS_STATUS_IF_ERROR(pending->status);
        }
        {
            std::lock_guard lock(_flight_mutex);
            _flight_client = std::move(pending->client);
            _stream = std::move(pending->stream);
        }
        if (_is_cancelled()) {
            _cancel_flight_call();
            _stop_cancellation_watcher();
            return Status::Cancelled("Remote Doris Flight open was cancelled");
        }
        return Status::OK();
    }

    Status next(std::shared_ptr<arrow::RecordBatch>* batch) override {
        DORIS_CHECK(batch != nullptr);
        if (_io_ctx != nullptr && _io_ctx->should_stop) {
            _cancel_flight_call();
            return Status::Cancelled("Remote Doris Flight read was cancelled");
        }
        arrow::flight::FlightStreamChunk chunk;
        RETURN_DORIS_STATUS_IF_ERROR(_stream->Next().Value(&chunk));
        *batch = chunk.data;
        return Status::OK();
    }

    Status close() override {
        _stop_cancellation_watcher();
        {
            std::lock_guard lock(_flight_mutex);
            if (_stream != nullptr) {
                _stream->Cancel();
            }
        }
        _stream.reset();
        if (_flight_client != nullptr) {
            RETURN_DORIS_STATUS_IF_ERROR(_flight_client->Close());
            _flight_client.reset();
        }
        return Status::OK();
    }

private:
    bool _is_cancelled() const {
        return (_runtime_state != nullptr && _runtime_state->is_cancelled()) ||
               (_io_ctx != nullptr && _io_ctx->should_stop);
    }

    void _cancel_flight_call() {
        std::lock_guard lock(_flight_mutex);
        if (_stream != nullptr) {
            _stream->Cancel();
        }
    }

    void _watch_cancellation_loop(std::stop_token watcher_stop_token) {
        while (!watcher_stop_token.stop_requested()) {
            if (_is_cancelled()) {
                _cancel_flight_call();
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
    }

    void _watch_cancellation(std::stop_token watcher_stop_token) {
        if (_runtime_state != nullptr && _runtime_state->get_query_ctx() != nullptr &&
            _runtime_state->get_query_ctx()->resource_ctx() != nullptr) {
            // The watcher is query-owned and may allocate in Arrow while signalling cancellation.
            SCOPED_ATTACH_TASK(_runtime_state);
            _watch_cancellation_loop(watcher_stop_token);
            return;
        }
        // Metadata/tests can construct a RuntimeState without a QueryContext; initialize TLS there
        // instead of violating AttachTask's non-null resource-context invariant.
        SCOPED_INIT_THREAD_CONTEXT();
        _watch_cancellation_loop(watcher_stop_token);
    }

    void _stop_cancellation_watcher() {
        if (_cancellation_watcher.joinable()) {
            _cancellation_watcher.request_stop();
            _cancellation_watcher.join();
        }
    }

    const TFileRangeDesc _range;
    std::shared_ptr<io::IOContext> _io_ctx;
    RuntimeState* _runtime_state;
    int _timeout_seconds;
    std::jthread _cancellation_watcher;
    std::mutex _flight_mutex;
    std::unique_ptr<arrow::flight::FlightClient> _flight_client;
    std::unique_ptr<arrow::flight::FlightStreamReader> _stream;
};

Status create_flight_stream(const TFileRangeDesc& range, std::shared_ptr<io::IOContext> io_ctx,
                            RuntimeState* runtime_state, int timeout_seconds,
                            std::unique_ptr<RemoteDorisStream>* out) {
    DORIS_CHECK(out != nullptr);
    auto stream = std::make_unique<FlightRemoteDorisStream>(range, std::move(io_ctx), runtime_state,
                                                            timeout_seconds);
    RETURN_IF_ERROR(stream->open());
    *out = std::move(stream);
    return Status::OK();
}

ColumnDefinition remote_doris_child_definition(const std::string& name, DataTypePtr type,
                                               int32_t local_id);

std::vector<ColumnDefinition> synthesize_remote_doris_children(const DataTypePtr& type) {
    std::vector<ColumnDefinition> children;
    DORIS_CHECK(type != nullptr);
    const auto nested_type = remove_nullable(type);
    switch (nested_type->get_primitive_type()) {
    case TYPE_ARRAY: {
        const auto* array_type = assert_cast<const DataTypeArray*>(nested_type.get());
        children.push_back(
                remote_doris_child_definition("element", array_type->get_nested_type(), 0));
        break;
    }
    case TYPE_MAP: {
        const auto* map_type = assert_cast<const DataTypeMap*>(nested_type.get());
        children.push_back(remote_doris_child_definition("key", map_type->get_key_type(), 0));
        children.push_back(remote_doris_child_definition("value", map_type->get_value_type(), 1));
        break;
    }
    case TYPE_STRUCT: {
        const auto* struct_type = assert_cast<const DataTypeStruct*>(nested_type.get());
        children.reserve(struct_type->get_elements().size());
        for (size_t idx = 0; idx < struct_type->get_elements().size(); ++idx) {
            children.push_back(remote_doris_child_definition(struct_type->get_element_name(idx),
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

ColumnDefinition remote_doris_child_definition(const std::string& name, DataTypePtr type,
                                               int32_t local_id) {
    ColumnDefinition child;
    child.identifier = Field::create_field<TYPE_STRING>(name);
    child.local_id = local_id;
    child.name = name;
    child.type = std::move(type);
    child.children = synthesize_remote_doris_children(child.type);
    return child;
}

} // namespace

RemoteDorisFileReader::RemoteDorisFileReader(
        std::shared_ptr<io::FileSystemProperties>& system_properties,
        std::unique_ptr<io::FileDescription>& file_description,
        std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile, const TFileRangeDesc& range,
        const std::vector<SlotDescriptor*>& file_slot_descs,
        RemoteDorisStreamFactory stream_factory)
        : FileReader(system_properties, file_description, std::move(io_ctx), profile),
          _range(range),
          _file_slot_descs(file_slot_descs),
          _stream_factory(std::move(stream_factory)) {
    TimezoneUtils::find_cctz_time_zone(TimezoneUtils::default_time_zone, _ctz);
}

RemoteDorisFileReader::~RemoteDorisFileReader() {
    static_cast<void>(close());
}

void RemoteDorisFileReader::_init_profile() {
    if (_profile == nullptr) {
        return;
    }
    const auto hierarchy = file_scan_profile::ensure_hierarchy(_profile);
    _io_time = hierarchy.io;
    static const char* remote_profile = "RemoteDorisFileReader";
    _total_time =
            ADD_CHILD_TIMER_WITH_LEVEL(_profile, remote_profile, file_scan_profile::FILE_READER, 1);
    _open_stream_time =
            ADD_CHILD_TIMER_WITH_LEVEL(_profile, "RemoteDorisOpenStreamTime", remote_profile, 1);
    _next_batch_time =
            ADD_CHILD_TIMER_WITH_LEVEL(_profile, "RemoteDorisNextBatchTime", remote_profile, 1);
    _materialize_time =
            ADD_CHILD_TIMER_WITH_LEVEL(_profile, "RemoteDorisMaterializeTime", remote_profile, 1);
    _filter_time = ADD_CHILD_TIMER_WITH_LEVEL(_profile, "RemoteDorisFilterTime", remote_profile, 1);
}

Status RemoteDorisFileReader::init(RuntimeState* state) {
    _init_profile();
    SCOPED_TIMER(_total_time);
    if (state != nullptr) {
        _flight_timeout_seconds = std::max(1, state->execution_timeout());
    }
    _runtime_state = state;
    RETURN_IF_ERROR(validate_remote_doris_range(_range));
    RETURN_IF_ERROR(_build_col_name_to_file_id());
    _eof = false;
    return Status::OK();
}

Status RemoteDorisFileReader::get_schema(std::vector<ColumnDefinition>* file_schema) const {
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
                // Remote Doris exposes table slots as file columns. Complex columns still need
                // structural children so TableColumnMapper can validate and project them.
                .children = synthesize_remote_doris_children(slot->type()),
        });
    }
    return Status::OK();
}

Status RemoteDorisFileReader::open(std::shared_ptr<FileScanRequest> request) {
    SCOPED_TIMER(_total_time);
    SCOPED_TIMER(_open_stream_time);
    RETURN_IF_ERROR(FileReader::open(std::move(request)));
    RETURN_IF_ERROR(_open_stream());
    _eof = false;
    return Status::OK();
}

Status RemoteDorisFileReader::get_block(Block* file_block, size_t* rows, bool* eof) {
    SCOPED_TIMER(_total_time);
    DORIS_CHECK(file_block != nullptr);
    DORIS_CHECK(rows != nullptr);
    DORIS_CHECK(eof != nullptr);
    if (_stream == nullptr) {
        return Status::InternalError("Remote Doris v2 reader is not open");
    }
    if (_io_ctx != nullptr && _io_ctx->should_stop) {
        // Observe cancellation before entering a potentially blocking Flight read; the production
        // stream also carries a query-bounded RPC deadline for cancellation arriving mid-read.
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

Status RemoteDorisFileReader::close() {
    SCOPED_TIMER(_total_time);
    if (_stream != nullptr) {
        RETURN_IF_ERROR(_stream->close());
        _stream.reset();
    }
    _request.reset();
    _eof = true;
    return Status::OK();
}

Status RemoteDorisFileReader::_open_stream() {
    DORIS_CHECK(_stream == nullptr);
    if (_stream_factory) {
        RETURN_IF_ERROR(_stream_factory(_range, &_stream));
    } else {
        RETURN_IF_ERROR(create_flight_stream(_range, _io_ctx, _runtime_state,
                                             _flight_timeout_seconds, &_stream));
    }
    DORIS_CHECK(_stream != nullptr);
    return Status::OK();
}

Status RemoteDorisFileReader::_materialize_record_batch(const arrow::RecordBatch& batch,
                                                        Block* file_block, size_t* rows) const {
    DORIS_CHECK(file_block != nullptr);
    DORIS_CHECK(rows != nullptr);
    if (_request == nullptr) {
        return Status::InternalError("Remote Doris v2 reader is not open");
    }

    std::vector<bool> materialized_columns(file_block->columns(), false);
    for (int arrow_idx = 0; arrow_idx < batch.num_columns(); ++arrow_idx) {
        const std::string& column_name = batch.schema()->field(arrow_idx)->name();
        const auto file_id_it = _col_name_to_file_id.find(column_name);
        if (file_id_it == _col_name_to_file_id.end()) {
            return Status::InternalError("Remote Doris returned unknown column {}", column_name);
        }
        const auto block_position_it = _request->local_positions.find(file_id_it->second);
        if (block_position_it == _request->local_positions.end()) {
            continue;
        }
        RETURN_IF_ERROR(_materialize_arrow_column(batch, arrow_idx, file_id_it->second,
                                                  block_position_it->second, file_block));
        materialized_columns[block_position_it->second.value()] = true;
    }

    for (const auto& [file_column_id, block_position] : _request->local_positions) {
        if (block_position.value() >= materialized_columns.size()) {
            return Status::InternalError(
                    "Remote Doris requested block position {} out of range, block columns {}",
                    block_position.value(), materialized_columns.size());
        }
        if (!materialized_columns[block_position.value()]) {
            return Status::InternalError("Remote Doris did not return requested file column id {}",
                                         file_column_id.value());
        }
    }

    *rows = cast_set<size_t>(batch.num_rows());
    return Status::OK();
}

Status RemoteDorisFileReader::_materialize_arrow_column(const arrow::RecordBatch& batch,
                                                        int arrow_column_idx,
                                                        LocalColumnId file_column_id,
                                                        const LocalIndex& block_position,
                                                        Block* file_block) const {
    DORIS_CHECK(file_block != nullptr);
    if (block_position.value() >= file_block->columns()) {
        return Status::InternalError(
                "Remote Doris block position {} out of range, block columns {}",
                block_position.value(), file_block->columns());
    }
    const auto column_name = batch.schema()->field(arrow_column_idx)->name();
    auto columns_guard = file_block->mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    try {
        RETURN_IF_ERROR(columns_guard.get_datatype_by_position(block_position.value())
                                ->get_serde()
                                ->read_column_from_arrow(*columns[block_position.value()],
                                                         batch.column(arrow_column_idx).get(), 0,
                                                         batch.num_rows(), _ctz));
    } catch (const Exception& e) {
        return Status::InternalError(
                "Failed to convert Remote Doris Arrow column '{}' (file_column_id={}) to Doris "
                "block: {}",
                column_name, file_column_id.value(), e.what());
    }
    return Status::OK();
}

Status RemoteDorisFileReader::_build_col_name_to_file_id() {
    _col_name_to_file_id.clear();
    _col_name_to_file_id.reserve(_file_slot_descs.size());
    for (size_t idx = 0; idx < _file_slot_descs.size(); ++idx) {
        const auto* slot = _file_slot_descs[idx];
        DORIS_CHECK(slot != nullptr);
        _col_name_to_file_id.emplace(slot->col_name(), LocalColumnId(cast_set<int32_t>(idx)));
    }
    return Status::OK();
}

RemoteDorisReader::RemoteDorisReader(RemoteDorisStreamFactory stream_factory)
        : _stream_factory(std::move(stream_factory)) {}

Status RemoteDorisReader::init(TableReadOptions&& options) {
    if (options.file_slot_descs == nullptr) {
        return Status::InvalidArgument("Remote Doris v2 reader requires file slot descriptors");
    }
    return TableReader::init(std::move(options));
}

Status RemoteDorisReader::prepare_split(const SplitReadOptions& options) {
    {
        // Keep protocol validation visible while avoiding overlap with TableReader's own scopes.
        SCOPED_TIMER(_profile.total_timer);
        SCOPED_TIMER(_profile.prepare_split_timer);
        RETURN_IF_ERROR(validate_remote_doris_range(options.current_range));
    }
    return TableReader::prepare_split(options);
}

Status RemoteDorisReader::create_file_reader(std::unique_ptr<FileReader>* reader) {
    DORIS_CHECK(reader != nullptr);
    DORIS_CHECK(_file_slot_descs != nullptr);
    *reader = std::make_unique<RemoteDorisFileReader>(
            _system_properties, _current_task->data_file, _io_ctx, _scanner_profile,
            _current_file_range_desc, *_file_slot_descs, _stream_factory);
    return Status::OK();
}

} // namespace doris::format::remote_doris
