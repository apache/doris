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

#include "format_v2/arrow/arrow_stream_reader.h"

#include <arrow/array.h>
#include <arrow/buffer.h>
#include <arrow/compute/api.h>
#include <arrow/io/interfaces.h>
#include <arrow/ipc/reader.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include <utility>

#include "core/block/block.h"
#include "format/arrow/arrow_array_normalizer.h"
#include "format/arrow/arrow_row_batch.h"
#include "format/arrow/arrow_utils.h"
#include "format/parquet/arrow_memory_pool.h"
#include "format_v2/column_mapper.h"
#include "format_v2/materialized_reader_util.h"
#include "io/fs/tracing_file_reader.h"
#include "runtime/descriptors.h"
#include "runtime/file_scan_profile.h"
#include "runtime/runtime_state.h"

namespace doris::format::arrow_stream {

namespace {

std::shared_ptr<arrow::DataType> input_binding_type(
        const std::shared_ptr<arrow::DataType>& wire,
        const std::shared_ptr<arrow::DataType>& declared) {
    if (wire->Equals(declared)) {
        return declared;
    }
    // SerDe owns datetime rounding and naive/aware interpretation. Casting timestamp[us]
    // to the declared timestamp[ms] first would reject .123789 instead of rounding to .124.
    if (wire->id() == arrow::Type::TIMESTAMP && declared->id() == arrow::Type::TIMESTAMP) {
        return wire;
    }
    if (declared->id() == arrow::Type::LIST &&
        (wire->id() == arrow::Type::LIST || wire->id() == arrow::Type::LARGE_LIST ||
         wire->id() == arrow::Type::FIXED_SIZE_LIST)) {
        const auto& source = static_cast<const arrow::BaseListType&>(*wire);
        const auto& target = static_cast<const arrow::ListType&>(*declared);
        return arrow::list(target.value_field()->WithType(
                input_binding_type(source.value_type(), target.value_type())));
    }
    if (wire->id() == arrow::Type::MAP && declared->id() == arrow::Type::MAP) {
        const auto& source = static_cast<const arrow::MapType&>(*wire);
        const auto& target = static_cast<const arrow::MapType&>(*declared);
        return std::make_shared<arrow::MapType>(target.key_field()->WithType(input_binding_type(
                                                        source.key_type(), target.key_type())),
                                                target.item_field()->WithType(input_binding_type(
                                                        source.item_type(), target.item_type())),
                                                target.keys_sorted());
    }
    if (wire->id() == arrow::Type::STRUCT && declared->id() == arrow::Type::STRUCT &&
        wire->num_fields() == declared->num_fields()) {
        arrow::FieldVector fields;
        for (int i = 0; i < wire->num_fields(); ++i) {
            fields.push_back(declared->field(i)->WithType(
                    input_binding_type(wire->field(i)->type(), declared->field(i)->type())));
        }
        return arrow::struct_(fields);
    }
    return declared;
}

} // namespace

// Peek one byte between IPC streams without requesting a large buffer from a blocking load pipe.
// For example, a second schema can immediately follow the first stream's EOS marker.
class ArrowInputStream final : public arrow::io::InputStream {
public:
    ArrowInputStream(io::FileReaderSPtr reader, std::shared_ptr<io::IOContext> io_ctx,
                     arrow::MemoryPool* pool)
            : _reader(std::move(reader)), _io_ctx(std::move(io_ctx)), _pool(pool) {}

    arrow::Status Close() override {
        _reader.reset();
        return arrow::Status::OK();
    }
    bool closed() const override { return _reader == nullptr; }
    arrow::Result<int64_t> Tell() const override { return _position; }

    Status has_next(bool* has_next) {
        size_t bytes = 0;
        RETURN_IF_ERROR(_reader->read_at(_position, Slice(&_next_byte, 1), &bytes, _io_ctx.get()));
        _has_next_byte = bytes != 0;
        *has_next = _has_next_byte;
        return Status::OK();
    }

    arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
        auto* output = static_cast<char*>(out);
        int64_t total = 0;
        if (nbytes > 0 && _has_next_byte) {
            output[total++] = _next_byte;
            ++_position;
            _has_next_byte = false;
        }
        while (total < nbytes) {
            size_t bytes = 0;
            const auto status = _reader->read_at(_position, Slice(output + total, nbytes - total),
                                                 &bytes, _io_ctx.get());
            if (!status.ok()) {
                return arrow::Status::IOError(status.to_string());
            }
            if (bytes == 0) {
                break;
            }
            total += bytes;
            _position += bytes;
        }
        return total;
    }
    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
        ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes, _pool));
        ARROW_ASSIGN_OR_RAISE(auto bytes, Read(nbytes, buffer->mutable_data()));
        ARROW_RETURN_NOT_OK(buffer->Resize(bytes, false));
        buffer->ZeroPadding();
        return buffer;
    }

private:
    io::FileReaderSPtr _reader;
    std::shared_ptr<io::IOContext> _io_ctx;
    arrow::MemoryPool* _pool;
    int64_t _position = 0;
    char _next_byte = 0;
    bool _has_next_byte = false;
};

ArrowStreamReader::ArrowStreamReader(std::shared_ptr<io::FileSystemProperties>& properties,
                                     std::unique_ptr<io::FileDescription>& file,
                                     std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
                                     std::vector<SlotDescriptor*> file_slots,
                                     std::optional<TUniqueId> load_id)
        : FileReader(properties, file, std::move(io_ctx), profile),
          _file_slots(std::move(file_slots)),
          _load_id(std::move(load_id)),
          _arrow_pool(std::make_unique<ArrowMemoryPool<>>()) {}

ArrowStreamReader::~ArrowStreamReader() {
    static_cast<void>(close());
}

void ArrowStreamReader::_init_profile() {
    if (_profile == nullptr) {
        return;
    }
    file_scan_profile::ensure_hierarchy(_profile);
    _total_time = ADD_CHILD_TIMER_WITH_LEVEL(_profile, "ArrowStreamReader",
                                             file_scan_profile::FILE_READER, 1);
    _read_time =
            ADD_CHILD_TIMER_WITH_LEVEL(_profile, "ArrowStreamReadTime", "ArrowStreamReader", 1);
    _materialize_time = ADD_CHILD_TIMER_WITH_LEVEL(_profile, "ArrowStreamMaterializeTime",
                                                   "ArrowStreamReader", 1);
}

Status ArrowStreamReader::init(RuntimeState* state) {
    _state = state;
    _init_profile();
    SCOPED_TIMER(_total_time);
    if (_system_properties->system_type != TFileType::FILE_STREAM || !_load_id) {
        return Status::InvalidArgument("Arrow IPC load requires a stream load id");
    }
    for (const auto* slot : _file_slots) {
        std::shared_ptr<arrow::DataType> type;
        RETURN_IF_ERROR(DorisArrowSchemaConvertor(state->timezone())
                                .convert_to_arrow_type(slot->type(), &type));
        _arrow_types.push_back(std::move(type));
    }
    _eof = false;
    return Status::OK();
}

Status ArrowStreamReader::get_schema(std::vector<ColumnDefinition>* schema) const {
    schema->clear();
    for (size_t i = 0; i < _file_slots.size(); ++i) {
        const auto* slot = _file_slots[i];
        schema->push_back({.identifier = Field::create_field<TYPE_STRING>(slot->col_name()),
                           .local_id = cast_set<int32_t>(i),
                           .name = slot->col_name(),
                           .type = slot->type(),
                           .children = materialized_schema_children(slot->type())});
    }
    return Status::OK();
}

std::unique_ptr<TableColumnMapper> ArrowStreamReader::create_column_mapper(
        TableColumnMapperOptions options) const {
    return std::make_unique<MaterializedColumnMapper>(options);
}

Status ArrowStreamReader::open(std::shared_ptr<FileScanRequest> request) {
    RETURN_IF_ERROR(FileReader::open(std::move(request)));
    RETURN_IF_ERROR(FileFactory::create_pipe_reader(*_load_id, &_file_reader, _state, false));
    _tracing_file_reader = _io_ctx && _io_ctx->file_reader_stats
                                   ? std::make_shared<io::TracingFileReader>(
                                             _file_reader, _io_ctx->file_reader_stats)
                                   : _file_reader;
    _input = std::make_shared<ArrowInputStream>(_tracing_file_reader, _io_ctx, _arrow_pool.get());
    return Status::OK();
}

Status ArrowStreamReader::_open_next_stream() {
    bool has_next = false;
    RETURN_IF_ERROR(_input->has_next(&has_next));
    if (!has_next) {
        _eof = true;
        return Status::OK();
    }
    auto options = arrow::ipc::IpcReadOptions::Defaults();
    options.memory_pool = _arrow_pool.get();
    // Keep IPC decompression on the scanner thread, where the load memory tracker is attached.
    options.use_threads = false;
    RETURN_DORIS_STATUS_IF_RESULT_ERROR(_ipc_reader,
                                        arrow::ipc::RecordBatchStreamReader::Open(_input, options));
    _arrow_positions.assign(_file_slots.size(), -1);
    for (const auto& [id, position] : _request->local_positions) {
        const auto& name = _file_slots.at(id.value())->col_name();
        const auto indices = _ipc_reader->schema()->GetAllFieldIndices(name);
        if (indices.size() != 1) {
            return Status::InvalidArgument("Arrow stream requires exactly one column named '{}'",
                                           name);
        }
        _arrow_positions[id.value()] = indices[0];
    }
    return Status::OK();
}

Status ArrowStreamReader::_prepare_arrays() {
    _arrays.assign(_file_slots.size(), nullptr);
    for (const auto& [id, position] : _request->local_positions) {
        std::shared_ptr<arrow::Array> array;
        RETURN_IF_ERROR(normalize_arrow_array(_batch->column(_arrow_positions[id.value()]),
                                              _arrow_pool.get(), &array));
        const auto binding = input_binding_type(array->type(), _arrow_types[id.value()]);
        // Bind differing numeric widths before concrete SerDe array casts. Timestamp units
        // stay on the wire even inside containers, so Doris retains its own rounding semantics.
        if (!array->type()->Equals(binding)) {
            arrow::compute::ExecContext context(_arrow_pool.get());
            auto options = arrow::compute::CastOptions::Safe();
            // The Arrow load protocol permits binary payloads in Doris STRING columns.
            options.allow_invalid_utf8 = true;
            auto converted = arrow::compute::Cast(array, binding, options, &context);
            if (!converted.ok()) {
                return Status::InvalidArgument("Cannot decode Arrow column '{}': {}",
                                               _file_slots[id.value()]->col_name(),
                                               converted.status().ToString());
            }
            array = converted->make_array();
        }
        _arrays[id.value()] = std::move(array);
    }
    return Status::OK();
}

Status ArrowStreamReader::_next_batch() {
    SCOPED_TIMER(_read_time);
    while (!_batch || _batch_offset == _batch->num_rows()) {
        _batch.reset();
        _arrays.clear();
        _batch_offset = 0;
        if (!_ipc_reader) {
            RETURN_IF_ERROR(_open_next_stream());
            if (_eof) {
                return Status::OK();
            }
        }
        RETURN_DORIS_STATUS_IF_ERROR(_ipc_reader->ReadNext(&_batch));
        if (!_batch) {
            _ipc_reader.reset();
            continue;
        }
        RETURN_DORIS_STATUS_IF_ERROR(_batch->Validate());
        if (_batch->num_rows() != 0) {
            RETURN_IF_ERROR(_prepare_arrays());
        }
    }
    return Status::OK();
}

Status ArrowStreamReader::get_block(Block* block, size_t* rows, bool* eof) {
    SCOPED_TIMER(_total_time);
    *rows = 0;
    *eof = false;
    if (_eof || (_io_ctx && _io_ctx->should_stop)) {
        *eof = true;
        return Status::OK();
    }
    RETURN_IF_ERROR(_next_batch());
    if (_eof) {
        *eof = true;
        return Status::OK();
    }
    const int64_t end = _batch_offset +
                        std::min<int64_t>(_state->batch_size(), _batch->num_rows() - _batch_offset);
    {
        SCOPED_TIMER(_materialize_time);
        auto guard = block->mutate_columns_scoped();
        auto& columns = guard.mutable_columns();
        for (const auto& [id, position] : _request->local_positions) {
            // Preserve the original array and pass a row range: Arrow Slice creates nonzero
            // offsets, which nested Doris SerDes deliberately reject.
            RETURN_IF_ERROR(guard.get_datatype_by_position(position.value())
                                    ->get_serde()
                                    ->read_column_from_arrow(*columns.at(position.value()),
                                                             _arrays[id.value()].get(),
                                                             _batch_offset, end,
                                                             _state->timezone_obj()));
        }
    }
    *rows = cast_set<size_t>(end - _batch_offset);
    _batch_offset = end;
    _record_scan_rows(cast_set<int64_t>(*rows));
    return apply_materialized_reader_filters(_request.get(), _io_ctx.get(), block, rows);
}

Status ArrowStreamReader::close() {
    _batch.reset();
    _arrays.clear();
    _ipc_reader.reset();
    _input.reset();
    _request.reset();
    return FileReader::close();
}

} // namespace doris::format::arrow_stream
