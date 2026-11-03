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
#include <arrow/builder.h>
#include <arrow/compute/api.h>
#include <arrow/io/interfaces.h>
#include <arrow/ipc/reader.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include <utility>

#include "core/block/block.h"
#include "core/column/column_fixed_length_object.h"
#include "core/data_type/data_type_agg_state.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
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

class ArrowLoadSchemaConvertor final : public DorisArrowSchemaConvertor {
public:
    using DorisArrowSchemaConvertor::DorisArrowSchemaConvertor;

    Status convert_to_arrow_type(const DataTypePtr& origin_type,
                                 std::shared_ptr<arrow::DataType>* result) const override {
        const auto type = remove_nullable(origin_type);
        // VARIANT output may be UTF-8, but its input SerDe requires the native binary carrier,
        // including when VARIANT is nested in an ARRAY, MAP, or STRUCT.
        if (type->get_primitive_type() == TYPE_VARIANT) {
            *result = arrow::struct_({arrow::field("metadata", arrow::binary(), false),
                                      arrow::field("value", arrow::binary(), false)});
            return Status::OK();
        }
        if (type->get_primitive_type() == TYPE_AGG_STATE) {
            const auto& state = assert_cast<const DataTypeAggState&>(*type);
            const auto serialized_type = remove_nullable(state.get_serialized_type());
            // FixedLengthObject's type omits its width. The serialization column owns that
            // contract (e.g. count(INT NOT NULL) uses eight bytes), not the runtime state size.
            if (serialized_type->get_primitive_type() == TYPE_FIXED_LENGTH_OBJECT) {
                const auto column = state.create_column();
                const auto width = assert_cast<const ColumnFixedLengthObject&>(*column).item_size();
                *result = arrow::fixed_size_binary(cast_set<int32_t>(width));
                return Status::OK();
            }
            return convert_to_arrow_type(serialized_type, result);
        }
        return DorisArrowSchemaConvertor::convert_to_arrow_type(type, result);
    }
};

arrow::Result<std::shared_ptr<arrow::DataType>> input_binding_type(
        const std::shared_ptr<arrow::DataType>& wire,
        const std::shared_ptr<arrow::DataType>& declared, const DataTypePtr& doris_type) {
    if (wire->Equals(declared)) {
        return declared;
    }
    // SerDe owns fractional precision and naive/aware interpretation. Casting timestamp[us]
    // to the declared timestamp[ms] first would reject valid sub-millisecond input.
    if (wire->id() == arrow::Type::TIMESTAMP && declared->id() == arrow::Type::TIMESTAMP) {
        return wire;
    }
    // DATE64 SerDe checks whole days and the Doris year range; DATE32 rebinding would bypass it.
    if (wire->id() == arrow::Type::DATE64 && declared->id() == arrow::Type::DATE32) {
        return wire;
    }
    const auto type = remove_nullable(get_serialized_type(remove_nullable(doris_type)));
    // A native VARIANT carrier is a logical leaf, not a Doris STRUCT. Preserve its storage or
    // extension unchanged and let its SerDe validate the carrier shape and encoded payload.
    if (type->get_primitive_type() == TYPE_VARIANT) {
        return wire;
    }
    // DECIMALV2 SerDe rescales decimal128 input to scale nine, truncating extra fractional
    // digits toward zero (1.2345678901 -> 1.234567890). Other decimals retain checked casts.
    if (type->get_primitive_type() == TYPE_DECIMALV2 && wire->id() == arrow::Type::DECIMAL128) {
        return wire;
    }
    // IPv4 is a 32-bit bit pattern, including addresses above INT32_MAX. Numeric INT columns
    // still use the checked conversion below instead of inheriting this storage exception.
    if (type->get_primitive_type() == TYPE_IPV4 && wire->id() == arrow::Type::UINT32) {
        return wire;
    }
    if (declared->id() == arrow::Type::LIST &&
        (wire->id() == arrow::Type::LIST || wire->id() == arrow::Type::LARGE_LIST ||
         wire->id() == arrow::Type::FIXED_SIZE_LIST)) {
        const auto& source = static_cast<const arrow::BaseListType&>(*wire);
        const auto& target = static_cast<const arrow::ListType&>(*declared);
        const auto& array = assert_cast<const DataTypeArray&>(*type);
        ARROW_ASSIGN_OR_RAISE(auto child,
                              input_binding_type(source.value_type(), target.value_type(),
                                                 array.get_nested_type()));
        return arrow::list(source.value_field()->WithType(child));
    }
    if (wire->id() == arrow::Type::MAP && declared->id() == arrow::Type::MAP) {
        const auto& source = static_cast<const arrow::MapType&>(*wire);
        const auto& target = static_cast<const arrow::MapType&>(*declared);
        const auto& map = assert_cast<const DataTypeMap&>(*type);
        ARROW_ASSIGN_OR_RAISE(auto key, input_binding_type(source.key_type(), target.key_type(),
                                                           map.get_key_type()));
        ARROW_ASSIGN_OR_RAISE(auto item, input_binding_type(source.item_type(), target.item_type(),
                                                            map.get_value_type()));
        return std::make_shared<arrow::MapType>(source.key_field()->WithType(key),
                                                source.item_field()->WithType(item),
                                                source.keys_sorted());
    }
    if (wire->id() == arrow::Type::STRUCT && declared->id() == arrow::Type::STRUCT) {
        if (wire->num_fields() != declared->num_fields()) {
            return arrow::Status::Invalid(
                    "Arrow STRUCT field count does not match the load schema");
        }
        const auto& structure = assert_cast<const DataTypeStruct&>(*type);
        arrow::FieldVector fields;
        for (int i = 0; i < wire->num_fields(); ++i) {
            ARROW_ASSIGN_OR_RAISE(auto child, input_binding_type(wire->field(i)->type(),
                                                                 declared->field(i)->type(),
                                                                 structure.get_elements()[i]));
            // Struct SerDe binds children by position. Arrow casts bind by name, so retain the
            // wire names to prevent a case/name difference from synthesizing NULL children.
            fields.push_back(wire->field(i)->WithType(child));
        }
        return arrow::struct_(fields);
    }
    return declared;
}

arrow::Result<std::shared_ptr<arrow::Array>> mask_hidden_children(
        std::shared_ptr<arrow::Array> array, const std::shared_ptr<arrow::DataType>& binding,
        arrow::compute::ExecContext* context);

arrow::Result<std::shared_ptr<arrow::Array>> mask_struct_children(
        const std::shared_ptr<arrow::Array>& array, const std::shared_ptr<arrow::DataType>& binding,
        arrow::compute::ExecContext* context) {
    const auto& structure = static_cast<const arrow::StructArray&>(*array);
    auto data = array->data()->Copy();
    for (int i = 0; i < structure.num_fields(); ++i) {
        // A NULL struct can have valid child payloads. Combine ancestor validity before
        // checked casts can inspect those logically hidden values.
        ARROW_ASSIGN_OR_RAISE(auto child, structure.GetFlattenedField(i, context->memory_pool()));
        ARROW_ASSIGN_OR_RAISE(
                child, mask_hidden_children(std::move(child), binding->field(i)->type(), context));
        data->child_data[i] = child->data();
    }
    return arrow::MakeArray(data);
}

arrow::Result<std::shared_ptr<arrow::Array>> fixed_list_as_large_list(
        const arrow::FixedSizeListArray& list, const std::shared_ptr<arrow::Buffer>& validity,
        arrow::MemoryPool* pool) {
    arrow::Int64Builder offsets(pool);
    ARROW_RETURN_NOT_OK(offsets.Reserve(list.length() + 1));
    for (int64_t i = 0; i <= list.length(); ++i) {
        ARROW_RETURN_NOT_OK(offsets.Append(list.value_offset(i)));
    }
    ARROW_ASSIGN_OR_RAISE(auto values, offsets.Finish());
    return std::make_shared<arrow::LargeListArray>(
            arrow::large_list(list.list_type()->value_field()), list.length(),
            values->data()->buffers[1], list.values(), validity, list.null_count());
}

arrow::Result<std::shared_ptr<arrow::Array>> compact_null_containers(
        std::shared_ptr<arrow::Array> array, arrow::compute::ExecContext* context) {
    if (array->null_count() == 0) {
        return array;
    }
    // NULL lists/maps can reference nonempty child ranges. Take drops those ranges without
    // making map keys or other non-nullable descendants nullable.
    arrow::Int64Builder indices(context->memory_pool());
    ARROW_RETURN_NOT_OK(indices.Reserve(array->length()));
    for (int64_t i = 0; i < array->length(); ++i) {
        ARROW_RETURN_NOT_OK(array->IsNull(i) ? indices.AppendNull() : indices.Append(i));
    }
    ARROW_ASSIGN_OR_RAISE(auto positions, indices.Finish());
    // Fixed-list Take propagates a NULL index into every child, even non-nullable STRUCT
    // fields such as a:INT NOT NULL. View it as a variable list first so Take drops the whole
    // hidden range instead. Keep 64-bit offsets until the final checked LIST cast.
    if (array->type_id() == arrow::Type::FIXED_SIZE_LIST) {
        ARROW_ASSIGN_OR_RAISE(array, fixed_list_as_large_list(
                                             static_cast<const arrow::FixedSizeListArray&>(*array),
                                             positions->null_bitmap(), context->memory_pool()));
    }
    ARROW_ASSIGN_OR_RAISE(
            auto compacted,
            arrow::compute::Take(array, positions, arrow::compute::TakeOptions::NoBoundsCheck(),
                                 context));
    return compacted.make_array();
}

arrow::Result<std::shared_ptr<arrow::Array>> mask_list_children(
        std::shared_ptr<arrow::Array> array, const std::shared_ptr<arrow::DataType>& binding,
        arrow::compute::ExecContext* context) {
    ARROW_ASSIGN_OR_RAISE(array, compact_null_containers(std::move(array), context));
    auto data = array->data()->Copy();
    ARROW_ASSIGN_OR_RAISE(auto child, mask_hidden_children(arrow::MakeArray(data->child_data[0]),
                                                           binding->field(0)->type(), context));
    data->child_data[0] = child->data();
    return arrow::MakeArray(data);
}

arrow::Result<std::shared_ptr<arrow::Array>> mask_hidden_children(
        std::shared_ptr<arrow::Array> array, const std::shared_ptr<arrow::DataType>& binding,
        arrow::compute::ExecContext* context) {
    // Only conversions need hidden payloads masked. Descending into an unchanged VARIANT
    // carrier would make its non-nullable metadata/value fields NULL under a SQL NULL parent,
    // and Arrow's otherwise identical struct view would reject those synthetic child NULLs.
    if (array->type()->Equals(binding)) {
        return array;
    }
    const bool converts_list_offsets = binding->id() == arrow::Type::LIST &&
                                       (array->type_id() == arrow::Type::LARGE_LIST ||
                                        array->type_id() == arrow::Type::FIXED_SIZE_LIST);
    // A STRUCT sent for a STRING slot has no corresponding target children. Leave mismatched
    // container kinds to Arrow's cast error rather than indexing nonexistent binding fields.
    if (array->type_id() != binding->id() && !converts_list_offsets) {
        return array;
    }
    switch (array->type_id()) {
    case arrow::Type::STRUCT:
        return mask_struct_children(array, binding, context);
    case arrow::Type::LIST:
    case arrow::Type::LARGE_LIST:
    case arrow::Type::FIXED_SIZE_LIST:
    case arrow::Type::MAP:
        return mask_list_children(std::move(array), binding, context);
    default:
        return array;
    }
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
        RETURN_IF_ERROR(ArrowLoadSchemaConvertor(state->timezone())
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
        arrow::compute::ExecContext context(_arrow_pool.get());
        auto binding_result = input_binding_type(array->type(), _arrow_types[id.value()],
                                                 _file_slots[id.value()]->type());
        // A STRUCT field-count mismatch needs the same column context as a failed value cast.
        if (!binding_result.ok()) {
            return Status::InvalidArgument(
                    "Cannot decode Arrow column '{}' ({} -> {}): {}",
                    _file_slots[id.value()]->col_name(), array->type()->ToString(),
                    _arrow_types[id.value()]->ToString(), binding_result.status().ToString());
        }
        auto binding = std::move(binding_result).ValueUnsafe();
        // Bind differing numeric widths before concrete SerDe array casts. Timestamp units
        // stay on the wire even inside containers, so Doris retains its own rounding semantics.
        if (!array->type()->Equals(binding)) {
            RETURN_DORIS_STATUS_IF_RESULT_ERROR(
                    array, mask_hidden_children(std::move(array), binding, &context));
            auto options = arrow::compute::CastOptions::Safe();
            // The Arrow load protocol permits binary payloads in Doris STRING columns.
            options.allow_invalid_utf8 = true;
            auto converted = arrow::compute::Cast(array, binding, options, &context);
            if (!converted.ok()) {
                return Status::InvalidArgument("Cannot decode Arrow column '{}' ({} -> {}): {}",
                                               _file_slots[id.value()]->col_name(),
                                               array->type()->ToString(), binding->ToString(),
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
