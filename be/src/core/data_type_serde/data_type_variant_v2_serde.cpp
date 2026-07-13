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

#include "core/data_type_serde/data_type_variant_v2_serde.h"

#include "common/exception.h"

namespace doris {
namespace {

using data_type_variant_v2_serde_internal::CountingWriter;
using data_type_variant_v2_serde_internal::ReadInput;
using data_type_variant_v2_serde_internal::checked_row;
using data_type_variant_v2_serde_internal::for_each_value;
using data_type_variant_v2_serde_internal::write_json_value;

void preflight_json(const ReadInput& input, size_t start, size_t end,
                    const DataTypeSerDe::FormatOptions& options) {
    for_each_value(
            input, start, end, nullptr, [](size_t) {},
            [&](size_t, VariantValueRef value) {
                CountingWriter writer;
                write_json_value(value, writer, options);
            });
}

} // namespace

DataTypeVariantV2SerDe::DataTypeVariantV2SerDe(int nesting_level) : DataTypeSerDe(nesting_level) {}

std::string DataTypeVariantV2SerDe::get_name() const {
    return "Variant";
}

Status DataTypeVariantV2SerDe::serialize_one_cell_to_json(const IColumn& column, int64_t row_num,
                                                          BufferWritable& bw,
                                                          FormatOptions& options) const {
    RETURN_IF_CATCH_EXCEPTION({
        const ReadInput input(column);
        const size_t row = checked_row(row_num);
        preflight_json(input, row, row + 1, options);
        for_each_value(
                input, row, row + 1, nullptr, [](size_t) {},
                [&](size_t, VariantValueRef value) { write_json_value(value, bw, options); });
    });
    return Status::OK();
}

Status DataTypeVariantV2SerDe::serialize_column_to_json(const IColumn& column, int64_t start_idx,
                                                        int64_t end_idx, BufferWritable& bw,
                                                        FormatOptions& options) const {
    RETURN_IF_CATCH_EXCEPTION({
        const ReadInput input(column);
        const size_t start = checked_row(start_idx);
        const size_t end = checked_row(end_idx);
        preflight_json(input, start, end, options);
        for_each_value(
                input, start, end, nullptr, [](size_t) {},
                [&](size_t row, VariantValueRef value) {
                    if (row != start) {
                        bw.write(options.field_delim.data(), options.field_delim.size());
                    }
                    write_json_value(value, bw, options);
                });
    });
    return Status::OK();
}

Status DataTypeVariantV2SerDe::write_column_to_pb(const IColumn& column, PValues&, int64_t,
                                                  int64_t) const {
    return Status::NotSupported("write_column_to_pb with type " + column.get_name());
}

Status DataTypeVariantV2SerDe::read_column_from_pb(IColumn& column, const PValues&) const {
    return Status::NotSupported("read_column_from_pb with type " + column.get_name());
}

Status DataTypeVariantV2SerDe::read_column_from_arrow(IColumn& column, const arrow::Array*, int64_t,
                                                      int64_t, const cctz::time_zone&) const {
    return Status::Error(ErrorCode::NOT_IMPLEMENTED_ERROR,
                         "read_column_from_arrow with type " + column.get_name());
}

} // namespace doris
#include <limits>

#include "common/cast_set.h"
#include "common/exception.h"
#include "core/column/column_variant_v2.h"
#include "core/data_type_serde/data_type_string_serde.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "core/value/jsonb_value.h"
#include "util/jsonb_writer.h"
#include "util/variant/variant_block_builder.h"
#include "util/variant/variant_jsonb.h"

namespace doris {
namespace {

ColumnVariantV2& destination(IColumn& column) {
    auto* result = check_and_get_column<ColumnVariantV2>(column);
    if (result == nullptr) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant V2 SerDe destination requires ColumnVariantV2, got {}",
                        column.get_name());
    }
    return *result;
}

void require_jsonb_write(bool written, const char* operation) {
    if (!written) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "JSONB writer rejected {}", operation);
    }
}

} // namespace

Status DataTypeVariantV2SerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                              const FormatOptions&) const {
    RETURN_IF_CATCH_EXCEPTION({
        JsonToVariantEncoder encoder;
        encoder.add_json({slice.data, slice.size});
        VariantEncodedBlock block = encoder.finish_block();
        destination(column).insert_encoded_block(block.view());
    });
    return Status::OK();
}

Status DataTypeVariantV2SerDe::deserialize_one_cell_from_csv(IColumn& column, Slice& slice,
                                                             const FormatOptions& options) const {
    RETURN_IF_CATCH_EXCEPTION({
        if (slice.size != 0 && slice.data == nullptr) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant CSV input has a null data pointer");
        }
        if (slice.size != 0 && options.escape_char != 0) {
            escape_string_for_csv(slice.data, &slice.size, options.escape_char, options.quote_char);
        }
        VariantBlockBuilder builder(VariantBlockBuilder::ReserveHint {.rows = 1});
        auto row = builder.begin_row();
        row.add_string({slice.data, slice.size});
        row.finish();
        VariantEncodedBlock block = builder.finish_block();
        destination(column).insert_encoded_block(block.view());
    });
    return Status::OK();
}

Status DataTypeVariantV2SerDe::deserialize_column_from_json_vector(IColumn& column,
                                                                   std::vector<Slice>& slices,
                                                                   uint64_t* num_deserialized,
                                                                   const FormatOptions&) const {
    if (num_deserialized == nullptr) {
        return Status::InvalidArgument("Variant JSON deserialized counter is null");
    }
    if (slices.size() > std::numeric_limits<uint64_t>::max() - *num_deserialized) {
        return Status::InvalidArgument("Variant JSON deserialized counter overflows");
    }
    RETURN_IF_CATCH_EXCEPTION({
        ColumnVariantV2& result = destination(column);
        if (slices.empty()) {
            return Status::OK();
        }
        JsonToVariantEncoder encoder;
        for (const Slice& slice : slices) {
            encoder.add_json({slice.data, slice.size});
        }
        VariantEncodedBlock block = encoder.finish_block();
        result.insert_encoded_block(block.view());
        *num_deserialized += slices.size();
    });
    return Status::OK();
}

void DataTypeVariantV2SerDe::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
                                                     Arena&, int32_t col_id, int64_t row_num,
                                                     const FormatOptions& options) const {
    const data_type_variant_v2_serde_internal::ReadInput input(column);
    const size_t row = data_type_variant_v2_serde_internal::checked_row(row_num);
    JsonbWriter document;
    data_type_variant_v2_serde_internal::for_each_value(
            input, row, row + 1, nullptr, [](size_t) {},
            [&](size_t, VariantValueRef value) {
                variant_to_jsonb(value, document, {.timezone = options.timezone});
            });
    // DataTypeNullableSerDe pre-writes the key for a non-null nested value. A direct row-store call
    // does not. writeKey() is side-effect free when the writer is already waiting for a value, so
    // attempting it supports both call shapes; writeStartBinary() remains the state validation.
    static_cast<void>(result.writeKey(cast_set<JsonbKeyValue::keyid_type>(col_id)));
    require_jsonb_write(result.writeStartBinary(), "binary start");
    require_jsonb_write(
            result.writeBinary(document.getOutput()->getBuffer(), document.getOutput()->getSize()),
            "binary payload");
    require_jsonb_write(result.writeEndBinary(), "binary end");
}

void DataTypeVariantV2SerDe::read_one_cell_from_jsonb(IColumn& column,
                                                      const JsonbValue* arg) const {
    if (arg == nullptr || !arg->isBinary()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant V2 row-store value must be a binary JSONB document");
    }
    const auto* binary = arg->unpack<JsonbBinaryVal>();
    JsonbToVariantEncoder encoder;
    encoder.add_jsonb({binary->getBlob(), binary->getBlobLen()});
    VariantEncodedBlock block = encoder.finish_block();
    destination(column).insert_encoded_block(block.view());
}

} // namespace doris
#include <arrow/array/builder_binary.h>

#include <algorithm>
#include <limits>
#include <orc/Vector.hh>

#include "common/cast_set.h"
#include "common/exception.h"
#include "core/arena.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "util/mysql_row_buffer.h"

namespace doris {
namespace {

using data_type_variant_v2_serde_internal::CountingWriter;
using data_type_variant_v2_serde_internal::FixedWriter;
using data_type_variant_v2_serde_internal::ReadInput;
using data_type_variant_v2_serde_internal::checked_row;
using data_type_variant_v2_serde_internal::for_each_value;
using data_type_variant_v2_serde_internal::write_json_value;

DorisVector<size_t> json_lengths(const ReadInput& input, size_t start, size_t end,
                                 const NullMap* null_map,
                                 const DataTypeSerDe::FormatOptions& options) {
    input.validate_range(start, end);
    DorisVector<size_t> lengths(end - start, 0);
    for_each_value(
            input, start, end, null_map, [](size_t) {},
            [&](size_t row, VariantValueRef value) {
                CountingWriter writer;
                write_json_value(value, writer, options);
                lengths[row - start] = writer.count;
            });
    return lengths;
}

template <typename Builder>
Status write_arrow(const IColumn& column, const ReadInput& input, const NullMap* null_map,
                   Builder& builder, size_t start, size_t end,
                   const DataTypeSerDe::FormatOptions& options) {
    const DorisVector<size_t> lengths = json_lengths(input, start, end, null_map, options);
    const size_t maximum = lengths.empty() ? 0 : *std::ranges::max_element(lengths);
    if (maximum > static_cast<size_t>(std::numeric_limits<typename Builder::offset_type>::max())) {
        return Status::InvalidArgument("Variant JSON value exceeds Arrow offset range");
    }
    DorisVector<char> rendered(maximum);
    Status status = Status::OK();
    for_each_value(
            input, start, end, null_map,
            [&](size_t) {
                if (status.ok()) {
                    status = checkArrowStatus(builder.AppendNull(), column, builder);
                }
            },
            [&](size_t row, VariantValueRef value) {
                if (!status.ok()) {
                    return;
                }
                FixedWriter writer {.destination = rendered.data(),
                                    .capacity = lengths[row - start]};
                write_json_value(value, writer, options);
                DCHECK_EQ(writer.written, lengths[row - start]);
                status = checkArrowStatus(
                        builder.Append(rendered.data(),
                                       cast_set<typename Builder::offset_type>(writer.written)),
                        column, builder);
            });
    return status;
}

} // namespace

void DataTypeVariantV2SerDe::to_string(const IColumn& column, size_t row_num, BufferWritable& bw,
                                       const FormatOptions& options) const {
    const ReadInput input(column);
    const DorisVector<size_t> lengths = json_lengths(input, row_num, row_num + 1, nullptr, options);
    DCHECK_EQ(lengths.size(), 1);
    for_each_value(
            input, row_num, row_num + 1, nullptr, [](size_t) {},
            [&](size_t, VariantValueRef value) { write_json_value(value, bw, options); });
}

Status DataTypeVariantV2SerDe::write_column_to_mysql_binary(const IColumn& column,
                                                            MysqlRowBinaryBuffer& row_buffer,
                                                            int64_t row_idx, bool col_const,
                                                            const FormatOptions& options) const {
    RETURN_IF_CATCH_EXCEPTION({
        const ReadInput input(column);
        const size_t row = col_const ? 0 : checked_row(row_idx);
        const DorisVector<size_t> lengths = json_lengths(input, row, row + 1, nullptr, options);
        DorisVector<char> rendered(lengths[0]);
        for_each_value(
                input, row, row + 1, nullptr, [](size_t) {},
                [&](size_t, VariantValueRef value) {
                    FixedWriter writer {.destination = rendered.data(),
                                        .capacity = rendered.size()};
                    write_json_value(value, writer, options);
                    DCHECK_EQ(writer.written, rendered.size());
                });
        if (row_buffer.push_string(rendered.data(), rendered.size()) != 0) {
            throw Exception(ErrorCode::INTERNAL_ERROR, "Failed to pack Variant MySQL buffer");
        }
    });
    return Status::OK();
}

Status DataTypeVariantV2SerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                     arrow::ArrayBuilder* array_builder,
                                                     int64_t start, int64_t end,
                                                     const cctz::time_zone& ctz) const {
    if (array_builder == nullptr) {
        return Status::InvalidArgument("Variant Arrow builder is null");
    }
    RETURN_IF_CATCH_EXCEPTION({
        const ReadInput input(column);
        FormatOptions options;
        options.timezone = &ctz;
        const size_t first = checked_row(start);
        const size_t last = checked_row(end);
        if (array_builder->type()->id() == arrow::Type::STRING) {
            return write_arrow(column, input, null_map,
                               assert_cast<arrow::StringBuilder&>(*array_builder), first, last,
                               options);
        }
        if (array_builder->type()->id() == arrow::Type::LARGE_STRING) {
            return write_arrow(column, input, null_map,
                               assert_cast<arrow::LargeStringBuilder&>(*array_builder), first, last,
                               options);
        }
        return Status::InvalidArgument("Unsupported arrow type for variant column: {}",
                                       array_builder->type()->name());
    });
    return Status::OK();
}

Status DataTypeVariantV2SerDe::write_column_to_orc(const std::string&, const IColumn& column,
                                                   const NullMap* null_map,
                                                   orc::ColumnVectorBatch* orc_col_batch,
                                                   int64_t start, int64_t end, Arena& arena,
                                                   const FormatOptions& options) const {
    auto* batch = dynamic_cast<orc::StringVectorBatch*>(orc_col_batch);
    if (batch == nullptr) {
        return Status::InvalidArgument("Variant ORC output requires StringVectorBatch");
    }
    RETURN_IF_CATCH_EXCEPTION({
        const ReadInput input(column);
        const size_t first = checked_row(start);
        const size_t last = checked_row(end);
        const DorisVector<size_t> lengths = json_lengths(input, first, last, null_map, options);
        size_t total_size = 0;
        for (size_t length : lengths) {
            if (length > std::numeric_limits<size_t>::max() - total_size) {
                throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant ORC output is too large");
            }
            total_size += length;
        }
        char* output = total_size == 0 ? nullptr : arena.alloc(total_size);
        size_t offset = 0;
        batch->hasNulls = null_map != nullptr;
        for_each_value(
                input, first, last, null_map,
                [&](size_t row) {
                    batch->notNull[row] = 0;
                    batch->data[row] = nullptr;
                    batch->length[row] = 0;
                },
                [&](size_t row, VariantValueRef value) {
                    batch->notNull[row] = 1;
                    batch->data[row] = output + offset;
                    FixedWriter writer {.destination = output + offset,
                                        .capacity = lengths[row - first]};
                    write_json_value(value, writer, options);
                    DCHECK_EQ(writer.written, lengths[row - first]);
                    batch->length[row] = cast_set<int64_t>(writer.written);
                    offset += writer.written;
                });
        DCHECK_EQ(offset, total_size);
        batch->numElements = last - first;
    });
    return Status::OK();
}

} // namespace doris
