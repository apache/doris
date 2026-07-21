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

#include <arrow/array/builder_binary.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <orc/Vector.hh>
#include <utility>

#include "common/cast_set.h"
#include "common/exception.h"
#include "core/arena.h"
#include "core/assert_cast.h"
#include "core/column/column_const.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/column/variant_v2/column_variant_v2_typed_column.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type_serde/data_type_string_serde.h"
#include "core/types.h"
#include "core/value/jsonb_value.h"
#include "core/value/variant/variant_batch_builder.h"
#include "exprs/function/parse/variant_jsonb_parse.h"
#include "util/jsonb_writer.h"
#include "util/mysql_row_buffer.h"

namespace doris {
namespace {

using MetaIdsColumn = ColumnVector<TYPE_UINT32>;

using data_type_variant_v2_serde_internal::CountingWriter;
using data_type_variant_v2_serde_internal::checked_row;
using data_type_variant_v2_serde_internal::visit_variant_values;
using data_type_variant_v2_serde_internal::write_json_value;

constexpr size_t VARIANT_V2_TYPE_META_BYTES = sizeof(int32_t) * 4;

const ColumnVariantV2& get_variant_v2_column(const IColumn& column) {
    const IColumn* physical = &column;
    if (const auto* constant = check_and_get_column<ColumnConst>(column)) {
        physical = &constant->get_data_column();
    }
    return assert_cast<const ColumnVariantV2&>(*physical);
}

void write_variant_v2_type(const DataTypePtr& type, char*& buf) {
    uint32_t precision = type->get_precision();
    uint32_t scale = type->get_scale();
    if (type->get_primitive_type() == TYPE_DECIMALV2) {
        const auto& decimal = assert_cast<const DataTypeDecimalV2&>(*type);
        precision = decimal.get_original_precision();
        scale = decimal.get_original_scale();
    }
    const int32_t length = is_string_type(type->get_primitive_type())
                                   ? assert_cast<const DataTypeString&>(*type).len()
                                   : -1;
    unaligned_store<int32_t>(buf, static_cast<int32_t>(type->get_primitive_type()));
    buf += sizeof(int32_t);
    unaligned_store<uint32_t>(buf, precision);
    buf += sizeof(uint32_t);
    unaligned_store<uint32_t>(buf, scale);
    buf += sizeof(uint32_t);
    unaligned_store<int32_t>(buf, length);
    buf += sizeof(int32_t);
}

DataTypePtr read_variant_v2_type(const char*& buf) {
    const auto primitive = static_cast<PrimitiveType>(unaligned_load<int32_t>(buf));
    buf += sizeof(int32_t);
    const auto precision = unaligned_load<uint32_t>(buf);
    buf += sizeof(uint32_t);
    const auto scale = unaligned_load<uint32_t>(buf);
    buf += sizeof(uint32_t);
    const auto length = unaligned_load<int32_t>(buf);
    buf += sizeof(int32_t);
    if (!column_variant_v2_internal::is_supported_typed_identity(primitive)) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "Unsupported Variant V2 typed identity {}",
                        static_cast<int32_t>(primitive));
    }
    return DataTypeFactory::instance().create_data_type(primitive, false, precision, scale, length);
}

char* serialize_meta_ids(const IColumn& column, char* buf) {
    const auto& ids = assert_cast<const MetaIdsColumn&>(column).get_data();
    unaligned_store<size_t>(buf, ids.size());
    buf += sizeof(size_t);
    const size_t bytes = ids.size() * sizeof(uint32_t);
    if (bytes != 0) {
        std::memcpy(buf, ids.data(), bytes);
    }
    return buf + bytes;
}

const char* deserialize_meta_ids(const char* buf, MutableColumnPtr* column) {
    const auto size = unaligned_load<size_t>(buf);
    buf += sizeof(size_t);
    auto& ids = assert_cast<MetaIdsColumn&>(**column).get_data();
    ids.resize(size);
    const size_t bytes = size * sizeof(uint32_t);
    if (bytes != 0) {
        std::memcpy(ids.data(), buf, bytes);
    }
    return buf + bytes;
}

void preflight_json(const IColumn& column, size_t start, size_t end,
                    const DataTypeSerDe::FormatOptions& options) {
    visit_variant_values(
            column, start, end, {}, [](size_t) {},
            [&](size_t, VariantRef value) {
                CountingWriter writer;
                write_json_value(value, writer, options);
            });
}

} // namespace

DataTypeVariantV2SerDe::DataTypeVariantV2SerDe(int nesting_level) : DataTypeSerDe(nesting_level) {}

int64_t DataTypeVariantV2SerDe::get_uncompressed_serialized_bytes(const IColumn& column,
                                                                  int be_exec_version) {
    const auto& variant = get_variant_v2_column(column);
    int64_t size = sizeof(bool) + sizeof(size_t) * 2 + sizeof(bool);
    if (variant.is_typed()) {
        const DataTypePtr nullable_type = make_nullable(variant._typed_type);
        return size + VARIANT_V2_TYPE_META_BYTES +
               nullable_type->get_uncompressed_serialized_bytes(*variant._typed, be_exec_version);
    }
    const DataTypeString string_type;
    size += string_type.get_uncompressed_serialized_bytes(*variant._metadatas, be_exec_version);
    size += sizeof(size_t) + variant._meta_ids->size() * sizeof(uint32_t);
    size += string_type.get_uncompressed_serialized_bytes(*variant._values, be_exec_version);
    return size;
}

char* DataTypeVariantV2SerDe::serialize(const IColumn& column, char* buf, int be_exec_version) {
    const IColumn* physical = &column;
    size_t saved_rows = 0;
    buf = serialize_const_flag_and_row_num(&physical, buf, &saved_rows);
    const auto& variant = assert_cast<const ColumnVariantV2&>(*physical);
    DCHECK_EQ(variant.size(), saved_rows);
    unaligned_store<bool>(buf, variant.is_typed());
    buf += sizeof(bool);
    if (variant.is_typed()) {
        write_variant_v2_type(variant._typed_type, buf);
        return make_nullable(variant._typed_type)->serialize(*variant._typed, buf, be_exec_version);
    }
    const DataTypeString string_type;
    buf = string_type.serialize(*variant._metadatas, buf, be_exec_version);
    buf = serialize_meta_ids(*variant._meta_ids, buf);
    return string_type.serialize(*variant._values, buf, be_exec_version);
}

const char* DataTypeVariantV2SerDe::deserialize(const char* buf, MutableColumnPtr* column,
                                                int be_exec_version) {
    auto* destination = assert_cast<ColumnVariantV2*>(column->get());
    size_t saved_rows = 0;
    buf = deserialize_const_flag_and_row_num(buf, column, &saved_rows);
    const bool typed = unaligned_load<bool>(buf);
    buf += sizeof(bool);

    ColumnVariantV2::MutablePtr decoded;
    if (typed) {
        DataTypePtr type = read_variant_v2_type(buf);
        const DataTypePtr nullable_type = make_nullable(type);
        MutableColumnPtr typed_column = nullable_type->create_column();
        buf = nullable_type->deserialize(buf, &typed_column, be_exec_version);
        decoded = ColumnVariantV2::create_typed(std::move(typed_column), std::move(type));
    } else {
        const DataTypeString string_type;
        MutableColumnPtr metadatas = string_type.create_column();
        MutableColumnPtr meta_ids = MetaIdsColumn::create();
        MutableColumnPtr values = string_type.create_column();
        buf = string_type.deserialize(buf, &metadatas, be_exec_version);
        buf = deserialize_meta_ids(buf, &meta_ids);
        buf = string_type.deserialize(buf, &values, be_exec_version);

        const auto& ids = assert_cast<const MetaIdsColumn&>(*meta_ids).get_data();
        if (ids.size() != values->size()) {
            throw Exception(Status::Corruption(
                    "ColumnVariantV2 metadata id count {} does not match value count {}",
                    ids.size(), values->size()));
        }
        for (uint32_t id : ids) {
            if (id >= metadatas->size()) {
                throw Exception(Status::Corruption(
                        "ColumnVariantV2 metadata id {} exceeds metadata count {}", id,
                        metadatas->size()));
            }
        }
        decoded = ColumnVariantV2::create();
        static_cast<IColumn::Ptr&>(decoded->_metadatas) = std::move(metadatas);
        static_cast<IColumn::Ptr&>(decoded->_meta_ids) = std::move(meta_ids);
        static_cast<IColumn::Ptr&>(decoded->_values) = std::move(values);
        decoded->sanity_check();
    }
    if (decoded->size() != saved_rows) {
        throw Exception(Status::Corruption(
                "ColumnVariantV2 saved row count {} does not match decoded row count {}",
                saved_rows, decoded->size()));
    }
    destination->_adopt_state_from(*decoded);
    return buf;
}

std::string DataTypeVariantV2SerDe::get_name() const {
    return "Variant";
}

Status DataTypeVariantV2SerDe::serialize_one_cell_to_json(const IColumn& column, int64_t row_num,
                                                          BufferWritable& bw,
                                                          FormatOptions& options) const {
    RETURN_IF_CATCH_EXCEPTION({
        const size_t row = checked_row(row_num);
        preflight_json(column, row, row + 1, options);
        visit_variant_values(
                column, row, row + 1, {}, [](size_t) {},
                [&](size_t, VariantRef value) { write_json_value(value, bw, options); });
    });
    return Status::OK();
}

Status DataTypeVariantV2SerDe::serialize_column_to_json(const IColumn& column, int64_t start_idx,
                                                        int64_t end_idx, BufferWritable& bw,
                                                        FormatOptions& options) const {
    RETURN_IF_CATCH_EXCEPTION({
        const size_t start = checked_row(start_idx);
        const size_t end = checked_row(end_idx);
        preflight_json(column, start, end, options);
        visit_variant_values(
                column, start, end, {}, [](size_t) {},
                [&](size_t row, VariantRef value) {
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
        JsonStringToVariantEncoder encoder;
        encoder.add_json({slice.data, slice.size});
        VariantBatchBuilder block = encoder.finish_batch();
        destination(column).insert_encoded_batch(block);
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
        VariantBatchBuilder builder(VariantBatchBuilder::ReserveHint {.rows = 1});
        auto row = builder.begin_row();
        row.add_string({slice.data, slice.size});
        row.finish();
        VariantBatchBuilder block = builder.finish_batch();
        destination(column).insert_encoded_batch(block);
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
        JsonStringToVariantEncoder encoder;
        for (const Slice& slice : slices) {
            encoder.add_json({slice.data, slice.size});
        }
        VariantBatchBuilder block = encoder.finish_batch();
        result.insert_encoded_batch(block);
        *num_deserialized += slices.size();
    });
    return Status::OK();
}

void DataTypeVariantV2SerDe::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
                                                     Arena&, int32_t col_id, int64_t row_num,
                                                     const FormatOptions& options) const {
    const size_t row = data_type_variant_v2_serde_internal::checked_row(row_num);
    JsonbWriter document;
    data_type_variant_v2_serde_internal::visit_variant_values(
            column, row, row + 1, {}, [](size_t) {},
            [&](size_t, VariantRef value) {
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
    VariantBatchBuilder block = encoder.finish_batch();
    destination(column).insert_encoded_batch(block);
}

namespace {

using data_type_variant_v2_serde_internal::CountingWriter;
using data_type_variant_v2_serde_internal::FixedWriter;
using data_type_variant_v2_serde_internal::checked_row;
using data_type_variant_v2_serde_internal::forced_nulls;
using data_type_variant_v2_serde_internal::visit_variant_values;
using data_type_variant_v2_serde_internal::write_json_value;

DorisVector<size_t> json_lengths(const IColumn& column, size_t start, size_t end,
                                 const NullMap* null_map,
                                 const DataTypeSerDe::FormatOptions& options) {
    if (start > end || end > column.size()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant row range [{}, {}) exceeds column size {}", start, end,
                        column.size());
    }
    DorisVector<size_t> lengths(end - start, 0);
    visit_variant_values(
            column, start, end, forced_nulls(null_map), [](size_t) {},
            [&](size_t row, VariantRef value) {
                CountingWriter writer;
                write_json_value(value, writer, options);
                lengths[row - start] = writer.count;
            });
    return lengths;
}

template <typename Builder>
Status write_arrow(const IColumn& column, const NullMap* null_map, Builder& builder, size_t start,
                   size_t end, const DataTypeSerDe::FormatOptions& options) {
    const DorisVector<size_t> lengths = json_lengths(column, start, end, null_map, options);
    const size_t maximum = lengths.empty() ? 0 : *std::ranges::max_element(lengths);
    if (maximum > static_cast<size_t>(std::numeric_limits<typename Builder::offset_type>::max())) {
        return Status::InvalidArgument("Variant JSON value exceeds Arrow offset range");
    }
    DorisVector<char> rendered(maximum);
    Status status = Status::OK();
    visit_variant_values(
            column, start, end, forced_nulls(null_map),
            [&](size_t) {
                if (status.ok()) {
                    status = checkArrowStatus(builder.AppendNull(), column, builder);
                }
            },
            [&](size_t row, VariantRef value) {
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
    const DorisVector<size_t> lengths =
            json_lengths(column, row_num, row_num + 1, nullptr, options);
    DCHECK_EQ(lengths.size(), 1);
    visit_variant_values(
            column, row_num, row_num + 1, {}, [](size_t) {},
            [&](size_t, VariantRef value) { write_json_value(value, bw, options); });
}

Status DataTypeVariantV2SerDe::write_column_to_mysql_binary(const IColumn& column,
                                                            MysqlRowBinaryBuffer& row_buffer,
                                                            int64_t row_idx, bool col_const,
                                                            const FormatOptions& options) const {
    RETURN_IF_CATCH_EXCEPTION({
        const size_t row = col_const ? 0 : checked_row(row_idx);
        const DorisVector<size_t> lengths = json_lengths(column, row, row + 1, nullptr, options);
        DorisVector<char> rendered(lengths[0]);
        visit_variant_values(
                column, row, row + 1, {}, [](size_t) {},
                [&](size_t, VariantRef value) {
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
        FormatOptions options;
        options.timezone = &ctz;
        const size_t first = checked_row(start);
        const size_t last = checked_row(end);
        if (array_builder->type()->id() == arrow::Type::STRING) {
            return write_arrow(column, null_map, assert_cast<arrow::StringBuilder&>(*array_builder),
                               first, last, options);
        }
        if (array_builder->type()->id() == arrow::Type::LARGE_STRING) {
            return write_arrow(column, null_map,
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
        const size_t first = checked_row(start);
        const size_t last = checked_row(end);
        const DorisVector<size_t> lengths = json_lengths(column, first, last, null_map, options);
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
        visit_variant_values(
                column, first, last, forced_nulls(null_map),
                [&](size_t row) {
                    batch->notNull[row] = 0;
                    batch->data[row] = nullptr;
                    batch->length[row] = 0;
                },
                [&](size_t row, VariantRef value) {
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
