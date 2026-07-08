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

#include "format_v2/orc/orc_reader.h"

#include <cctz/time_zone.h>
#include <gen_cpp/Types_types.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <orc/OrcFile.hh>
#include <orc/Vector.hh>
#include <orc/sargs/Literal.hh>
#include <orc/sargs/SearchArgument.hh>
#include <set>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "common/consts.h"
#include "common/exception.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_decimal.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/types.h"
#include "core/value/timestamptz_value.h"
#include "core/value/vdatetime_value.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "format_v2/column_mapper.h"
#include "format_v2/orc/orc_search_argument.h"
#include "io/fs/file_reader.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_profile.h"
#include "storage/index/zone_map/zone_map_index.h"
#include "storage/segment/condition_cache.h"
#include "storage/utils.h"
#include "util/slice.h"
#include "util/timezone_utils.h"

namespace doris::format::orc {
namespace {

constexpr uint64_t DEFAULT_ORC_READ_BATCH_SIZE = 4096;
constexpr uint64_t DEFAULT_ORC_NATURAL_READ_SIZE = 128 * 1024;
constexpr int DECIMAL_PRECISION_FOR_HIVE11 = BeConsts::MAX_DECIMAL128_PRECISION;
constexpr int DECIMAL_SCALE_FOR_HIVE11 = 10;
constexpr int32_t DORIS_DATE_EPOCH_DAYNR = 719528;
constexpr const char* ORC_LIST_ELEMENT_NAME = "element";
constexpr const char* ORC_MAP_KEY_NAME = "key";
constexpr const char* ORC_MAP_VALUE_NAME = "value";
constexpr const char* ORC_ICEBERG_ID_ATTRIBUTE = "iceberg.id";

uint64_t orc_metric_value(const std::atomic<uint64_t>& metric) {
    return metric.load(std::memory_order_relaxed);
}

template <typename Metrics, typename = void>
struct OrcReadRowCountMetric {
    static uint64_t value(const Metrics&) { return 0; }
};

template <typename Metrics>
struct OrcReadRowCountMetric<Metrics,
                             std::void_t<decltype(std::declval<const Metrics&>().ReadRowCount)>> {
    static uint64_t value(const Metrics& metrics) { return orc_metric_value(metrics.ReadRowCount); }
};

uint64_t orc_read_row_count(const ::orc::ReaderMetrics& metrics) {
    return OrcReadRowCountMetric<::orc::ReaderMetrics>::value(metrics);
}

bool is_orc_stop(const io::IOContext* io_ctx, const std::exception& e) {
    return io_ctx != nullptr && io_ctx->should_stop && std::string_view(e.what()) == "stop";
}

bool is_hour_offset_timezone(std::string_view timezone) {
    return timezone.size() == 6 && (timezone[0] == '+' || timezone[0] == '-') &&
           std::isdigit(static_cast<unsigned char>(timezone[1])) &&
           std::isdigit(static_cast<unsigned char>(timezone[2])) && timezone[3] == ':' &&
           timezone[4] == '0' && timezone[5] == '0';
}

Status set_orc_reader_timezone(const std::string& timezone,
                               ::orc::RowReaderOptions* row_reader_options) {
    if (timezone == "CST") {
        row_reader_options->setTimezoneName("Asia/Shanghai");
        return Status::OK();
    }

    if (!timezone.empty() && (timezone[0] == '+' || timezone[0] == '-') &&
        is_hour_offset_timezone(timezone)) {
        const int hour = (timezone[1] - '0') * 10 + timezone[2] - '0';
        row_reader_options->setTimezoneName(
                hour == 0 ? "Etc/GMT"
                          : fmt::format("Etc/GMT{}{}", timezone[0] == '+' ? '-' : '+', hour));
        return Status::OK();
    }

    row_reader_options->setTimezoneName(timezone.empty() ? "UTC" : timezone);
    return Status::OK();
}

// Thin adapter from Doris FileReader to ORC's InputStream API. Keep IO policy,
// tracing, and retry behavior in the underlying FileReader.
class DorisOrcInputStream final : public ::orc::InputStream {
public:
    DorisOrcInputStream(std::string file_name, io::FileReaderSPtr file_reader,
                        io::IOContext* io_ctx)
            : _file_name(std::move(file_name)),
              _file_reader(std::move(file_reader)),
              _io_ctx(io_ctx) {}

    uint64_t getLength() const override { return _file_reader->size(); }

    uint64_t getNaturalReadSize() const override { return DEFAULT_ORC_NATURAL_READ_SIZE; }

    void read(void* buf, uint64_t length, uint64_t offset) override {
        uint64_t bytes_read = 0;
        auto* out = static_cast<uint8_t*>(buf);
        while (bytes_read < length) {
            if (_io_ctx != nullptr && _io_ctx->should_stop) {
                throw ::orc::ParseError("stop");
            }
            size_t loop_read = 0;
            Status st = _file_reader->read_at(
                    static_cast<size_t>(offset + bytes_read),
                    Slice(out + bytes_read, static_cast<size_t>(length - bytes_read)), &loop_read,
                    _io_ctx);
            if (!st.ok()) {
                throw ::orc::ParseError("Failed to read " + _file_name + ": " +
                                        st.to_string_no_stack());
            }
            if (loop_read == 0) {
                break;
            }
            bytes_read += loop_read;
        }
        if (bytes_read != length) {
            throw ::orc::ParseError("Short read from " + _file_name);
        }
    }

    const std::string& getName() const override { return _file_name; }

private:
    std::string _file_name;
    io::FileReaderSPtr _file_reader;
    io::IOContext* _io_ctx = nullptr;
};

// selected_rows is a source-row remap produced by ORC lazy callback:
// predicate columns are decoded first, then surviving row ids drive follower decodes.
bool is_null_at(const ::orc::ColumnVectorBatch& batch, size_t row) {
    return batch.hasNulls && !batch.notNull[row];
}

size_t decode_row_count(size_t rows, const std::vector<size_t>* selected_rows) {
    if (selected_rows == nullptr) {
        return rows;
    }
    return selected_rows->size();
}

size_t source_row_at(size_t row, const std::vector<size_t>* selected_rows) {
    if (selected_rows == nullptr) {
        return row;
    }
    return (*selected_rows)[row];
}

DecodedColumnView make_orc_decoded_view(size_t rows, const std::vector<size_t>* selected_rows,
                                        DecodedValueKind value_kind) {
    DecodedColumnView view;
    view.value_kind = value_kind;
    view.row_count = cast_set<int64_t>(decode_row_count(rows, selected_rows));
    return view;
}

void fill_orc_decoded_null_map(const ::orc::ColumnVectorBatch& batch, size_t rows,
                               const std::vector<size_t>* selected_rows, NullMap* null_map) {
    DORIS_CHECK(null_map != nullptr);
    if (!batch.hasNulls) {
        return;
    }
    const auto output_rows = decode_row_count(rows, selected_rows);
    null_map->resize(output_rows);
    for (size_t row = 0; row < output_rows; ++row) {
        (*null_map)[row] = !batch.notNull[source_row_at(row, selected_rows)];
    }
}

size_t trim_right_spaces(const char* value, size_t length) {
    while (length > 0 && value[length - 1] == ' ') {
        --length;
    }
    return length;
}

Int128 to_int128(::orc::Int128 value) {
    const auto high_bits = static_cast<__uint128_t>(static_cast<uint64_t>(value.getHighBits()));
    const auto low_bits = static_cast<__uint128_t>(value.getLowBits());
    return static_cast<Int128>((high_bits << 64) | low_bits);
}

::orc::Int128 to_orc_int128(Int128 value) {
    const auto unsigned_value = static_cast<__uint128_t>(value);
    return ::orc::Int128(static_cast<int64_t>(static_cast<uint64_t>(unsigned_value >> 64)),
                         static_cast<uint64_t>(unsigned_value));
}

Status scale_decimal_value(Int128 value, int32_t source_scale, int32_t target_scale,
                           Int128* scaled_value) {
    DORIS_CHECK(scaled_value != nullptr);
    if (source_scale == target_scale) {
        *scaled_value = value;
        return Status::OK();
    }
    if (source_scale < target_scale) {
        bool overflow = false;
        const auto scaled = ::orc::scaleUpInt128ByPowerOfTen(to_orc_int128(value),
                                                             target_scale - source_scale, overflow);
        if (overflow) {
            return Status::DataQualityError(
                    "ORC decimal value overflows when scaling from {} to {}", source_scale,
                    target_scale);
        }
        *scaled_value = to_int128(scaled);
        return Status::OK();
    }
    *scaled_value = to_int128(
            ::orc::scaleDownInt128ByPowerOfTen(to_orc_int128(value), source_scale - target_scale));
    return Status::OK();
}

void fill_decimal_big_endian_value(Int128 value, std::array<uint8_t, sizeof(Int128)>* bytes) {
    DORIS_CHECK(bytes != nullptr);
    const auto unsigned_value = static_cast<__uint128_t>(value);
    for (size_t byte_idx = 0; byte_idx < bytes->size(); ++byte_idx) {
        const auto shift = (bytes->size() - byte_idx - 1) * 8;
        (*bytes)[byte_idx] = static_cast<uint8_t>(unsigned_value >> shift);
    }
}

Status read_decoded_values(DataTypePtr data_type, IColumn& column, DecodedColumnView* view) {
    DORIS_CHECK(data_type != nullptr);
    DORIS_CHECK(view != nullptr);
    RETURN_IF_ERROR(data_type->get_serde()->read_column_from_decoded_values(column, *view));
    return Status::OK();
}

template <typename SourceType>
void fill_selected_values(const SourceType* source_values, size_t rows,
                          const std::vector<size_t>* selected_rows,
                          std::vector<SourceType>* selected_values) {
    DORIS_CHECK(source_values != nullptr);
    DORIS_CHECK(selected_values != nullptr);
    const auto output_rows = decode_row_count(rows, selected_rows);
    selected_values->resize(output_rows);
    for (size_t row = 0; row < output_rows; ++row) {
        (*selected_values)[row] = source_values[source_row_at(row, selected_rows)];
    }
}

template <typename OrcBatchType, typename SourceType>
Status decode_fixed_values_with_serde(DataTypePtr data_type, IColumn& column,
                                      const ::orc::ColumnVectorBatch& batch, size_t rows,
                                      const std::vector<size_t>* selected_rows,
                                      DecodedValueKind value_kind) {
    const auto* orc_batch = dynamic_cast<const OrcBatchType*>(&batch);
    if (orc_batch == nullptr) {
        return Status::InternalError("Unexpected ORC scalar batch type {}", batch.toString());
    }
    auto view = make_orc_decoded_view(rows, selected_rows, value_kind);
    NullMap null_map;
    fill_orc_decoded_null_map(batch, rows, selected_rows, &null_map);
    view.null_map = null_map.empty() ? nullptr : null_map.data();
    std::vector<SourceType> selected_values;
    if (selected_rows == nullptr) {
        view.values = reinterpret_cast<const uint8_t*>(orc_batch->data.data());
    } else {
        fill_selected_values(orc_batch->data.data(), rows, selected_rows, &selected_values);
        view.values = reinterpret_cast<const uint8_t*>(selected_values.data());
    }
    RETURN_IF_ERROR(read_decoded_values(std::move(data_type), column, &view));
    return Status::OK();
}

Status decode_float_values_with_serde(DataTypePtr data_type, IColumn& column,
                                      const ::orc::ColumnVectorBatch& batch, size_t rows,
                                      const std::vector<size_t>* selected_rows) {
    const auto* orc_batch = dynamic_cast<const ::orc::DoubleVectorBatch*>(&batch);
    if (orc_batch == nullptr) {
        return Status::InternalError("Unexpected ORC float batch type {}", batch.toString());
    }
    auto view = make_orc_decoded_view(rows, selected_rows, DecodedValueKind::FLOAT);
    NullMap null_map;
    fill_orc_decoded_null_map(batch, rows, selected_rows, &null_map);
    view.null_map = null_map.empty() ? nullptr : null_map.data();
    const auto output_rows = decode_row_count(rows, selected_rows);
    std::vector<float> float_values;
    float_values.resize(output_rows);
    for (size_t row = 0; row < output_rows; ++row) {
        float_values[row] = static_cast<float>(orc_batch->data[source_row_at(row, selected_rows)]);
    }
    view.values = reinterpret_cast<const uint8_t*>(float_values.data());
    RETURN_IF_ERROR(read_decoded_values(std::move(data_type), column, &view));
    return Status::OK();
}

Status decode_boolean_values_with_serde(DataTypePtr data_type, IColumn& column,
                                        const ::orc::ColumnVectorBatch& batch, size_t rows,
                                        const std::vector<size_t>* selected_rows) {
    const auto* orc_batch = dynamic_cast<const ::orc::LongVectorBatch*>(&batch);
    if (orc_batch == nullptr) {
        return Status::InternalError("Unexpected ORC boolean batch type {}", batch.toString());
    }
    auto view = make_orc_decoded_view(rows, selected_rows, DecodedValueKind::BOOL);
    NullMap null_map;
    fill_orc_decoded_null_map(batch, rows, selected_rows, &null_map);
    view.null_map = null_map.empty() ? nullptr : null_map.data();
    const auto output_rows = decode_row_count(rows, selected_rows);
    std::unique_ptr<bool[]> bool_values = std::make_unique<bool[]>(output_rows);
    for (size_t row = 0; row < output_rows; ++row) {
        bool_values[row] = orc_batch->data[source_row_at(row, selected_rows)] != 0;
    }
    view.values = reinterpret_cast<const uint8_t*>(bool_values.get());
    RETURN_IF_ERROR(read_decoded_values(std::move(data_type), column, &view));
    return Status::OK();
}

Status decode_string_values_with_serde(DataTypePtr data_type, IColumn& column,
                                       const ::orc::Type& file_type,
                                       const ::orc::ColumnVectorBatch& batch, size_t rows,
                                       const std::vector<size_t>* selected_rows) {
    const auto* orc_batch = dynamic_cast<const ::orc::StringVectorBatch*>(&batch);
    if (orc_batch == nullptr) {
        return Status::InternalError("Unexpected ORC string batch type {}", batch.toString());
    }
    auto view = make_orc_decoded_view(rows, selected_rows, DecodedValueKind::BINARY);
    NullMap null_map;
    fill_orc_decoded_null_map(batch, rows, selected_rows, &null_map);
    view.null_map = null_map.empty() ? nullptr : null_map.data();
    const auto output_rows = decode_row_count(rows, selected_rows);
    std::vector<StringRef> binary_values;
    binary_values.reserve(output_rows);
    for (size_t row = 0; row < output_rows; ++row) {
        const auto source_row = source_row_at(row, selected_rows);
        if (is_null_at(batch, source_row)) {
            binary_values.emplace_back("", 0);
            continue;
        }
        auto length = static_cast<size_t>(orc_batch->length[source_row]);
        if (file_type.getKind() == ::orc::TypeKind::CHAR) {
            length = trim_right_spaces(orc_batch->data[source_row], length);
        }
        binary_values.emplace_back(length == 0 ? "" : orc_batch->data[source_row], length);
    }
    view.binary_values = &binary_values;
    RETURN_IF_ERROR(read_decoded_values(std::move(data_type), column, &view));
    return Status::OK();
}

Status decode_date_values_with_serde(DataTypePtr data_type, IColumn& column,
                                     const ::orc::ColumnVectorBatch& batch, size_t rows,
                                     const std::vector<size_t>* selected_rows) {
    const auto* orc_batch = dynamic_cast<const ::orc::LongVectorBatch*>(&batch);
    if (orc_batch == nullptr) {
        return Status::InternalError("Unexpected ORC date batch type {}", batch.toString());
    }
    auto view = make_orc_decoded_view(rows, selected_rows, DecodedValueKind::INT32);
    NullMap null_map;
    fill_orc_decoded_null_map(batch, rows, selected_rows, &null_map);
    view.null_map = null_map.empty() ? nullptr : null_map.data();
    const auto output_rows = decode_row_count(rows, selected_rows);
    std::vector<int32_t> date_values;
    date_values.resize(output_rows);
    auto& date_dict = date_day_offset_dict::get();
    for (size_t row = 0; row < output_rows; ++row) {
        const auto source_row = source_row_at(row, selected_rows);
        // Match the v1 ORC reader for legacy Hive/ORC dates, including underflow fallback.
        const auto date = date_dict[cast_set<int>(orc_batch->data[source_row])];
        date_values[row] = cast_set<int32_t>(date.daynr() - DORIS_DATE_EPOCH_DAYNR);
    }
    view.values = reinterpret_cast<const uint8_t*>(date_values.data());
    RETURN_IF_ERROR(read_decoded_values(std::move(data_type), column, &view));
    return Status::OK();
}

Status decode_decimal_values_with_serde(DataTypePtr data_type, IColumn& column,
                                        const ::orc::Type& file_type,
                                        const ::orc::ColumnVectorBatch& batch, size_t rows,
                                        const std::vector<size_t>* selected_rows) {
    auto view = make_orc_decoded_view(rows, selected_rows, DecodedValueKind::FIXED_BINARY);
    view.decimal_precision = file_type.getPrecision() == 0
                                     ? DECIMAL_PRECISION_FOR_HIVE11
                                     : cast_set<int>(file_type.getPrecision());
    NullMap null_map;
    fill_orc_decoded_null_map(batch, rows, selected_rows, &null_map);
    view.null_map = null_map.empty() ? nullptr : null_map.data();
    view.fixed_length = sizeof(Int128);

    std::vector<StringRef> binary_values;
    std::vector<std::array<uint8_t, sizeof(Int128)>> decimal_values;
    const auto output_rows = decode_row_count(rows, selected_rows);
    decimal_values.resize(output_rows);
    binary_values.reserve(output_rows);
    const auto target_scale = cast_set<int32_t>(remove_nullable(data_type)->get_scale());

    if (const auto* decimal64_batch = dynamic_cast<const ::orc::Decimal64VectorBatch*>(&batch);
        decimal64_batch != nullptr) {
        view.decimal_scale = decimal64_batch->scale;
        for (size_t row = 0; row < output_rows; ++row) {
            Int128 value = 0;
            const auto source_row = source_row_at(row, selected_rows);
            if (!is_null_at(batch, source_row)) {
                RETURN_IF_ERROR(scale_decimal_value(decimal64_batch->values[source_row],
                                                    decimal64_batch->scale, target_scale, &value));
            }
            fill_decimal_big_endian_value(value, &decimal_values[row]);
            binary_values.emplace_back(reinterpret_cast<const char*>(decimal_values[row].data()),
                                       decimal_values[row].size());
        }
        view.binary_values = &binary_values;
        RETURN_IF_ERROR(read_decoded_values(std::move(data_type), column, &view));
        return Status::OK();
    }
    const auto* decimal128_batch = dynamic_cast<const ::orc::Decimal128VectorBatch*>(&batch);
    if (decimal128_batch == nullptr) {
        return Status::InternalError("Unexpected ORC decimal batch type {}", batch.toString());
    }
    view.decimal_scale = decimal128_batch->scale;
    for (size_t row = 0; row < output_rows; ++row) {
        Int128 value = 0;
        const auto source_row = source_row_at(row, selected_rows);
        if (!is_null_at(batch, source_row)) {
            RETURN_IF_ERROR(scale_decimal_value(to_int128(decimal128_batch->values[source_row]),
                                                decimal128_batch->scale, target_scale, &value));
        }
        fill_decimal_big_endian_value(value, &decimal_values[row]);
        binary_values.emplace_back(reinterpret_cast<const char*>(decimal_values[row].data()),
                                   decimal_values[row].size());
    }
    view.binary_values = &binary_values;
    RETURN_IF_ERROR(read_decoded_values(std::move(data_type), column, &view));
    return Status::OK();
}

// ORC nested projection is type-id based. These helpers translate Doris'
// LocalColumnIndex tree into the ORC type ids expected by includeTypes().
Status get_projection_child_index(const format::LocalColumnIndex& child, int32_t child_count,
                                  const std::string& column_name, int32_t* child_idx) {
    DORIS_CHECK(child_idx != nullptr);
    *child_idx = child.local_id();
    if (*child_idx < 0 || *child_idx >= child_count) {
        return Status::InvalidArgument("Invalid ORC projection child index {} for column {}",
                                       *child_idx, column_name);
    }
    return Status::OK();
}

void collect_type_and_descendant_ids(const ::orc::Type& type, std::set<uint64_t>* const type_ids) {
    DORIS_CHECK(type_ids != nullptr);
    type_ids->insert(type.getColumnId());
    for (uint64_t child_idx = 0; child_idx < type.getSubtypeCount(); ++child_idx) {
        const auto* child_type = type.getSubtype(child_idx);
        DORIS_CHECK(child_type != nullptr);
        collect_type_and_descendant_ids(*child_type, type_ids);
    }
}

Status collect_projected_type_ids(const ::orc::Type& type,
                                  const format::LocalColumnIndex& projection,
                                  std::set<uint64_t>* const type_ids);

Status collect_projected_map_type_ids(const ::orc::Type& type,
                                      const format::LocalColumnIndex& projection,
                                      std::set<uint64_t>* const type_ids);

Status collect_projected_map_type_ids(const ::orc::Type& type,
                                      const format::LocalColumnIndex& projection,
                                      std::set<uint64_t>* const type_ids) {
    DORIS_CHECK(type.getKind() == ::orc::TypeKind::MAP);
    DORIS_CHECK(type.getSubtypeCount() == 2);
    type_ids->insert(type.getColumnId());
    if (projection.project_all_children) {
        collect_type_and_descendant_ids(type, type_ids);
        return Status::OK();
    }
    if (projection.children.empty()) {
        return Status::NotSupported("ORC MAP projection for column {} contains no children",
                                    projection.local_id());
    }

    bool selected_key = false;
    bool selected_value = false;
    for (const auto& key_value_projection : projection.children) {
        int32_t key_value_idx = 0;
        RETURN_IF_ERROR(
                get_projection_child_index(key_value_projection, 2, "orc_map", &key_value_idx));
        const auto* child_type = type.getSubtype(static_cast<uint64_t>(key_value_idx));
        DORIS_CHECK(child_type != nullptr);
        RETURN_IF_ERROR(collect_projected_type_ids(*child_type, key_value_projection, type_ids));
        selected_key = selected_key || key_value_idx == 0;
        selected_value = selected_value || key_value_idx == 1;
    }
    if (!selected_key || !selected_value) {
        return Status::NotSupported("ORC MAP projection must include both key and value");
    }
    return Status::OK();
}

Status collect_projected_type_ids(const ::orc::Type& type,
                                  const format::LocalColumnIndex& projection,
                                  std::set<uint64_t>* const type_ids) {
    DORIS_CHECK(type_ids != nullptr);
    type_ids->insert(type.getColumnId());
    if (projection.project_all_children) {
        collect_type_and_descendant_ids(type, type_ids);
        return Status::OK();
    }
    if (projection.children.empty()) {
        return Status::NotSupported("ORC projection contains no children");
    }
    if (type.getKind() == ::orc::TypeKind::MAP) {
        return collect_projected_map_type_ids(type, projection, type_ids);
    }
    if (type.getKind() != ::orc::TypeKind::STRUCT && type.getKind() != ::orc::TypeKind::LIST) {
        return Status::InvalidArgument("Cannot project children from non-complex ORC type {}",
                                       static_cast<int>(type.getKind()));
    }

    const auto child_count = static_cast<int32_t>(type.getSubtypeCount());
    for (const auto& child_projection : projection.children) {
        int32_t child_idx = 0;
        RETURN_IF_ERROR(get_projection_child_index(child_projection, child_count, "orc_complex",
                                                   &child_idx));
        const auto* child_type = type.getSubtype(static_cast<uint64_t>(child_idx));
        DORIS_CHECK(child_type != nullptr);
        RETURN_IF_ERROR(collect_projected_type_ids(*child_type, child_projection, type_ids));
    }
    return Status::OK();
}

// For arrays/maps, selected parent rows expand into selected element rows. The
// returned offsets are compacted so downstream child decoding can append densely.
Status append_orc_offsets(ColumnArray::Offsets64& doris_offsets,
                          const ::orc::DataBuffer<int64_t>& orc_offsets, size_t rows,
                          size_t* element_size, const std::vector<size_t>* selected_rows = nullptr,
                          std::vector<size_t>* element_selection = nullptr) {
    if (selected_rows != nullptr) {
        DORIS_CHECK(element_selection != nullptr);
        const auto prev_offset = doris_offsets.empty() ? 0 : doris_offsets.back();
        ColumnArray::Offset64 current_offset = prev_offset;
        element_selection->clear();
        for (size_t row = 0; row < selected_rows->size(); ++row) {
            const auto source_row = (*selected_rows)[row];
            DORIS_CHECK(source_row < rows);
            const auto begin_offset = orc_offsets[source_row];
            const auto end_offset = orc_offsets[source_row + 1];
            if (end_offset < begin_offset) {
                return Status::Corruption("Invalid ORC offsets");
            }
            const auto delta = static_cast<size_t>(end_offset - begin_offset);
            for (size_t element_idx = 0; element_idx < delta; ++element_idx) {
                element_selection->push_back(static_cast<size_t>(begin_offset) + element_idx);
            }
            current_offset += static_cast<ColumnArray::Offset64>(delta);
            doris_offsets.push_back(current_offset);
        }
        *element_size = element_selection->size();
        return Status::OK();
    }

    const auto prev_offset = doris_offsets.empty() ? 0 : doris_offsets.back();
    const auto base_offset = orc_offsets[0];
    for (size_t idx = 1; idx <= rows; ++idx) {
        const auto delta = orc_offsets[idx] - base_offset;
        if (delta < 0) {
            return Status::Corruption("Invalid ORC offsets");
        }
        doris_offsets.push_back(prev_offset + static_cast<ColumnArray::Offset64>(delta));
    }
    const auto total_delta = orc_offsets[rows] - base_offset;
    if (total_delta < 0) {
        return Status::Corruption("Invalid ORC offsets");
    }
    *element_size = static_cast<size_t>(total_delta);
    return Status::OK();
}

int64_t find_struct_child_index(const ::orc::Type& type, const std::string& field_name) {
    DORIS_CHECK(type.getKind() == ::orc::TypeKind::STRUCT);
    for (uint64_t child_idx = 0; child_idx < type.getSubtypeCount(); ++child_idx) {
        if (type.getFieldName(child_idx) == field_name) {
            return static_cast<int64_t>(child_idx);
        }
    }
    return -1;
}

bool is_row_position_column(format::LocalColumnId file_column_id) {
    return file_column_id == format::LocalColumnId(format::ROW_POSITION_COLUMN_ID);
}

bool is_global_rowid_column(format::LocalColumnId file_column_id) {
    return file_column_id == format::LocalColumnId(format::GLOBAL_ROWID_COLUMN_ID);
}

bool is_virtual_column(format::LocalColumnId file_column_id) {
    return is_row_position_column(file_column_id) || is_global_rowid_column(file_column_id);
}

format::ColumnDefinition nullable_global_rowid_column_definition() {
    auto field = format::global_rowid_column_definition();
    field.type = make_nullable(field.type);
    return field;
}

const format::LocalColumnIndex* find_projection(
        const std::vector<format::LocalColumnIndex>& projections,
        format::LocalColumnId file_column_id) {
    const auto it = std::find_if(projections.begin(), projections.end(),
                                 [&](const format::LocalColumnIndex& projection) {
                                     return projection.column_id() == file_column_id;
                                 });
    return it == projections.end() ? nullptr : &*it;
}

bool local_column_ids_are_unique(const std::vector<format::LocalColumnIndex>& projections) {
    std::set<format::LocalColumnId> column_ids;
    for (const auto& projection : projections) {
        if (!column_ids.insert(projection.column_id()).second) {
            return false;
        }
    }
    return true;
}

const format::LocalColumnIndex* find_request_projection(const format::FileScanRequest& request,
                                                        format::LocalColumnId file_column_id) {
    if (const auto* projection = find_projection(request.predicate_columns, file_column_id);
        projection != nullptr) {
        return projection;
    }
    return find_projection(request.non_predicate_columns, file_column_id);
}

bool has_pruned_projection(const format::LocalColumnIndex& projection) {
    return !projection.project_all_children;
}

Status collect_lazy_filter_type_ids(const ::orc::Type& root_type,
                                    const std::vector<format::LocalColumnIndex>& projections,
                                    std::set<uint64_t>* const type_ids) {
    DORIS_CHECK(type_ids != nullptr);
    for (const auto& projection : projections) {
        const auto file_column_id = projection.column_id();
        if (is_virtual_column(file_column_id)) {
            continue;
        }
        const auto* type = root_type.getSubtype(static_cast<uint64_t>(file_column_id.value()));
        DORIS_CHECK(type != nullptr);
        if (!has_pruned_projection(projection)) {
            collect_type_and_descendant_ids(*type, type_ids);
            continue;
        }
        RETURN_IF_ERROR(collect_projected_type_ids(*type, projection, type_ids));
    }
    return Status::OK();
}

// Stripe pruning maps ORC stripe statistics into Doris ZoneMap semantics. Missing
// or unsupported statistics are treated conservatively and never prune.
bool set_integer_zone_map(const ::orc::Type& type, const ::orc::ColumnStatistics& statistics,
                          segment_v2::ZoneMap* zone_map) {
    const auto* integer_statistics =
            dynamic_cast<const ::orc::IntegerColumnStatistics*>(&statistics);
    if (integer_statistics == nullptr || !integer_statistics->hasMinimum() ||
        !integer_statistics->hasMaximum()) {
        return false;
    }
    switch (type.getKind()) {
    case ::orc::TypeKind::BYTE:
        zone_map->min_value =
                Field::create_field<TYPE_TINYINT>(cast_set<Int8>(integer_statistics->getMinimum()));
        zone_map->max_value =
                Field::create_field<TYPE_TINYINT>(cast_set<Int8>(integer_statistics->getMaximum()));
        return true;
    case ::orc::TypeKind::SHORT:
        zone_map->min_value = Field::create_field<TYPE_SMALLINT>(
                cast_set<Int16>(integer_statistics->getMinimum()));
        zone_map->max_value = Field::create_field<TYPE_SMALLINT>(
                cast_set<Int16>(integer_statistics->getMaximum()));
        return true;
    case ::orc::TypeKind::INT:
        zone_map->min_value =
                Field::create_field<TYPE_INT>(cast_set<Int32>(integer_statistics->getMinimum()));
        zone_map->max_value =
                Field::create_field<TYPE_INT>(cast_set<Int32>(integer_statistics->getMaximum()));
        return true;
    case ::orc::TypeKind::LONG:
        zone_map->min_value = Field::create_field<TYPE_BIGINT>(integer_statistics->getMinimum());
        zone_map->max_value = Field::create_field<TYPE_BIGINT>(integer_statistics->getMaximum());
        return true;
    default:
        return false;
    }
}

bool set_boolean_zone_map(const ::orc::ColumnStatistics& statistics,
                          segment_v2::ZoneMap* zone_map) {
    const auto* boolean_statistics =
            dynamic_cast<const ::orc::BooleanColumnStatistics*>(&statistics);
    if (boolean_statistics == nullptr || !boolean_statistics->hasCount()) {
        return false;
    }
    const bool has_false = boolean_statistics->getFalseCount() > 0;
    const bool has_true = boolean_statistics->getTrueCount() > 0;
    if (!has_false && !has_true) {
        return false;
    }
    zone_map->min_value = Field::create_field<TYPE_BOOLEAN>(static_cast<UInt8>(has_false ? 0 : 1));
    zone_map->max_value = Field::create_field<TYPE_BOOLEAN>(static_cast<UInt8>(has_true ? 1 : 0));
    return true;
}

bool set_floating_zone_map(const ::orc::Type& type, const ::orc::ColumnStatistics& statistics,
                           segment_v2::ZoneMap* zone_map) {
    const auto* double_statistics = dynamic_cast<const ::orc::DoubleColumnStatistics*>(&statistics);
    if (double_statistics == nullptr || !double_statistics->hasMinimum() ||
        !double_statistics->hasMaximum()) {
        return false;
    }
    if (type.getKind() == ::orc::TypeKind::FLOAT) {
        zone_map->min_value = Field::create_field<TYPE_FLOAT>(
                static_cast<Float32>(double_statistics->getMinimum()));
        zone_map->max_value = Field::create_field<TYPE_FLOAT>(
                static_cast<Float32>(double_statistics->getMaximum()));
        return true;
    }
    if (type.getKind() == ::orc::TypeKind::DOUBLE) {
        zone_map->min_value = Field::create_field<TYPE_DOUBLE>(double_statistics->getMinimum());
        zone_map->max_value = Field::create_field<TYPE_DOUBLE>(double_statistics->getMaximum());
        return true;
    }
    return false;
}

bool set_string_zone_map(const ::orc::Type& type, const ::orc::ColumnStatistics& statistics,
                         segment_v2::ZoneMap* zone_map) {
    const auto* string_statistics = dynamic_cast<const ::orc::StringColumnStatistics*>(&statistics);
    if (string_statistics == nullptr || !string_statistics->hasMinimum() ||
        !string_statistics->hasMaximum()) {
        return false;
    }
    const auto build_field = [&](const std::string& value) {
        if (type.getKind() != ::orc::TypeKind::CHAR) {
            return Field::create_field<TYPE_STRING>(value);
        }
        return Field::create_field<TYPE_STRING>(
                std::string(value.data(), trim_right_spaces(value.data(), value.size())));
    };
    zone_map->min_value = build_field(string_statistics->getMinimum());
    zone_map->max_value = build_field(string_statistics->getMaximum());
    return true;
}

bool set_date_zone_map(const ::orc::ColumnStatistics& statistics, segment_v2::ZoneMap* zone_map) {
    const auto* date_statistics = dynamic_cast<const ::orc::DateColumnStatistics*>(&statistics);
    if (date_statistics == nullptr || !date_statistics->hasMinimum() ||
        !date_statistics->hasMaximum()) {
        return false;
    }
    auto& date_dict = date_day_offset_dict::get();
    zone_map->min_value =
            Field::create_field<TYPE_DATEV2>(date_dict[date_statistics->getMinimum()]);
    zone_map->max_value =
            Field::create_field<TYPE_DATEV2>(date_dict[date_statistics->getMaximum()]);
    return true;
}

DateV2Value<DateTimeV2ValueType> datetime_v2_from_orc_millis(int64_t millis, int32_t nanos_tail,
                                                             const cctz::time_zone& timezone) {
    int64_t seconds = millis / 1000;
    int64_t millis_remainder = millis % 1000;
    if (millis_remainder < 0) {
        --seconds;
        millis_remainder += 1000;
    }
    const auto extra_nanos = std::max<int32_t>(nanos_tail, 0);
    const auto microseconds = cast_set<uint64_t>(millis_remainder * 1000 + extra_nanos / 1000);
    DateV2Value<DateTimeV2ValueType> value;
    value.from_unixtime(seconds, timezone);
    value.set_microsecond(microseconds);
    return value;
}

bool set_timestamp_zone_map(const ::orc::ColumnStatistics& statistics,
                            const cctz::time_zone& timezone, segment_v2::ZoneMap* zone_map) {
    const auto* timestamp_statistics =
            dynamic_cast<const ::orc::TimestampColumnStatistics*>(&statistics);
    if (timestamp_statistics == nullptr || !timestamp_statistics->hasMinimum() ||
        !timestamp_statistics->hasMaximum()) {
        return false;
    }
    zone_map->min_value = Field::create_field<TYPE_DATETIMEV2>(datetime_v2_from_orc_millis(
            timestamp_statistics->getMinimum(), timestamp_statistics->getMinimumNanos(), timezone));
    zone_map->max_value = Field::create_field<TYPE_DATETIMEV2>(datetime_v2_from_orc_millis(
            timestamp_statistics->getMaximum(), timestamp_statistics->getMaximumNanos(), timezone));
    return true;
}

int32_t decimal_scale_for_orc_type(const ::orc::Type& type) {
    return type.getPrecision() == 0 ? DECIMAL_SCALE_FOR_HIVE11 : cast_set<int32_t>(type.getScale());
}

std::optional<Decimal128V3> decimal_value_at_scale(const ::orc::Decimal& decimal,
                                                   int32_t target_scale) {
    if (decimal.scale == target_scale) {
        return Decimal128V3(to_int128(decimal.value));
    }
    if (decimal.scale < target_scale) {
        bool overflow = false;
        const auto scaled = ::orc::scaleUpInt128ByPowerOfTen(
                decimal.value, target_scale - decimal.scale, overflow);
        if (overflow) {
            return std::nullopt;
        }
        return Decimal128V3(to_int128(scaled));
    }

    const auto scale_diff = decimal.scale - target_scale;
    const auto scaled = ::orc::scaleDownInt128ByPowerOfTen(decimal.value, scale_diff);
    bool overflow = false;
    const auto restored = ::orc::scaleUpInt128ByPowerOfTen(scaled, scale_diff, overflow);
    if (overflow || restored != decimal.value) {
        return std::nullopt;
    }
    return Decimal128V3(to_int128(scaled));
}

bool set_decimal_zone_map(const ::orc::Type& type, const ::orc::ColumnStatistics& statistics,
                          segment_v2::ZoneMap* zone_map) {
    const auto* decimal_statistics =
            dynamic_cast<const ::orc::DecimalColumnStatistics*>(&statistics);
    if (decimal_statistics == nullptr || !decimal_statistics->hasMinimum() ||
        !decimal_statistics->hasMaximum()) {
        return false;
    }
    const auto min = decimal_statistics->getMinimum();
    const auto max = decimal_statistics->getMaximum();
    const auto expected_scale = decimal_scale_for_orc_type(type);
    const auto min_value = decimal_value_at_scale(min, expected_scale);
    const auto max_value = decimal_value_at_scale(max, expected_scale);
    if (!min_value.has_value() || !max_value.has_value()) {
        return false;
    }
    zone_map->min_value = Field::create_field<TYPE_DECIMAL128I>(*min_value);
    zone_map->max_value = Field::create_field<TYPE_DECIMAL128I>(*max_value);
    return true;
}

bool build_zone_map_from_orc_statistics(const ::orc::Type& type,
                                        const ::orc::ColumnStatistics& statistics,
                                        const cctz::time_zone& timezone,
                                        segment_v2::ZoneMap* zone_map) {
    DORIS_CHECK(zone_map != nullptr);
    zone_map->has_null = statistics.hasNull();
    zone_map->has_not_null = statistics.getNumberOfValues() > 0;
    if (!zone_map->has_not_null) {
        return true;
    }
    switch (type.getKind()) {
    case ::orc::TypeKind::BOOLEAN:
        return set_boolean_zone_map(statistics, zone_map);
    case ::orc::TypeKind::BYTE:
    case ::orc::TypeKind::SHORT:
    case ::orc::TypeKind::INT:
    case ::orc::TypeKind::LONG:
        return set_integer_zone_map(type, statistics, zone_map);
    case ::orc::TypeKind::FLOAT:
    case ::orc::TypeKind::DOUBLE:
        return set_floating_zone_map(type, statistics, zone_map);
    case ::orc::TypeKind::STRING:
    case ::orc::TypeKind::VARCHAR:
    case ::orc::TypeKind::CHAR:
        return set_string_zone_map(type, statistics, zone_map);
    case ::orc::TypeKind::DATE:
        return set_date_zone_map(statistics, zone_map);
    case ::orc::TypeKind::TIMESTAMP:
    case ::orc::TypeKind::TIMESTAMP_INSTANT:
        return set_timestamp_zone_map(statistics, timezone, zone_map);
    case ::orc::TypeKind::DECIMAL:
        return set_decimal_zone_map(type, statistics, zone_map);
    default:
        return false;
    }
}

Status find_projected_minmax_leaf_in_type(const ::orc::Type& type,
                                          const format::LocalColumnIndex& projection,
                                          const ::orc::Type** leaf_type) {
    DORIS_CHECK(leaf_type != nullptr);
    if (projection.project_all_children || projection.children.empty()) {
        if (type.getSubtypeCount() > 0) {
            return Status::NotSupported(
                    "ORC aggregate pushdown only supports primitive column kind {}",
                    static_cast<int>(type.getKind()));
        }
        *leaf_type = &type;
        return Status::OK();
    }
    if (projection.children.size() != 1) {
        return Status::NotSupported(
                "ORC aggregate pushdown only supports a single nested leaf under column kind {}",
                static_cast<int>(type.getKind()));
    }
    if (type.getKind() != ::orc::TypeKind::STRUCT) {
        return Status::NotSupported(
                "ORC aggregate pushdown only supports struct nested leaf projection, got kind {}",
                static_cast<int>(type.getKind()));
    }
    const auto& child_projection = projection.children[0];
    if (child_projection.local_id() < 0 ||
        child_projection.local_id() >= static_cast<int32_t>(type.getSubtypeCount())) {
        return Status::InvalidArgument("Invalid ORC aggregate child local id {} for kind {}",
                                       child_projection.local_id(),
                                       static_cast<int>(type.getKind()));
    }
    const auto* child_type = type.getSubtype(static_cast<uint64_t>(child_projection.local_id()));
    DORIS_CHECK(child_type != nullptr);
    return find_projected_minmax_leaf_in_type(*child_type, child_projection, leaf_type);
}

Status find_projected_minmax_leaf(const ::orc::Type& root_type,
                                  const format::LocalColumnIndex& projection,
                                  const ::orc::Type** leaf_type) {
    DORIS_CHECK(leaf_type != nullptr);
    if (root_type.getKind() != ::orc::TypeKind::STRUCT) {
        return Status::NotSupported("ORC aggregate pushdown requires top-level struct schema");
    }
    const auto file_column_id = projection.column_id();
    if (!file_column_id.is_valid() ||
        file_column_id.value() >= static_cast<int32_t>(root_type.getSubtypeCount())) {
        return Status::InvalidArgument("Invalid ORC aggregate column id {}",
                                       file_column_id.value());
    }
    const auto* column_type = root_type.getSubtype(static_cast<uint64_t>(file_column_id.value()));
    DORIS_CHECK(column_type != nullptr);
    return find_projected_minmax_leaf_in_type(*column_type, projection, leaf_type);
}

} // namespace

class OrcReader::OrcFilterImpl final : public ::orc::ORCFilter {
public:
    explicit OrcFilterImpl(OrcReader* reader) : _reader(reader) {}

    void filter(::orc::ColumnVectorBatch& data, uint16_t* sel, uint16_t size,
                void* arg) const override {
        THROW_IF_ERROR(_reader->_filter_orc_batch(data, sel, size, arg));
    }

private:
    OrcReader* _reader = nullptr;
};

// Per-open mutable ORC state. close() publishes counters first, then resets this
// object so the reader can be opened again without carrying stale scan state.
struct OrcReaderScanState {
    struct StripeRange {
        uint64_t first_stripe = 0;
        uint64_t last_stripe = 0;
        uint64_t offset = 0;
        uint64_t length = 0;
    };

    std::unique_ptr<::orc::Reader> reader;
    const ::orc::Type* root_type = nullptr;
    ::orc::ReaderMetrics reader_metrics;
    ::orc::RowReaderOptions row_reader_options; // projection + filter + SARG + stripe range
    std::string timezone = TimezoneUtils::default_time_zone;
    cctz::time_zone timezone_obj;
    std::unique_ptr<::orc::RowReader> row_reader;
    const ::orc::Type* selected_type = nullptr;
    std::unique_ptr<::orc::ColumnVectorBatch> batch;

    std::vector<format::LocalColumnId> read_columns;
    std::map<format::LocalColumnId, size_t> column_to_selected_batch_index;

    uint64_t current_batch_first_row = 0;
    uint64_t row_reader_range_first_row = 0;
    uint64_t row_reader_range_end_row = 0;
    uint64_t row_reader_range_rows = 0;
    uint64_t condition_cache_next_row = 0;
    std::shared_ptr<ConditionCacheContext> condition_cache_ctx;

    std::vector<size_t> orc_lazy_selected_rows;
    size_t orc_lazy_input_rows = 0;
    uint64_t orc_lazy_next_batch_first_row = 0;
    bool orc_lazy_read_enabled = false;
    bool orc_lazy_selection_valid = false;

    std::vector<StripeRange> selected_stripe_ranges;
    size_t current_stripe_range = 0;
    bool stripe_pruning_applied = false;

    bool row_reader_created = false;
};

OrcReader::OrcReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
                     std::unique_ptr<io::FileDescription>& file_description,
                     std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
                     std::optional<format::GlobalRowIdContext> global_rowid_context,
                     bool enable_mapping_timestamp_tz)
        : FileReader(system_properties, file_description, io_ctx, profile),
          _global_rowid_context(std::move(global_rowid_context)),
          _enable_mapping_timestamp_tz(enable_mapping_timestamp_tz) {}

OrcReader::~OrcReader() = default;

// Expose ORC pruning and lazy-read statistics in RuntimeProfile. These counters
// are the quickest way to confirm whether SARG/stripe pruning actually fired.
void OrcReader::_init_profile() {
    if (_profile == nullptr) {
        return;
    }

    static const char* orc_profile = "OrcReader";
    ADD_TIMER_WITH_LEVEL(_profile, orc_profile, 1);
    _orc_profile.reader_call =
            ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "ReaderCall", TUnit::UNIT, orc_profile, 1);
    _orc_profile.reader_inclusive_latency_us = ADD_CHILD_COUNTER_WITH_LEVEL(
            _profile, "ReaderInclusiveLatencyUs", TUnit::UNIT, orc_profile, 1);
    _orc_profile.decompression_call = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "DecompressionCall",
                                                                   TUnit::UNIT, orc_profile, 1);
    _orc_profile.decompression_latency_us = ADD_CHILD_COUNTER_WITH_LEVEL(
            _profile, "DecompressionLatencyUs", TUnit::UNIT, orc_profile, 1);
    _orc_profile.decoding_call =
            ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "DecodingCall", TUnit::UNIT, orc_profile, 1);
    _orc_profile.decoding_latency_us = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "DecodingLatencyUs",
                                                                    TUnit::UNIT, orc_profile, 1);
    _orc_profile.byte_decoding_call =
            ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "ByteDecodingCall", TUnit::UNIT, orc_profile, 1);
    _orc_profile.byte_decoding_latency_us = ADD_CHILD_COUNTER_WITH_LEVEL(
            _profile, "ByteDecodingLatencyUs", TUnit::UNIT, orc_profile, 1);
    _orc_profile.io_count =
            ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "IOCount", TUnit::UNIT, orc_profile, 1);
    _orc_profile.io_blocking_latency_us = ADD_CHILD_COUNTER_WITH_LEVEL(
            _profile, "IOBlockingLatencyUs", TUnit::UNIT, orc_profile, 1);
    _orc_profile.selected_row_group_count = ADD_CHILD_COUNTER_WITH_LEVEL(
            _profile, "SelectedRowGroupCount", TUnit::UNIT, orc_profile, 1);
    _orc_profile.evaluated_row_group_count = ADD_CHILD_COUNTER_WITH_LEVEL(
            _profile, "EvaluatedRowGroupCount", TUnit::UNIT, orc_profile, 1);
    _orc_profile.read_row_count =
            ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "ReadRowCount", TUnit::UNIT, orc_profile, 1);
    _orc_profile.filtered_row_groups = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "RowGroupsFiltered",
                                                                    TUnit::UNIT, orc_profile, 1);
    _orc_profile.filtered_row_groups_by_min_max = ADD_CHILD_COUNTER_WITH_LEVEL(
            _profile, "RowGroupsFilteredByMinMax", TUnit::UNIT, orc_profile, 1);
    _orc_profile.read_row_groups =
            ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "RowGroupsReadNum", TUnit::UNIT, orc_profile, 1);
    _orc_profile.filtered_group_rows = ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "FilteredRowsByGroup",
                                                                    TUnit::UNIT, orc_profile, 1);
    _orc_profile.lazy_read_filtered_rows = ADD_CHILD_COUNTER_WITH_LEVEL(
            _profile, "FilteredRowsByLazyRead", TUnit::UNIT, orc_profile, 1);
    _orc_profile.orc_lazy_read_filtered_rows = ADD_CHILD_COUNTER_WITH_LEVEL(
            _profile, "FilteredRowsByOrcLazyRead", TUnit::UNIT, orc_profile, 1);
    _orc_profile.filtered_bytes =
            ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "FilteredBytes", TUnit::BYTES, orc_profile, 1);
    _orc_profile.open_file_num =
            ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "FileNum", TUnit::UNIT, orc_profile, 1);
}

void OrcReader::_collect_profile() const {
    if (_state == nullptr) {
        return;
    }

    const auto& reader_metrics = _state->reader_metrics;
    const uint64_t read_row_count = orc_read_row_count(reader_metrics);
    if (_profile != nullptr) {
        COUNTER_UPDATE(_orc_profile.reader_call, orc_metric_value(reader_metrics.ReaderCall));
        COUNTER_UPDATE(_orc_profile.reader_inclusive_latency_us,
                       orc_metric_value(reader_metrics.ReaderInclusiveLatencyUs));
        COUNTER_UPDATE(_orc_profile.decompression_call,
                       orc_metric_value(reader_metrics.DecompressionCall));
        COUNTER_UPDATE(_orc_profile.decompression_latency_us,
                       orc_metric_value(reader_metrics.DecompressionLatencyUs));
        COUNTER_UPDATE(_orc_profile.decoding_call, orc_metric_value(reader_metrics.DecodingCall));
        COUNTER_UPDATE(_orc_profile.decoding_latency_us,
                       orc_metric_value(reader_metrics.DecodingLatencyUs));
        COUNTER_UPDATE(_orc_profile.byte_decoding_call,
                       orc_metric_value(reader_metrics.ByteDecodingCall));
        COUNTER_UPDATE(_orc_profile.byte_decoding_latency_us,
                       orc_metric_value(reader_metrics.ByteDecodingLatencyUs));
        COUNTER_UPDATE(_orc_profile.io_count, orc_metric_value(reader_metrics.IOCount));
        COUNTER_UPDATE(_orc_profile.io_blocking_latency_us,
                       orc_metric_value(reader_metrics.IOBlockingLatencyUs));
        COUNTER_UPDATE(_orc_profile.selected_row_group_count,
                       orc_metric_value(reader_metrics.SelectedRowGroupCount));
        COUNTER_UPDATE(_orc_profile.evaluated_row_group_count,
                       orc_metric_value(reader_metrics.EvaluatedRowGroupCount));
        COUNTER_UPDATE(_orc_profile.read_row_count, read_row_count);
        COUNTER_UPDATE(_orc_profile.filtered_row_groups, _reader_statistics.filtered_row_groups);
        COUNTER_UPDATE(_orc_profile.filtered_row_groups_by_min_max,
                       _reader_statistics.filtered_row_groups_by_min_max);
        COUNTER_UPDATE(_orc_profile.read_row_groups, _reader_statistics.read_row_groups);
        COUNTER_UPDATE(_orc_profile.filtered_group_rows, _reader_statistics.filtered_group_rows);
        COUNTER_UPDATE(_orc_profile.lazy_read_filtered_rows,
                       _reader_statistics.lazy_read_filtered_rows);
        COUNTER_UPDATE(_orc_profile.orc_lazy_read_filtered_rows,
                       _reader_statistics.lazy_read_filtered_rows);
        COUNTER_UPDATE(_orc_profile.filtered_bytes, _reader_statistics.filtered_bytes);
        COUNTER_UPDATE(_orc_profile.open_file_num, _reader_statistics.open_file_num);
    }
    if (_io_ctx != nullptr && _io_ctx->file_reader_stats != nullptr) {
        _io_ctx->file_reader_stats->read_rows += read_row_count;
    }
}

format::ColumnDefinition OrcReader::row_position_column_definition() {
    return format::row_position_column_definition();
}

Status OrcReader::init(RuntimeState* state) {
    RETURN_IF_ERROR(format::FileReader::init(state));
    _state = std::make_unique<OrcReaderScanState>();
    TimezoneUtils::find_cctz_time_zone(_state->timezone, _state->timezone_obj);
    if (state != nullptr) {
        _state->timezone = state->timezone();
        _state->timezone_obj = state->timezone_obj();
    }

    ::orc::ReaderOptions options;
    options.setMemoryPool(*ExecEnv::GetInstance()->orc_memory_pool());
    options.setReaderMetrics(&_state->reader_metrics);

    auto input_stream = std::make_unique<DorisOrcInputStream>(_file_description->path,
                                                              _tracing_file_reader, _io_ctx.get());
    try {
        _state->reader = ::orc::createReader(std::move(input_stream), options);
        _state->root_type = &_state->reader->getType();
    } catch (const std::exception& e) {
        if (is_orc_stop(_io_ctx.get(), e)) {
            return Status::EndOfFile("stop");
        }
        return Status::InternalError("Failed to open ORC file {}: {}", _file_description->path,
                                     e.what());
    }
    return Status::OK();
}

void OrcReader::set_condition_cache_context(std::shared_ptr<ConditionCacheContext> ctx) {
    DORIS_CHECK(_state != nullptr);
    _state->condition_cache_ctx = std::move(ctx);
    if (_state->condition_cache_ctx != nullptr &&
        _state->condition_cache_ctx->filter_result != nullptr) {
        _state->condition_cache_ctx->base_granule = static_cast<int64_t>(
                _state->row_reader_range_first_row / ConditionCacheContext::GRANULE_SIZE);
    }
}

int64_t OrcReader::get_total_rows() const {
    DORIS_CHECK(_state != nullptr);
    if (_state->row_reader != nullptr) {
        return cast_set<int64_t>(_state->row_reader_range_rows);
    }
    if (_state->reader != nullptr) {
        return cast_set<int64_t>(_state->reader->getNumberOfRows());
    }
    return 0;
}

DataTypePtr OrcReader::_convert_to_doris_type(const ::orc::Type& type) const {
    DataTypePtr data_type;
    switch (type.getKind()) {
    case ::orc::TypeKind::BOOLEAN:
        data_type = std::make_shared<DataTypeUInt8>();
        break;
    case ::orc::TypeKind::BYTE:
        data_type = std::make_shared<DataTypeInt8>();
        break;
    case ::orc::TypeKind::SHORT:
        data_type = std::make_shared<DataTypeInt16>();
        break;
    case ::orc::TypeKind::INT:
        data_type = std::make_shared<DataTypeInt32>();
        break;
    case ::orc::TypeKind::LONG:
        data_type = std::make_shared<DataTypeInt64>();
        break;
    case ::orc::TypeKind::FLOAT:
        data_type = std::make_shared<DataTypeFloat32>();
        break;
    case ::orc::TypeKind::DOUBLE:
        data_type = std::make_shared<DataTypeFloat64>();
        break;
    case ::orc::TypeKind::STRING:
    case ::orc::TypeKind::BINARY:
        data_type = std::make_shared<DataTypeString>();
        break;
    case ::orc::TypeKind::VARCHAR:
        data_type = std::make_shared<DataTypeString>(cast_set<int>(type.getMaximumLength()),
                                                     PrimitiveType::TYPE_VARCHAR);
        break;
    case ::orc::TypeKind::CHAR:
        data_type = std::make_shared<DataTypeString>(cast_set<int>(type.getMaximumLength()),
                                                     PrimitiveType::TYPE_CHAR);
        break;
    case ::orc::TypeKind::DATE:
        data_type = std::make_shared<DataTypeDateV2>();
        break;
    case ::orc::TypeKind::TIMESTAMP:
        data_type = std::make_shared<DataTypeDateTimeV2>(6);
        break;
    case ::orc::TypeKind::TIMESTAMP_INSTANT:
        if (_enable_mapping_timestamp_tz) {
            data_type = std::make_shared<DataTypeTimeStampTz>(6);
        } else {
            data_type = std::make_shared<DataTypeDateTimeV2>(6);
        }
        break;
    case ::orc::TypeKind::DECIMAL:
        data_type = std::make_shared<DataTypeDecimal<TYPE_DECIMAL128I>>(
                type.getPrecision() == 0 ? DECIMAL_PRECISION_FOR_HIVE11
                                         : cast_set<int>(type.getPrecision()),
                type.getPrecision() == 0 ? DECIMAL_SCALE_FOR_HIVE11
                                         : cast_set<int>(type.getScale()));
        break;
    case ::orc::TypeKind::LIST:
        data_type = _convert_list_to_doris_type(type);
        break;
    case ::orc::TypeKind::MAP:
        data_type = _convert_map_to_doris_type(type);
        break;
    case ::orc::TypeKind::STRUCT:
        data_type = _convert_struct_to_doris_type(type);
        break;
    default:
        throw doris::Exception(
                Status::NotSupported("ORC type {} is not supported by new ORC reader",
                                     static_cast<int>(type.getKind())));
    }
    return make_nullable(data_type);
}

DataTypePtr OrcReader::_convert_list_to_doris_type(const ::orc::Type& type) const {
    DORIS_CHECK(type.getKind() == ::orc::TypeKind::LIST);
    DORIS_CHECK(type.getSubtypeCount() == 1);
    const auto* element_type = type.getSubtype(0);
    DORIS_CHECK(element_type != nullptr);
    return std::make_shared<DataTypeArray>(_convert_to_doris_type(*element_type));
}

DataTypePtr OrcReader::_convert_map_to_doris_type(const ::orc::Type& type) const {
    DORIS_CHECK(type.getKind() == ::orc::TypeKind::MAP);
    DORIS_CHECK(type.getSubtypeCount() == 2);
    const auto* key_type = type.getSubtype(0);
    const auto* value_type = type.getSubtype(1);
    DORIS_CHECK(key_type != nullptr);
    DORIS_CHECK(value_type != nullptr);
    return std::make_shared<DataTypeMap>(_convert_to_doris_type(*key_type),
                                         _convert_to_doris_type(*value_type));
}

DataTypePtr OrcReader::_convert_struct_to_doris_type(const ::orc::Type& type) const {
    DORIS_CHECK(type.getKind() == ::orc::TypeKind::STRUCT);
    DataTypes child_types;
    Strings child_names;
    child_types.reserve(type.getSubtypeCount());
    child_names.reserve(type.getSubtypeCount());
    for (uint64_t child_idx = 0; child_idx < type.getSubtypeCount(); ++child_idx) {
        const auto* child_type = type.getSubtype(child_idx);
        DORIS_CHECK(child_type != nullptr);
        child_types.push_back(_convert_to_doris_type(*child_type));
        child_names.push_back(type.getFieldName(child_idx));
    }
    return std::make_shared<DataTypeStruct>(child_types, child_names);
}

Status OrcReader::_fill_schema_field(const ::orc::Type& type, int32_t local_id,
                                     const std::string& field_name,
                                     format::ColumnDefinition* const field) const {
    if (field == nullptr) {
        return Status::InvalidArgument("schema field is null");
    }
    field->local_id = local_id;
    field->name = field_name;
    field->column_type = format::ColumnType::DATA_COLUMN;
    if (type.hasAttributeKey(ORC_ICEBERG_ID_ATTRIBUTE)) {
        const auto iceberg_id = type.getAttributeValue(ORC_ICEBERG_ID_ATTRIBUTE);
        int32_t parsed_id = 0;
        const auto* begin = iceberg_id.data();
        const auto* end = begin + iceberg_id.size();
        const auto [ptr, ec] = std::from_chars(begin, end, parsed_id);
        if (ec != std::errc() || ptr != end) {
            return Status::InvalidArgument("Invalid ORC Iceberg field id '{}' for column {}",
                                           iceberg_id, field_name);
        }
        field->identifier = Field::create_field<TYPE_INT>(parsed_id);
    }
    try {
        field->type = _convert_to_doris_type(type);
    } catch (const doris::Exception& e) {
        return e.to_status();
    }
    field->children.clear();
    switch (type.getKind()) {
    case ::orc::TypeKind::STRUCT:
        return _fill_struct_schema_children(type, field);
    case ::orc::TypeKind::LIST:
        return _fill_list_schema_children(type, field);
    case ::orc::TypeKind::MAP:
        return _fill_map_schema_children(type, field);
    default:
        break;
    }
    return Status::OK();
}

Status OrcReader::_fill_struct_schema_children(const ::orc::Type& type,
                                               format::ColumnDefinition* const field) const {
    DORIS_CHECK(type.getKind() == ::orc::TypeKind::STRUCT);
    field->children.reserve(type.getSubtypeCount());
    for (uint64_t child_idx = 0; child_idx < type.getSubtypeCount(); ++child_idx) {
        const auto* child_type = type.getSubtype(child_idx);
        DORIS_CHECK(child_type != nullptr);
        const auto child_name = type.getFieldName(child_idx);
        format::ColumnDefinition child_field;
        RETURN_IF_ERROR(_fill_schema_field(*child_type, static_cast<int32_t>(child_idx), child_name,
                                           &child_field));
        field->children.push_back(std::move(child_field));
    }
    return Status::OK();
}

Status OrcReader::_fill_list_schema_children(const ::orc::Type& type,
                                             format::ColumnDefinition* const field) const {
    DORIS_CHECK(type.getKind() == ::orc::TypeKind::LIST);
    DORIS_CHECK(type.getSubtypeCount() == 1);
    const auto* element_type = type.getSubtype(0);
    DORIS_CHECK(element_type != nullptr);

    format::ColumnDefinition element_field;
    RETURN_IF_ERROR(_fill_schema_field(*element_type, 0, ORC_LIST_ELEMENT_NAME, &element_field));
    field->children.push_back(std::move(element_field));
    return Status::OK();
}

Status OrcReader::_fill_map_schema_children(const ::orc::Type& type,
                                            format::ColumnDefinition* const field) const {
    DORIS_CHECK(type.getKind() == ::orc::TypeKind::MAP);
    DORIS_CHECK(type.getSubtypeCount() == 2);
    const auto* key_type = type.getSubtype(0);
    const auto* value_type = type.getSubtype(1);
    DORIS_CHECK(key_type != nullptr);
    DORIS_CHECK(value_type != nullptr);

    format::ColumnDefinition key_field;
    RETURN_IF_ERROR(_fill_schema_field(*key_type, 0, ORC_MAP_KEY_NAME, &key_field));
    format::ColumnDefinition value_field;
    RETURN_IF_ERROR(_fill_schema_field(*value_type, 1, ORC_MAP_VALUE_NAME, &value_field));

    field->children.push_back(std::move(key_field));
    field->children.push_back(std::move(value_field));
    return Status::OK();
}

Status OrcReader::get_schema(std::vector<format::ColumnDefinition>* const file_schema) const {
    if (file_schema == nullptr) {
        return Status::InvalidArgument("file_schema is null");
    }
    if (_state == nullptr || _state->root_type == nullptr) {
        return Status::Uninitialized("OrcReader is not open");
    }
    if (_state->root_type->getKind() != ::orc::TypeKind::STRUCT) {
        return Status::NotSupported("ORC reader only supports top-level struct schema");
    }
    file_schema->clear();
    const auto extra_columns = _global_rowid_context.has_value() ? 1 : 0;
    file_schema->reserve(_state->root_type->getSubtypeCount() + extra_columns);
    for (uint64_t child_idx = 0; child_idx < _state->root_type->getSubtypeCount(); ++child_idx) {
        const auto* child_type = _state->root_type->getSubtype(child_idx);
        DORIS_CHECK(child_type != nullptr);
        const auto child_name = _state->root_type->getFieldName(child_idx);
        format::ColumnDefinition field;
        RETURN_IF_ERROR(_fill_schema_field(*child_type, static_cast<int32_t>(child_idx), child_name,
                                           &field));
        file_schema->push_back(std::move(field));
    }
    if (_global_rowid_context.has_value()) {
        file_schema->push_back(nullable_global_rowid_column_definition());
    }
    return Status::OK();
}

std::unique_ptr<format::TableColumnMapper> OrcReader::create_column_mapper(
        format::TableColumnMapperOptions options) const {
    return std::make_unique<format::TableColumnMapper>(std::move(options));
}

Status OrcReader::open(std::shared_ptr<format::FileScanRequest> request) {
    if (_state == nullptr || _state->reader == nullptr || _state->root_type == nullptr) {
        return Status::Uninitialized("OrcReader is not open");
    }
    RETURN_IF_ERROR(format::FileReader::open(std::move(request)));

    if (_request->local_positions.empty()) {
        size_t next_position = 0;
        for (const auto& projection : _request->predicate_columns) {
            if (_request->local_positions
                        .emplace(projection.column_id(), format::LocalIndex(next_position))
                        .second) {
                ++next_position;
            }
        }
        for (const auto& projection : _request->non_predicate_columns) {
            if (_request->local_positions
                        .emplace(projection.column_id(), format::LocalIndex(next_position))
                        .second) {
                ++next_position;
            }
        }
    }

    _state->read_columns.clear();
    _state->read_columns.reserve(_request->predicate_columns.size() +
                                 _request->non_predicate_columns.size());
    for (const auto& projection : _request->predicate_columns) {
        _state->read_columns.push_back(projection.column_id());
    }
    for (const auto& projection : _request->non_predicate_columns) {
        _state->read_columns.push_back(projection.column_id());
    }
    DCHECK(local_column_ids_are_unique(_request->predicate_columns));
    DCHECK(local_column_ids_are_unique(_request->non_predicate_columns));
    std::sort(_state->read_columns.begin(), _state->read_columns.end());
    _state->read_columns.erase(
            std::unique(_state->read_columns.begin(), _state->read_columns.end()),
            _state->read_columns.end());

    _state->orc_lazy_read_enabled = _can_apply_orc_lazy_callback();

    RETURN_IF_ERROR(_configure_row_reader_projection());
    RETURN_IF_ERROR(set_orc_reader_timezone(_state->timezone, &_state->row_reader_options));
    _state->row_reader_options.setEnableLazyDecoding(_state->orc_lazy_read_enabled);
    _state->row_reader_options.setUseTightNumericVector(false);

    RETURN_IF_ERROR(_init_search_argument_from_local_filters());

    // Seed the split byte range so this scanner only reads its own stripes. When SARG
    // pruning applies, _apply_current_stripe_range() overwrites this with the (already
    // split-constrained) stripe ranges; otherwise this seeded range governs selection.
    _apply_split_range();

    RETURN_IF_ERROR(_select_stripe_ranges_by_statistics());
    if (_state->stripe_pruning_applied && _state->selected_stripe_ranges.empty()) {
        _eof = true;
        return Status::OK();
    }
    _apply_current_stripe_range();

    RETURN_IF_ERROR(_create_row_reader());
    _eof = get_total_rows() == 0;
    return Status::OK();
}

bool OrcReader::_can_apply_orc_lazy_callback() const {
    if (!_filter_has_row_level_predicates() || _request->predicate_columns.empty() ||
        _request->non_predicate_columns.empty()) {
        return false;
    }
    for (const auto& projection : _request->non_predicate_columns) {
        if (is_virtual_column(projection.column_id())) {
            continue;
        }
        // ORC lazy decoding returns follower complex columns with selected-row layout.
        // Decode pruned complex projections after full batch materialization so nested offsets
        // and child values stay aligned.
        if (has_pruned_projection(projection)) {
            return false;
        }
    }
    bool has_physical_read_column = false;
    for (const auto file_column_id : _state->read_columns) {
        if (is_virtual_column(file_column_id)) {
            continue;
        }
        has_physical_read_column = true;
        if (find_request_projection(*_request, file_column_id) == nullptr) {
            return false;
        }
    }
    if (!has_physical_read_column) {
        return false;
    }
    std::set<format::LocalColumnId> decoded_columns;
    for (const auto& projection : _request->predicate_columns) {
        const auto file_column_id = projection.column_id();
        if (is_global_rowid_column(file_column_id)) {
            return false;
        }
        decoded_columns.insert(file_column_id);
    }
    if (decoded_columns.empty()) {
        return false;
    }
    return _can_filter_with_decoded_columns(decoded_columns);
}

Status OrcReader::_configure_row_reader_projection() {
    const auto num_fields = static_cast<int32_t>(_state->root_type->getSubtypeCount());
    bool has_complex_projection = false;
    for (const auto file_column_id : _state->read_columns) {
        if (is_virtual_column(file_column_id)) {
            DORIS_CHECK(_request->local_positions.contains(file_column_id));
            continue;
        }
        DORIS_CHECK(file_column_id.is_valid() && file_column_id.value() < num_fields);
        DORIS_CHECK(_request->local_positions.contains(file_column_id));
        const auto* projection = find_request_projection(*_request, file_column_id);
        DORIS_CHECK(projection != nullptr);
        has_complex_projection = has_complex_projection || has_pruned_projection(*projection);
    }
    if (!has_complex_projection) {
        std::list<uint64_t> include_columns;
        for (const auto file_column_id : _state->read_columns) {
            if (is_virtual_column(file_column_id)) {
                continue;
            }
            include_columns.push_back(static_cast<uint64_t>(file_column_id.value()));
        }
        _state->row_reader_options.include(include_columns);
        if (_state->orc_lazy_read_enabled) {
            std::list<std::string> filter_columns;
            for (const auto& projection : _request->predicate_columns) {
                if (is_row_position_column(projection.column_id())) {
                    continue;
                }
                DORIS_CHECK(!is_virtual_column(projection.column_id()));
                // ORC RowReader lazy filter uses column names (or type ids) to mark leaders.
                // Passing field indexes does not activate the callback in this ORC build.
                filter_columns.push_back(
                        _state->root_type->getFieldName(projection.column_id().value()));
            }
            if (filter_columns.empty()) {
                DORIS_CHECK(!include_columns.empty());
                filter_columns.push_back(_state->root_type->getFieldName(include_columns.front()));
            }
            _state->row_reader_options.filter(filter_columns);
        }
        return Status::OK();
    }

    std::set<uint64_t> include_type_ids;
    include_type_ids.insert(_state->root_type->getColumnId());
    for (const auto file_column_id : _state->read_columns) {
        if (is_virtual_column(file_column_id)) {
            continue;
        }
        const auto* type =
                _state->root_type->getSubtype(static_cast<uint64_t>(file_column_id.value()));
        DORIS_CHECK(type != nullptr);
        const auto* projection = find_request_projection(*_request, file_column_id);
        DORIS_CHECK(projection != nullptr);
        if (!has_pruned_projection(*projection)) {
            collect_type_and_descendant_ids(*type, &include_type_ids);
            continue;
        }
        RETURN_IF_ERROR(collect_projected_type_ids(*type, *projection, &include_type_ids));
    }
    std::list<uint64_t> include_type_id_list(include_type_ids.begin(), include_type_ids.end());
    _state->row_reader_options.includeTypes(include_type_id_list);
    if (_state->orc_lazy_read_enabled) {
        std::set<uint64_t> filter_type_ids;
        RETURN_IF_ERROR(collect_lazy_filter_type_ids(
                *_state->root_type, _request->predicate_columns, &filter_type_ids));
        DORIS_CHECK(!filter_type_ids.empty());
        std::list<uint64_t> filter_type_id_list(filter_type_ids.begin(), filter_type_ids.end());
        _state->row_reader_options.filterTypes(filter_type_id_list);
    }
    return Status::OK();
}

Status OrcReader::_init_search_argument_from_local_filters() {
    if (_request->conjuncts.empty()) {
        return Status::OK();
    }

    try {
        auto builder = ::orc::SearchArgumentFactory::newBuilder();
        bool has_pushdown = false;
        builder->startAnd();
        for (const auto& conjunct : _request->conjuncts) {
            if (conjunct == nullptr) {
                continue;
            }
            has_pushdown =
                    build_orc_search_argument(*_request, *_state->root_type, _state->timezone_obj,
                                              conjunct->root(), builder) ||
                    has_pushdown;
        }
        if (!has_pushdown) {
            return Status::OK();
        }
        builder->end();
        _state->row_reader_options.searchArgument(builder->build());
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to build ORC search argument: {}", e.what());
    }
    return Status::OK();
}

void OrcReader::_split_byte_window(uint64_t* start, uint64_t* end) const {
    const int64_t range_start = _file_description->range_start_offset;
    const int64_t range_size = _file_description->range_size;
    DORIS_CHECK(range_start >= 0);
    *start = static_cast<uint64_t>(range_start);
    if (range_size < 0) {
        // Unset sentinel: read the whole file.
        *end = std::numeric_limits<uint64_t>::max();
        return;
    }
    DORIS_CHECK(range_size <= std::numeric_limits<int64_t>::max() - range_start);
    const int64_t range_end = range_start + range_size;
    DORIS_CHECK(range_end >= range_start);
    *end = static_cast<uint64_t>(range_end);
}

void OrcReader::_apply_split_range() {
    uint64_t start = 0;
    uint64_t end = std::numeric_limits<uint64_t>::max();
    _split_byte_window(&start, &end);
    if (start == 0 && end == std::numeric_limits<uint64_t>::max()) {
        // Whole file: keep ORC library defaults and avoid uint64 overflow on (start + length).
        return;
    }
    _state->row_reader_options.range(start, end - start);
}

// ORC RowReader ranges are continuous, so non-adjacent surviving stripes are
// compacted into separate ranges.
Status OrcReader::_select_stripe_ranges_by_statistics() {
    _state->selected_stripe_ranges.clear();
    _state->current_stripe_range = 0;
    _state->stripe_pruning_applied = false;
    const bool has_search_argument = _state->row_reader_options.getSearchArgument() != nullptr;
    if (!has_search_argument) {
        return Status::OK();
    }

    const auto stripe_count = _state->reader->getNumberOfStripes();
    if (stripe_count == 0) {
        return Status::OK();
    }

    std::vector<int> sarg_needed_stripes;
    try {
        sarg_needed_stripes = _state->reader->getNeedReadStripes(_state->row_reader_options);
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to evaluate ORC search argument: {}", e.what());
    }

    // Only stripes whose offset falls in this split's byte window belong to this scanner
    // (matching ORC's own inclusion test). getNeedReadStripes() ignores the range, so
    // out-of-split stripes must be excluded here and must NOT be counted in the filtered
    // statistics (they are handled by other splits).
    uint64_t split_start = 0;
    uint64_t split_end = std::numeric_limits<uint64_t>::max();
    _split_byte_window(&split_start, &split_end);

    std::vector<uint64_t> selected_stripes;
    selected_stripes.reserve(stripe_count);
    int64_t filtered_stripes = 0;
    int64_t filtered_rows = 0;
    int64_t filtered_bytes = 0;
    for (uint64_t stripe_index = 0; stripe_index < stripe_count; ++stripe_index) {
        uint64_t stripe_offset = 0;
        try {
            stripe_offset = _state->reader->getStripe(stripe_index)->getOffset();
        } catch (const std::exception& e) {
            return Status::InternalError("Failed to read ORC stripe info: {}", e.what());
        }
        if (stripe_offset < split_start || stripe_offset >= split_end) {
            // Belongs to another split; do not select or count it.
            continue;
        }

        bool drop = false;
        if (stripe_index < sarg_needed_stripes.size() && sarg_needed_stripes[stripe_index] == 0) {
            drop = true;
        }
        if (!drop) {
            selected_stripes.push_back(stripe_index);
            continue;
        }

        ++filtered_stripes;
        try {
            const auto stripe = _state->reader->getStripe(stripe_index);
            filtered_rows += cast_set<int64_t>(stripe->getNumberOfRows());
            filtered_bytes += cast_set<int64_t>(stripe->getLength());
        } catch (const std::exception&) {
        }
    }

    if (filtered_stripes == 0) {
        return Status::OK();
    }

    _state->stripe_pruning_applied = true;
    _reader_statistics.filtered_row_groups = cast_set<int32_t>(filtered_stripes);
    _reader_statistics.filtered_row_groups_by_min_max = cast_set<int32_t>(filtered_stripes);
    _reader_statistics.filtered_group_rows = filtered_rows;
    _reader_statistics.filtered_bytes = filtered_bytes;
    _reader_statistics.read_row_groups = cast_set<int32_t>(selected_stripes.size());
    if (selected_stripes.empty()) {
        return Status::OK();
    }

    auto append_stripe_range = [&](uint64_t first_stripe, uint64_t last_stripe) -> Status {
        DORIS_CHECK(first_stripe < last_stripe);
        try {
            const auto first = _state->reader->getStripe(first_stripe);
            const auto last = _state->reader->getStripe(last_stripe - 1);
            const auto offset = first->getOffset();
            const auto end_offset = last->getOffset() + last->getLength();
            DORIS_CHECK(end_offset > offset);
            _state->selected_stripe_ranges.push_back(OrcReaderScanState::StripeRange {
                    .first_stripe = first_stripe,
                    .last_stripe = last_stripe,
                    .offset = offset,
                    .length = end_offset - offset,
            });
        } catch (const std::exception& e) {
            return Status::InternalError("Failed to build ORC stripe read range: {}", e.what());
        }
        return Status::OK();
    };

    uint64_t range_first = selected_stripes.front();
    uint64_t previous = range_first;
    for (size_t idx = 1; idx < selected_stripes.size(); ++idx) {
        const auto stripe_index = selected_stripes[idx];
        if (stripe_index == previous + 1) {
            previous = stripe_index;
            continue;
        }
        RETURN_IF_ERROR(append_stripe_range(range_first, previous + 1));
        range_first = stripe_index;
        previous = stripe_index;
    }
    RETURN_IF_ERROR(append_stripe_range(range_first, previous + 1));
    return Status::OK();
}

void OrcReader::_apply_current_stripe_range() {
    if (!_state->stripe_pruning_applied || _state->selected_stripe_ranges.empty()) {
        return;
    }
    DORIS_CHECK(_state->current_stripe_range < _state->selected_stripe_ranges.size());
    const auto& stripe_range = _state->selected_stripe_ranges[_state->current_stripe_range];
    _state->row_reader_options.range(stripe_range.offset, stripe_range.length);
}

Status OrcReader::_advance_to_next_stripe_range(bool* advanced) {
    DORIS_CHECK(advanced != nullptr);
    *advanced = false;
    if (!_state->stripe_pruning_applied || _state->selected_stripe_ranges.empty() ||
        _state->current_stripe_range + 1 >= _state->selected_stripe_ranges.size()) {
        return Status::OK();
    }
    ++_state->current_stripe_range;
    _apply_current_stripe_range();
    RETURN_IF_ERROR(_create_row_reader());
    *advanced = true;
    return Status::OK();
}

Status OrcReader::_create_row_reader() {
    try {
        if (_state->orc_lazy_read_enabled && _orc_filter == nullptr) {
            _orc_filter = std::make_unique<OrcFilterImpl>(this);
        }
        _state->row_reader = _state->reader->createRowReader(
                _state->row_reader_options,
                _state->orc_lazy_read_enabled ? _orc_filter.get() : nullptr);
        _state->selected_type = &_state->row_reader->getSelectedType();
        DORIS_CHECK(_state->selected_type->getKind() == ::orc::TypeKind::STRUCT);
        _state->batch = _state->row_reader->createRowBatch(DEFAULT_ORC_READ_BATCH_SIZE);
        _state->orc_lazy_selection_valid = false;
        _state->orc_lazy_selected_rows.clear();
        _state->orc_lazy_input_rows = 0;
        const uint64_t file_total_rows = _state->reader->getNumberOfRows();
        const auto initial_row_number = _state->row_reader->getRowNumber();
        _state->row_reader_range_rows = _state->row_reader->getNumberOfRows();
        if (initial_row_number == std::numeric_limits<uint64_t>::max()) {
            _state->row_reader_range_first_row = 0;
        } else if (initial_row_number >= file_total_rows) {
            _state->row_reader_range_first_row = file_total_rows;
        } else {
            _state->row_reader_range_first_row = initial_row_number + 1;
        }
        DORIS_CHECK(_state->row_reader_range_first_row <= file_total_rows);
        DORIS_CHECK(_state->row_reader_range_rows <=
                    file_total_rows - _state->row_reader_range_first_row);
        _state->row_reader_range_end_row =
                _state->row_reader_range_first_row + _state->row_reader_range_rows;
        _state->orc_lazy_next_batch_first_row = _state->row_reader_range_first_row;
        _state->condition_cache_next_row = _state->orc_lazy_next_batch_first_row;
        _state->column_to_selected_batch_index.clear();
        size_t physical_read_column_count = 0;
        for (const auto file_column_id : _state->read_columns) {
            physical_read_column_count += !is_virtual_column(file_column_id);
        }
        for (uint64_t selected_idx = 0; selected_idx < _state->selected_type->getSubtypeCount();
             ++selected_idx) {
            const auto field_name = _state->selected_type->getFieldName(selected_idx);
            for (const auto file_column_id : _state->read_columns) {
                if (is_virtual_column(file_column_id)) {
                    continue;
                }
                if (field_name == _state->root_type->getFieldName(file_column_id.value())) {
                    _state->column_to_selected_batch_index.emplace(
                            file_column_id, static_cast<size_t>(selected_idx));
                    break;
                }
            }
        }
        DORIS_CHECK(_state->column_to_selected_batch_index.size() == physical_read_column_count);
        _state->row_reader_created = true;
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to create ORC row reader: {}", e.what());
    }
    return Status::OK();
}

void OrcReader::_skip_condition_cache_false_granules(size_t* rows, bool* eof) {
    DORIS_CHECK(rows != nullptr);
    DORIS_CHECK(eof != nullptr);
    if (_state->condition_cache_ctx == nullptr || !_state->condition_cache_ctx->is_hit) {
        return;
    }
    DORIS_CHECK(_state->condition_cache_ctx->filter_result != nullptr);
    const auto base_granule = _state->condition_cache_ctx->base_granule;
    const auto& cache = *_state->condition_cache_ctx->filter_result;
    constexpr uint64_t granule_size = ConditionCacheContext::GRANULE_SIZE;
    const uint64_t file_total_rows = _state->reader->getNumberOfRows();
    DORIS_CHECK(_state->condition_cache_next_row <= file_total_rows);

    const auto current_granule =
            static_cast<int64_t>(_state->condition_cache_next_row / granule_size);
    const auto cache_idx_offset = current_granule - base_granule;
    if (cache_idx_offset < 0 || static_cast<size_t>(cache_idx_offset) >= cache.size()) {
        return;
    }
    size_t cache_idx = static_cast<size_t>(cache_idx_offset);
    while (cache_idx < cache.size() && !cache[cache_idx]) {
        ++cache_idx;
    }
    if (cache_idx >= cache.size()) {
        if (_state->row_reader_range_end_row <= _state->condition_cache_next_row) {
            return;
        }
        const auto last_range_granule =
                static_cast<int64_t>((_state->row_reader_range_end_row - 1) / granule_size);
        const auto last_cache_idx_offset = last_range_granule - base_granule;
        if (last_cache_idx_offset < 0 ||
            static_cast<size_t>(last_cache_idx_offset) >= cache.size()) {
            return;
        }
        _state->row_reader->seekToRow(_state->row_reader_range_end_row);
        if (_io_ctx != nullptr) {
            _io_ctx->condition_cache_filtered_rows +=
                    _state->row_reader_range_end_row - _state->condition_cache_next_row;
        }
        _state->condition_cache_next_row = _state->row_reader_range_end_row;
        _state->orc_lazy_next_batch_first_row = _state->row_reader_range_end_row;
        return;
    }

    const auto target_granule_offset = base_granule + static_cast<int64_t>(cache_idx);
    DORIS_CHECK(target_granule_offset >= 0);
    const auto target_granule = static_cast<uint64_t>(target_granule_offset);
    const uint64_t target_row = target_granule * granule_size;
    if (target_row >= _state->row_reader_range_end_row) {
        if (_state->row_reader_range_end_row > _state->condition_cache_next_row) {
            _state->row_reader->seekToRow(_state->row_reader_range_end_row);
            if (_io_ctx != nullptr) {
                _io_ctx->condition_cache_filtered_rows +=
                        _state->row_reader_range_end_row - _state->condition_cache_next_row;
            }
            _state->condition_cache_next_row = _state->row_reader_range_end_row;
            _state->orc_lazy_next_batch_first_row = _state->row_reader_range_end_row;
        }
        return;
    }
    if (target_row > _state->condition_cache_next_row) {
        DORIS_CHECK(target_row <= file_total_rows);
        _state->row_reader->seekToRow(target_row);
        if (_io_ctx != nullptr) {
            _io_ctx->condition_cache_filtered_rows += target_row - _state->condition_cache_next_row;
        }
        _state->condition_cache_next_row = target_row;
        _state->orc_lazy_next_batch_first_row = target_row;
    }
}

void OrcReader::_mark_condition_cache_surviving_rows(const IColumn::Filter& keep_filter,
                                                     size_t rows) const {
    DORIS_CHECK(keep_filter.size() == rows);
    if (_state->condition_cache_ctx == nullptr || _state->condition_cache_ctx->is_hit) {
        return;
    }
    DORIS_CHECK(_state->condition_cache_ctx->filter_result != nullptr);
    const auto base_granule = _state->condition_cache_ctx->base_granule;
    auto& cache = *_state->condition_cache_ctx->filter_result;
    constexpr uint64_t granule_size = ConditionCacheContext::GRANULE_SIZE;
    for (size_t row = 0; row < rows; ++row) {
        if (keep_filter[row] == 0) {
            continue;
        }
        const auto granule =
                static_cast<int64_t>((_state->current_batch_first_row + row) / granule_size);
        const auto cache_idx = granule - base_granule;
        if (cache_idx >= 0 && static_cast<size_t>(cache_idx) < cache.size()) {
            cache[static_cast<size_t>(cache_idx)] = true;
        }
    }
}

Status OrcReader::_filter_orc_batch(::orc::ColumnVectorBatch& data, uint16_t* sel, uint16_t size,
                                    void* /*arg*/) {
    if (!_state->orc_lazy_read_enabled || sel == nullptr || size == 0) {
        data.numElements = size;
        return Status::OK();
    }
    if (size > DEFAULT_ORC_READ_BATCH_SIZE) {
        return Status::InvalidArgument("ORC lazy filter batch size {} exceeds {}", size,
                                       DEFAULT_ORC_READ_BATCH_SIZE);
    }

    auto* struct_batch = dynamic_cast<::orc::StructVectorBatch*>(&data);
    if (struct_batch == nullptr) {
        return Status::InternalError("New ORC lazy filter expects struct row batch");
    }

    std::vector<std::optional<format::LocalColumnId>> position_to_column(
            _request->local_positions.size());
    for (const auto& [file_column_id, local_position] : _request->local_positions) {
        const auto position = local_position.value();
        if (position >= position_to_column.size()) {
            return Status::InvalidArgument("ORC scan local position {} is out of range {}",
                                           position, position_to_column.size());
        }
        if (position_to_column[position].has_value()) {
            return Status::InvalidArgument("ORC scan local position {} is duplicated", position);
        }
        position_to_column[position] = file_column_id;
    }

    Block filter_block;
    filter_block.reserve(position_to_column.size());
    for (size_t position = 0; position < position_to_column.size(); ++position) {
        if (!position_to_column[position].has_value()) {
            return Status::InvalidArgument("ORC scan local positions are not dense at {}",
                                           position);
        }
        const auto file_column_id = *position_to_column[position];
        if (is_row_position_column(file_column_id)) {
            auto field = row_position_column_definition();
            filter_block.insert({field.type->create_column(), field.type, field.name});
            continue;
        }
        if (is_global_rowid_column(file_column_id)) {
            auto field = nullable_global_rowid_column_definition();
            filter_block.insert({field.type->create_column(), field.type, field.name});
            continue;
        }
        const auto batch_index_it = _state->column_to_selected_batch_index.find(file_column_id);
        DORIS_CHECK(batch_index_it != _state->column_to_selected_batch_index.end());
        const auto* selected_type = _state->selected_type->getSubtype(batch_index_it->second);
        DORIS_CHECK(selected_type != nullptr);
        auto column_type = _convert_to_doris_type(*selected_type);
        if (column_type == nullptr) {
            return Status::NotSupported("ORC type {} is not supported by new ORC reader",
                                        static_cast<int>(selected_type->getKind()));
        }
        filter_block.insert({column_type->create_column(), column_type,
                             _state->root_type->getFieldName(file_column_id.value())});
    }

    _state->current_batch_first_row = _state->orc_lazy_next_batch_first_row;
    _state->orc_lazy_next_batch_first_row += size;
    _state->condition_cache_next_row = _state->current_batch_first_row + size;
    std::set<format::LocalColumnId> decoded_columns;
    RETURN_IF_ERROR(_decode_columns(*struct_batch, _request->predicate_columns, size, &filter_block,
                                    &decoded_columns));

    IColumn::Filter keep_filter(size, 1);
    RETURN_IF_ERROR(_build_keep_filter(&filter_block, size, &keep_filter));
    _mark_condition_cache_surviving_rows(keep_filter, size);

    _state->orc_lazy_selected_rows.clear();
    _state->orc_lazy_selected_rows.reserve(size);
    uint16_t selected_rows = 0;
    for (uint16_t row = 0; row < size; ++row) {
        if (keep_filter[row] == 0) {
            continue;
        }
        sel[selected_rows++] = row;
        _state->orc_lazy_selected_rows.push_back(row);
    }
    _state->orc_lazy_input_rows = size;
    _state->orc_lazy_selection_valid = true;
    data.numElements = selected_rows;
    const auto filtered_rows = cast_set<int64_t>(size - selected_rows);
    _reader_statistics.lazy_read_filtered_rows += filtered_rows;
    return Status::OK();
}

Status OrcReader::_decode_column(const ::orc::Type& file_type, const ::orc::Type& selected_type,
                                 const ::orc::ColumnVectorBatch& batch, MutableColumnPtr& column,
                                 size_t rows, const std::vector<size_t>* selected_rows) const {
    DORIS_CHECK(file_type.getKind() == selected_type.getKind());
    DORIS_CHECK(column->is_nullable());
    const auto output_rows = decode_row_count(rows, selected_rows);
    if (output_rows == 0) {
        return Status::OK();
    }
    const auto column_type = _convert_to_doris_type(selected_type);

    switch (file_type.getKind()) {
    case ::orc::TypeKind::BOOLEAN:
        return decode_boolean_values_with_serde(column_type, *column, batch, rows, selected_rows);
    case ::orc::TypeKind::BYTE:
    case ::orc::TypeKind::SHORT:
    case ::orc::TypeKind::INT:
    case ::orc::TypeKind::LONG:
        return decode_fixed_values_with_serde<::orc::LongVectorBatch, int64_t>(
                column_type, *column, batch, rows, selected_rows, DecodedValueKind::INT64);
    case ::orc::TypeKind::FLOAT:
        return decode_float_values_with_serde(column_type, *column, batch, rows, selected_rows);
    case ::orc::TypeKind::DOUBLE:
        return decode_fixed_values_with_serde<::orc::DoubleVectorBatch, double>(
                column_type, *column, batch, rows, selected_rows, DecodedValueKind::DOUBLE);
    case ::orc::TypeKind::STRING:
    case ::orc::TypeKind::BINARY:
    case ::orc::TypeKind::VARCHAR:
    case ::orc::TypeKind::CHAR:
        return decode_string_values_with_serde(column_type, *column, file_type, batch, rows,
                                               selected_rows);
    case ::orc::TypeKind::DATE:
        return decode_date_values_with_serde(column_type, *column, batch, rows, selected_rows);
    case ::orc::TypeKind::DECIMAL:
        return decode_decimal_values_with_serde(column_type, *column, file_type, batch, rows,
                                                selected_rows);
    default:
        break;
    }

    auto& nullable_column = assert_cast<ColumnNullable&>(*column);
    auto nested_column = nullable_column.get_nested_column_ptr();
    auto& null_map = nullable_column.get_null_map_data();
    const size_t old_size = null_map.size();
    null_map.resize(old_size + output_rows);
    if (batch.hasNulls) {
        for (size_t row = 0; row < output_rows; ++row) {
            null_map[old_size + row] = !batch.notNull[source_row_at(row, selected_rows)];
        }
    } else {
        std::memset(null_map.data() + old_size, 0, output_rows);
    }

    switch (file_type.getKind()) {
    case ::orc::TypeKind::TIMESTAMP:
        // ORC timestamp decoding currently applies reader timezone directly to epoch seconds.
        // Keep this path until DecodedColumnView exposes the same format-level semantics.
        return _decode_timestamp_column(batch, _state->timezone_obj, nested_column, rows,
                                        selected_rows);
    case ::orc::TypeKind::TIMESTAMP_INSTANT:
        if (_enable_mapping_timestamp_tz) {
            return _decode_timestamp_tz_column(batch, nested_column, rows, selected_rows);
        }
        return _decode_timestamp_column(batch, _state->timezone_obj, nested_column, rows,
                                        selected_rows);
    case ::orc::TypeKind::LIST:
        return _decode_list_column(file_type, selected_type, batch, nested_column, rows,
                                   selected_rows);
    case ::orc::TypeKind::MAP:
        return _decode_map_column(file_type, selected_type, batch, nested_column, rows,
                                  selected_rows);
    case ::orc::TypeKind::STRUCT:
        return _decode_struct_column(file_type, selected_type, batch, nested_column, rows,
                                     selected_rows);
    default:
        return Status::NotSupported("ORC type {} is not supported by new ORC reader",
                                    static_cast<int>(file_type.getKind()));
    }
}

Status OrcReader::_decode_timestamp_column(const ::orc::ColumnVectorBatch& batch,
                                           const cctz::time_zone& timezone,
                                           MutableColumnPtr& nested_column, size_t rows,
                                           const std::vector<size_t>* selected_rows) const {
    const auto* orc_batch = dynamic_cast<const ::orc::TimestampVectorBatch*>(&batch);
    if (orc_batch == nullptr) {
        return Status::InternalError("Unexpected ORC timestamp batch type {}", batch.toString());
    }
    auto& data = assert_cast<ColumnDateTimeV2&>(*nested_column).get_data();
    const size_t old_data_size = data.size();
    const auto output_rows = decode_row_count(rows, selected_rows);
    data.resize(old_data_size + output_rows);
    for (size_t row = 0; row < output_rows; ++row) {
        const auto source_row = source_row_at(row, selected_rows);
        if (is_null_at(batch, source_row)) {
            data[old_data_size + row] = DateV2Value<DateTimeV2ValueType> {};
            continue;
        }
        auto& value =
                reinterpret_cast<DateV2Value<DateTimeV2ValueType>&>(data[old_data_size + row]);
        value.from_unixtime(orc_batch->data[source_row], timezone);
        value.set_microsecond(cast_set<uint64_t>(orc_batch->nanoseconds[source_row] / 1000));
    }
    return Status::OK();
}

Status OrcReader::_decode_timestamp_tz_column(const ::orc::ColumnVectorBatch& batch,
                                              MutableColumnPtr& nested_column, size_t rows,
                                              const std::vector<size_t>* selected_rows) const {
    const auto* orc_batch = dynamic_cast<const ::orc::TimestampVectorBatch*>(&batch);
    if (orc_batch == nullptr) {
        return Status::InternalError("Unexpected ORC timestamp batch type {}", batch.toString());
    }
    auto& data = assert_cast<ColumnTimeStampTz&>(*nested_column).get_data();
    const size_t old_data_size = data.size();
    const auto output_rows = decode_row_count(rows, selected_rows);
    data.resize(old_data_size + output_rows);
    static const auto utc_time_zone = cctz::utc_time_zone();
    for (size_t row = 0; row < output_rows; ++row) {
        const auto source_row = source_row_at(row, selected_rows);
        if (is_null_at(batch, source_row)) {
            data[old_data_size + row] = TimestampTzValue {};
            continue;
        }
        auto& value = data[old_data_size + row];
        value.from_unixtime(orc_batch->data[source_row], utc_time_zone);
        value.set_microsecond(cast_set<uint64_t>(orc_batch->nanoseconds[source_row] / 1000));
    }
    return Status::OK();
}

Status OrcReader::_decode_list_column(const ::orc::Type& file_type,
                                      const ::orc::Type& selected_type,
                                      const ::orc::ColumnVectorBatch& batch,
                                      MutableColumnPtr& nested_column, size_t rows,
                                      const std::vector<size_t>* selected_rows) const {
    const auto* orc_list = dynamic_cast<const ::orc::ListVectorBatch*>(&batch);
    if (orc_list == nullptr) {
        return Status::InternalError("Unexpected ORC list batch type {}", batch.toString());
    }
    DORIS_CHECK(file_type.getSubtypeCount() == 1);
    DORIS_CHECK(selected_type.getSubtypeCount() == 1);
    DORIS_CHECK(orc_list->elements != nullptr);
    const auto* file_element_type = file_type.getSubtype(0);
    const auto* selected_element_type = selected_type.getSubtype(0);
    DORIS_CHECK(file_element_type != nullptr);
    DORIS_CHECK(selected_element_type != nullptr);

    auto& array_column = assert_cast<ColumnArray&>(*nested_column);
    size_t element_size = 0;
    std::vector<size_t> element_selection;
    RETURN_IF_ERROR(append_orc_offsets(array_column.get_offsets(), orc_list->offsets, rows,
                                       &element_size, selected_rows, &element_selection));
    auto element_column = array_column.get_data_ptr()->assert_mutable();
    const auto child_rows = selected_rows == nullptr
                                    ? element_size
                                    : static_cast<size_t>(orc_list->elements->numElements);
    const auto* child_selection = selected_rows == nullptr ? nullptr : &element_selection;
    RETURN_IF_ERROR(_decode_column(*file_element_type, *selected_element_type, *orc_list->elements,
                                   element_column, child_rows, child_selection));
    array_column.get_data_ptr() = std::move(element_column);
    return Status::OK();
}

Status OrcReader::_decode_map_column(const ::orc::Type& file_type, const ::orc::Type& selected_type,
                                     const ::orc::ColumnVectorBatch& batch,
                                     MutableColumnPtr& nested_column, size_t rows,
                                     const std::vector<size_t>* selected_rows) const {
    const auto* orc_map = dynamic_cast<const ::orc::MapVectorBatch*>(&batch);
    if (orc_map == nullptr) {
        return Status::InternalError("Unexpected ORC map batch type {}", batch.toString());
    }
    DORIS_CHECK(file_type.getSubtypeCount() == 2);
    DORIS_CHECK(selected_type.getSubtypeCount() == 2);
    DORIS_CHECK(orc_map->keys != nullptr);
    DORIS_CHECK(orc_map->elements != nullptr);
    auto& map_column = assert_cast<ColumnMap&>(*nested_column);
    size_t element_size = 0;
    std::vector<size_t> element_selection;
    RETURN_IF_ERROR(append_orc_offsets(map_column.get_offsets(), orc_map->offsets, rows,
                                       &element_size, selected_rows, &element_selection));

    auto key_column = map_column.get_keys_ptr()->assert_mutable();
    const auto* file_key_type = file_type.getSubtype(0);
    const auto* selected_key_type = selected_type.getSubtype(0);
    DORIS_CHECK(file_key_type != nullptr);
    DORIS_CHECK(selected_key_type != nullptr);
    const auto child_rows = selected_rows == nullptr
                                    ? element_size
                                    : static_cast<size_t>(orc_map->keys->numElements);
    const auto* child_selection = selected_rows == nullptr ? nullptr : &element_selection;
    RETURN_IF_ERROR(_decode_column(*file_key_type, *selected_key_type, *orc_map->keys, key_column,
                                   child_rows, child_selection));
    map_column.get_keys_ptr() = std::move(key_column);
    auto value_column = map_column.get_values_ptr()->assert_mutable();
    const auto* file_value_type = file_type.getSubtype(1);
    const auto* selected_value_type = selected_type.getSubtype(1);
    DORIS_CHECK(file_value_type != nullptr);
    DORIS_CHECK(selected_value_type != nullptr);
    RETURN_IF_ERROR(_decode_column(
            *file_value_type, *selected_value_type, *orc_map->elements, value_column,
            selected_rows == nullptr ? element_size
                                     : static_cast<size_t>(orc_map->elements->numElements),
            child_selection));
    map_column.get_values_ptr() = std::move(value_column);
    return Status::OK();
}

Status OrcReader::_decode_struct_column(const ::orc::Type& file_type,
                                        const ::orc::Type& selected_type,
                                        const ::orc::ColumnVectorBatch& batch,
                                        MutableColumnPtr& nested_column, size_t rows,
                                        const std::vector<size_t>* selected_rows) const {
    const auto* orc_struct = dynamic_cast<const ::orc::StructVectorBatch*>(&batch);
    if (orc_struct == nullptr) {
        return Status::InternalError("Unexpected ORC struct batch type {}", batch.toString());
    }
    DORIS_CHECK(selected_type.getSubtypeCount() == orc_struct->fields.size());
    auto& struct_column = assert_cast<ColumnStruct&>(*nested_column);
    DORIS_CHECK(struct_column.tuple_size() == selected_type.getSubtypeCount());

    for (uint64_t selected_idx = 0; selected_idx < selected_type.getSubtypeCount();
         ++selected_idx) {
        const auto field_name = selected_type.getFieldName(selected_idx);
        const auto file_child_idx = find_struct_child_index(file_type, field_name);
        if (file_child_idx < 0) {
            return Status::InternalError("Selected ORC field {} is not in file struct", field_name);
        }
        const auto* file_child_type = file_type.getSubtype(static_cast<uint64_t>(file_child_idx));
        const auto* selected_child_type = selected_type.getSubtype(selected_idx);
        DORIS_CHECK(file_child_type != nullptr);
        DORIS_CHECK(selected_child_type != nullptr);
        DORIS_CHECK(selected_idx < orc_struct->fields.size());
        auto child_column =
                struct_column.get_column_ptr(static_cast<size_t>(selected_idx))->assert_mutable();
        RETURN_IF_ERROR(_decode_column(*file_child_type, *selected_child_type,
                                       *orc_struct->fields[selected_idx], child_column, rows,
                                       selected_rows));
        struct_column.get_column_ptr(static_cast<size_t>(selected_idx)) = std::move(child_column);
    }
    return Status::OK();
}

Status OrcReader::get_block(Block* file_block, size_t* rows, bool* eof) {
    DORIS_CHECK(file_block != nullptr);
    DORIS_CHECK(rows != nullptr);
    DORIS_CHECK(eof != nullptr);
    if (_state == nullptr) {
        return Status::Uninitialized("OrcReader is not open");
    }
    *rows = 0;
    file_block->clear_column_data(file_block->columns());
    if (_io_ctx != nullptr && _io_ctx->should_stop) {
        *eof = true;
        _eof = true;
        return Status::OK();
    }
    if (_eof) {
        *eof = true;
        return Status::OK();
    }
    if (!_state->row_reader_created || _state->batch == nullptr) {
        return Status::Uninitialized("OrcReader is not open");
    }

    bool has_next = false;
    while (true) {
        _skip_condition_cache_false_granules(rows, eof);
        if (*eof) {
            return Status::OK();
        }
        try {
            _state->orc_lazy_selection_valid = false;
            _state->orc_lazy_selected_rows.clear();
            _state->orc_lazy_input_rows = 0;
            has_next = _state->row_reader->next(*_state->batch);
        } catch (const std::exception& e) {
            if (is_orc_stop(_io_ctx.get(), e)) {
                file_block->clear_column_data(file_block->columns());
                *rows = 0;
                *eof = true;
                _eof = true;
                return Status::OK();
            }
            return Status::InternalError("Orc row reader nextBatch failed. reason = {}", e.what());
        }
        if (has_next) {
            if (_state->orc_lazy_read_enabled && _state->orc_lazy_selection_valid &&
                _state->batch != nullptr && _state->batch->numElements == 0 &&
                _state->orc_lazy_input_rows > 0 && _state->orc_lazy_selected_rows.empty()) {
                // ORC lazy can read rows but return an empty batch when the callback rejects all
                // rows. Keep pulling so callers either see real rows or a clean EOF.
                continue;
            }
            break;
        }
        bool advanced = false;
        RETURN_IF_ERROR(_advance_to_next_stripe_range(&advanced));
        if (!advanced) {
            _eof = true;
            *eof = true;
            return Status::OK();
        }
    }

    const auto batch_rows = static_cast<size_t>(_state->batch->numElements);
    _state->current_batch_first_row = _state->row_reader->getRowNumber();
    _state->condition_cache_next_row = _state->current_batch_first_row + batch_rows;
    auto* struct_batch = dynamic_cast<::orc::StructVectorBatch*>(_state->batch.get());
    if (struct_batch == nullptr) {
        return Status::InternalError("New ORC reader expects struct row batch");
    }

    const bool orc_lazy_read_applied =
            _state->orc_lazy_read_enabled && _state->orc_lazy_selection_valid;
    if (orc_lazy_read_applied && _state->orc_lazy_input_rows != batch_rows) {
        return Status::InternalError("ORC lazy filter selected from {} rows but batch has {} rows",
                                     _state->orc_lazy_input_rows, batch_rows);
    }

    std::set<format::LocalColumnId> decoded_columns;
    RETURN_IF_ERROR(_decode_columns(*struct_batch, _request->predicate_columns, batch_rows,
                                    file_block, &decoded_columns));

    const auto columns_decoded_before_selection = decoded_columns;
    IColumn::Filter keep_filter;
    size_t selected_rows = batch_rows;
    std::vector<size_t> selected_row_indices;
    const std::vector<size_t>* non_predicate_selected_rows = nullptr;
    if (orc_lazy_read_applied) {
        selected_rows = _state->orc_lazy_selected_rows.size();
        if (selected_rows != batch_rows) {
            keep_filter.resize(batch_rows);
            std::fill(keep_filter.begin(), keep_filter.end(), 0);
            for (const auto row : _state->orc_lazy_selected_rows) {
                DORIS_CHECK(row < batch_rows);
                keep_filter[row] = 1;
            }
            selected_row_indices = _state->orc_lazy_selected_rows;
            non_predicate_selected_rows = &selected_row_indices;
        }
    }

    RETURN_IF_ERROR(_decode_columns(*struct_batch, _request->non_predicate_columns, batch_rows,
                                    file_block, &decoded_columns, non_predicate_selected_rows));

    *rows = batch_rows;
    if (orc_lazy_read_applied) {
        if (selected_rows != batch_rows) {
            _filter_decoded_columns(file_block, keep_filter, selected_rows,
                                    columns_decoded_before_selection);
            *rows = selected_rows;
        }
        _state->orc_lazy_selection_valid = false;
    } else {
        RETURN_IF_ERROR(_filter_block(file_block, rows));
    }
    *eof = false;
    return Status::OK();
}

Status OrcReader::get_aggregate_result(const format::FileAggregateRequest& request,
                                       format::FileAggregateResult* result) {
    DORIS_CHECK(result != nullptr);
    if (_state == nullptr || _state->reader == nullptr || _state->root_type == nullptr) {
        return Status::Uninitialized("OrcReader is not open");
    }

    result->count = 0;
    result->columns.clear();
    if (request.agg_type != TPushAggOp::type::COUNT &&
        request.agg_type != TPushAggOp::type::MINMAX) {
        return Status::NotSupported("Unsupported ORC aggregate pushdown type {}", request.agg_type);
    }

    std::vector<uint64_t> selected_stripes;
    if (_state->stripe_pruning_applied) {
        for (const auto& stripe_range : _state->selected_stripe_ranges) {
            if (stripe_range.last_stripe < stripe_range.first_stripe ||
                stripe_range.last_stripe > _state->reader->getNumberOfStripes()) {
                return Status::InternalError("Invalid ORC stripe range {}-{}",
                                             stripe_range.first_stripe, stripe_range.last_stripe);
            }
            for (uint64_t stripe_index = stripe_range.first_stripe;
                 stripe_index < stripe_range.last_stripe; ++stripe_index) {
                selected_stripes.push_back(stripe_index);
            }
        }
    } else {
        const auto stripe_count = _state->reader->getNumberOfStripes();
        selected_stripes.reserve(stripe_count);
        for (uint64_t stripe_index = 0; stripe_index < stripe_count; ++stripe_index) {
            selected_stripes.push_back(stripe_index);
        }
    }

    for (const auto stripe_index : selected_stripes) {
        std::unique_ptr<::orc::StripeInformation> stripe_information;
        try {
            stripe_information = _state->reader->getStripe(stripe_index);
        } catch (const std::exception& e) {
            return Status::InternalError("Failed to read ORC stripe {}: {}", stripe_index,
                                         e.what());
        }
        DORIS_CHECK(stripe_information != nullptr);
        result->count += cast_set<int64_t>(stripe_information->getNumberOfRows());
    }

    if (request.agg_type == TPushAggOp::type::COUNT) {
        if (request.columns.empty()) {
            return Status::OK();
        }
        if (request.columns.size() != 1) {
            return Status::NotSupported("ORC COUNT pushdown only supports one count column");
        }
        const auto& count_projection = request.columns[0].projection;
        if (!count_projection.project_all_children || !count_projection.children.empty()) {
            return Status::NotSupported(
                    "ORC COUNT pushdown only supports top-level column projection");
        }
        if (count_projection.local_id() < 0 ||
            count_projection.local_id() >=
                    static_cast<int32_t>(_state->root_type->getSubtypeCount())) {
            return Status::InvalidArgument("Invalid ORC COUNT aggregate column id {}",
                                           count_projection.local_id());
        }
        const auto* count_type =
                _state->root_type->getSubtype(static_cast<uint64_t>(count_projection.local_id()));
        DORIS_CHECK(count_type != nullptr);

        result->count = 0;
        const auto stripe_statistics_count = _state->reader->getNumberOfStripeStatistics();
        for (const auto stripe_index : selected_stripes) {
            if (stripe_index >= stripe_statistics_count) {
                return Status::NotSupported(
                        "Missing ORC stripe statistics for COUNT column kind {} in stripe {}",
                        static_cast<int>(count_type->getKind()), stripe_index);
            }
            std::unique_ptr<::orc::StripeStatistics> stripe_statistics;
            try {
                stripe_statistics = _state->reader->getStripeStatistics(stripe_index);
            } catch (const std::exception& e) {
                return Status::InternalError("Failed to read ORC stripe statistics {}: {}",
                                             stripe_index, e.what());
            }
            if (stripe_statistics == nullptr) {
                return Status::NotSupported("Missing ORC stripe statistics for stripe {}",
                                            stripe_index);
            }
            const auto* column_statistics = stripe_statistics->getColumnStatistics(
                    cast_set<uint32_t>(count_type->getColumnId()));
            if (column_statistics == nullptr) {
                return Status::NotSupported(
                        "Missing ORC COUNT statistics for column kind {} in stripe {}",
                        static_cast<int>(count_type->getKind()), stripe_index);
            }
            result->count += cast_set<int64_t>(column_statistics->getNumberOfValues());
        }
        return Status::OK();
    }

    result->columns.resize(request.columns.size());
    if (selected_stripes.empty()) {
        return Status::NotSupported("No ORC stripe selected for min/max pushdown");
    }

    const auto stripe_statistics_count = _state->reader->getNumberOfStripeStatistics();
    for (size_t column_idx = 0; column_idx < request.columns.size(); ++column_idx) {
        const auto& request_column = request.columns[column_idx];
        const ::orc::Type* leaf_type = nullptr;
        RETURN_IF_ERROR(find_projected_minmax_leaf(*_state->root_type, request_column.projection,
                                                   &leaf_type));
        DORIS_CHECK(leaf_type != nullptr);

        auto& aggregate_column = result->columns[column_idx];
        aggregate_column.projection = request_column.projection;
        for (const auto stripe_index : selected_stripes) {
            if (stripe_index >= stripe_statistics_count) {
                return Status::NotSupported(
                        "Missing ORC stripe statistics for stripe {} and column kind {}",
                        stripe_index, static_cast<int>(leaf_type->getKind()));
            }
            std::unique_ptr<::orc::StripeStatistics> stripe_statistics;
            try {
                stripe_statistics = _state->reader->getStripeStatistics(stripe_index);
            } catch (const std::exception& e) {
                return Status::InternalError("Failed to read ORC stripe statistics {}: {}",
                                             stripe_index, e.what());
            }
            if (stripe_statistics == nullptr) {
                return Status::NotSupported("Missing ORC stripe statistics for stripe {}",
                                            stripe_index);
            }
            const auto* column_statistics = stripe_statistics->getColumnStatistics(
                    cast_set<uint32_t>(leaf_type->getColumnId()));
            if (column_statistics == nullptr) {
                return Status::NotSupported(
                        "Missing ORC min/max statistics for column kind {} in stripe {}",
                        static_cast<int>(leaf_type->getKind()), stripe_index);
            }

            segment_v2::ZoneMap zone_map;
            if (!build_zone_map_from_orc_statistics(*leaf_type, *column_statistics,
                                                    _state->timezone_obj, &zone_map)) {
                return Status::NotSupported(
                        "Missing ORC min/max statistics for column kind {} in stripe {}",
                        static_cast<int>(leaf_type->getKind()), stripe_index);
            }
            if (!zone_map.has_not_null) {
                continue;
            }
            if (!aggregate_column.has_min || zone_map.min_value < aggregate_column.min_value) {
                aggregate_column.min_value = zone_map.min_value;
                aggregate_column.has_min = true;
            }
            if (!aggregate_column.has_max || aggregate_column.max_value < zone_map.max_value) {
                aggregate_column.max_value = zone_map.max_value;
                aggregate_column.has_max = true;
            }
        }
        if (!aggregate_column.has_min || !aggregate_column.has_max) {
            return Status::NotSupported("No ORC non-null statistics selected for min/max pushdown");
        }
    }
    return Status::OK();
}

Status OrcReader::_decode_column_into_block(const ::orc::StructVectorBatch& struct_batch,
                                            format::LocalColumnId file_column_id, size_t rows,
                                            Block* file_block,
                                            const std::vector<size_t>* selected_rows) const {
    DORIS_CHECK(file_block != nullptr);
    if (is_virtual_column(file_column_id)) {
        return Status::OK();
    }
    const auto position_it = _request->local_positions.find(file_column_id);
    DORIS_CHECK(position_it != _request->local_positions.end());
    const auto block_position = position_it->second;
    DORIS_CHECK(block_position.value() < file_block->columns());
    const auto* type = _state->root_type->getSubtype(static_cast<uint64_t>(file_column_id.value()));
    DORIS_CHECK(type != nullptr);
    const auto batch_index_it = _state->column_to_selected_batch_index.find(file_column_id);
    DORIS_CHECK(batch_index_it != _state->column_to_selected_batch_index.end());
    const size_t selected_batch_idx = batch_index_it->second;
    DORIS_CHECK(selected_batch_idx < struct_batch.fields.size());
    const auto* selected_type = _state->selected_type->getSubtype(selected_batch_idx);
    DORIS_CHECK(selected_type != nullptr);
    auto column = file_block->get_by_position(block_position.value()).column->assert_mutable();
    RETURN_IF_ERROR(_decode_column(*type, *selected_type, *struct_batch.fields[selected_batch_idx],
                                   column, rows, selected_rows));
    file_block->replace_by_position(block_position.value(), std::move(column));
    return Status::OK();
}

Status OrcReader::_decode_columns(const ::orc::StructVectorBatch& struct_batch,
                                  const std::vector<format::LocalColumnIndex>& projections,
                                  size_t rows, Block* file_block,
                                  std::set<format::LocalColumnId>* decoded_columns,
                                  const std::vector<size_t>* selected_rows) const {
    DORIS_CHECK(decoded_columns != nullptr);
    for (const auto& projection : projections) {
        const auto file_column_id = projection.column_id();
        if (!decoded_columns->insert(file_column_id).second) {
            continue;
        }
        if (is_row_position_column(file_column_id)) {
            _fill_row_position_column(file_block, rows, selected_rows);
        } else if (is_global_rowid_column(file_column_id)) {
            RETURN_IF_ERROR(_fill_global_rowid_column(file_block, rows, selected_rows));
        } else {
            RETURN_IF_ERROR(_decode_column_into_block(struct_batch, file_column_id, rows,
                                                      file_block, selected_rows));
        }
    }
    return Status::OK();
}

// The row-position virtual column stores the original physical ORC row number.
void OrcReader::_fill_row_position_column(Block* file_block, size_t rows,
                                          const std::vector<size_t>* selected_rows) const {
    const auto position_it =
            _request->local_positions.find(format::LocalColumnId(format::ROW_POSITION_COLUMN_ID));
    if (position_it == _request->local_positions.end()) {
        return;
    }
    DORIS_CHECK(file_block != nullptr);
    const auto block_position = position_it->second;
    DORIS_CHECK(block_position.value() < file_block->columns());
    auto column = file_block->get_by_position(block_position.value()).column->assert_mutable();
    const auto output_rows = decode_row_count(rows, selected_rows);
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(*column)) {
        auto& data = assert_cast<ColumnInt64&>(nullable_column->get_nested_column()).get_data();
        auto& null_map = nullable_column->get_null_map_data();
        const auto old_size = data.size();
        data.resize(old_size + output_rows);
        null_map.resize_fill(old_size + output_rows, 0);
        for (size_t row = 0; row < output_rows; ++row) {
            data[old_size + row] = cast_set<int64_t>(_state->current_batch_first_row +
                                                     source_row_at(row, selected_rows));
        }
    } else {
        auto& data = assert_cast<ColumnInt64&>(*column).get_data();
        const auto old_size = data.size();
        data.resize(old_size + output_rows);
        for (size_t row = 0; row < output_rows; ++row) {
            data[old_size + row] = cast_set<int64_t>(_state->current_batch_first_row +
                                                     source_row_at(row, selected_rows));
        }
    }
    file_block->replace_by_position(block_position.value(), std::move(column));
}

Status OrcReader::_fill_global_rowid_column(Block* file_block, size_t rows,
                                            const std::vector<size_t>* selected_rows) const {
    const auto position_it =
            _request->local_positions.find(format::LocalColumnId(format::GLOBAL_ROWID_COLUMN_ID));
    if (position_it == _request->local_positions.end()) {
        return Status::OK();
    }
    if (!_global_rowid_context.has_value()) {
        return Status::InvalidArgument("ORC global row id requested without row id context");
    }

    DORIS_CHECK(file_block != nullptr);
    const auto block_position = position_it->second;
    DORIS_CHECK(block_position.value() < file_block->columns());
    auto column = file_block->get_by_position(block_position.value()).column->assert_mutable();
    const auto output_rows = decode_row_count(rows, selected_rows);

    ColumnString* data_column = nullptr;
    ColumnUInt8::Container* null_map = nullptr;
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(*column)) {
        data_column = check_and_get_column<ColumnString>(nullable_column->get_nested_column());
        null_map = &nullable_column->get_null_map_data();
    } else {
        data_column = check_and_get_column<ColumnString>(*column);
    }
    if (data_column == nullptr) {
        return Status::InvalidArgument("ORC global row id column must be STRING");
    }
    if (null_map != nullptr) {
        null_map->resize_fill(null_map->size() + output_rows, 0);
    }

    const auto& context = *_global_rowid_context;
    for (size_t row = 0; row < output_rows; ++row) {
        const auto row_id = cast_set<uint32_t>(_state->current_batch_first_row +
                                               source_row_at(row, selected_rows));
        const GlobalRowLoacationV2 location(context.version, context.backend_id, context.file_id,
                                            row_id);
        data_column->insert_data(reinterpret_cast<const char*>(&location), sizeof(location));
    }
    file_block->replace_by_position(block_position.value(), std::move(column));
    return Status::OK();
}

bool OrcReader::_can_filter_with_decoded_columns(
        const std::set<format::LocalColumnId>& decoded_columns) const {
    auto expr_can_run = [&](const VExprContextSPtr& expr) {
        DORIS_CHECK(expr != nullptr);
        std::set<int> block_positions;
        expr->root()->collect_slot_column_ids(block_positions);
        for (const auto block_position : block_positions) {
            if (block_position < 0) {
                return false;
            }
            const auto local_position = format::LocalIndex(cast_set<size_t>(block_position));
            const auto position_it = std::ranges::find_if(
                    _request->local_positions,
                    [&](const auto& entry) { return entry.second == local_position; });
            if (position_it == _request->local_positions.end() ||
                !decoded_columns.contains(position_it->first)) {
                return false;
            }
        }
        return true;
    };

    for (const auto& conjunct : _request->conjuncts) {
        if (!expr_can_run(conjunct)) {
            return false;
        }
    }
    for (const auto& delete_conjunct : _request->delete_conjuncts) {
        if (!expr_can_run(delete_conjunct)) {
            return false;
        }
    }
    return true;
}

bool OrcReader::_filter_has_row_level_predicates() const {
    return !_request->conjuncts.empty() || !_request->delete_conjuncts.empty();
}

Status OrcReader::_build_keep_filter(Block* file_block, size_t rows,
                                     IColumn::Filter* keep_filter) const {
    DORIS_CHECK(keep_filter != nullptr);
    if (!_filter_has_row_level_predicates()) {
        return Status::OK();
    }
    DORIS_CHECK(file_block != nullptr);
    DORIS_CHECK(keep_filter->size() == rows);
    if (rows == 0) {
        return Status::OK();
    }

    RETURN_IF_ERROR(_execute_conjuncts(file_block, rows, keep_filter));
    RETURN_IF_ERROR(_execute_delete_conjuncts(file_block, rows, keep_filter));
    return Status::OK();
}

Status OrcReader::_filter_block(Block* file_block, size_t* rows) const {
    if (!_filter_has_row_level_predicates()) {
        return Status::OK();
    }
    DORIS_CHECK(file_block != nullptr);
    DORIS_CHECK(rows != nullptr);
    if (*rows == 0) {
        return Status::OK();
    }

    IColumn::Filter keep_filter(*rows, 1);
    RETURN_IF_ERROR(_build_keep_filter(file_block, *rows, &keep_filter));
    _mark_condition_cache_surviving_rows(keep_filter, *rows);
    size_t selected_rows = 0;
    for (const auto keep : keep_filter) {
        selected_rows += keep != 0;
    }
    _filter_block_with_keep_filter(file_block, keep_filter, selected_rows, rows);
    return Status::OK();
}

void OrcReader::_filter_block_with_keep_filter(Block* file_block,
                                               const IColumn::Filter& keep_filter,
                                               size_t selected_rows, size_t* rows) const {
    DORIS_CHECK(file_block != nullptr);
    DORIS_CHECK(rows != nullptr);
    if (selected_rows == *rows) {
        return;
    }
    _filter_requested_columns(file_block, keep_filter, selected_rows);
    *rows = selected_rows;
}

void OrcReader::_filter_decoded_columns(
        Block* file_block, const IColumn::Filter& keep_filter, size_t selected_rows,
        const std::set<format::LocalColumnId>& decoded_columns) const {
    DORIS_CHECK(file_block != nullptr);
    for (const auto file_column_id : decoded_columns) {
        const auto position_it = _request->local_positions.find(file_column_id);
        DORIS_CHECK(position_it != _request->local_positions.end());
        const auto position = position_it->second.value();
        DORIS_CHECK(position < file_block->columns());
        file_block->replace_by_position(
                position,
                file_block->get_by_position(position).column->filter(keep_filter, selected_rows));
    }
}

Status OrcReader::_execute_conjuncts(Block* file_block, size_t rows,
                                     IColumn::Filter* keep_filter) const {
    DORIS_CHECK(file_block != nullptr);
    DORIS_CHECK(keep_filter != nullptr);
    for (const auto& conjunct : _request->conjuncts) {
        IColumn::Filter conjunct_filter(rows, 1);
        bool can_filter_all = false;
        RETURN_IF_ERROR(conjunct->execute_filter(file_block, conjunct_filter.data(), rows, false,
                                                 &can_filter_all));
        if (can_filter_all) {
            std::fill(keep_filter->begin(), keep_filter->end(), 0);
            return Status::OK();
        }
        for (size_t row = 0; row < rows; ++row) {
            (*keep_filter)[row] &= conjunct_filter[row];
        }
    }
    return Status::OK();
}

Status OrcReader::_execute_delete_conjuncts(Block* file_block, size_t rows,
                                            IColumn::Filter* keep_filter) const {
    DORIS_CHECK(file_block != nullptr);
    DORIS_CHECK(keep_filter != nullptr);
    for (const auto& delete_conjunct : _request->delete_conjuncts) {
        DORIS_CHECK(delete_conjunct != nullptr);
        int result_column_id = -1;
        RETURN_IF_ERROR(delete_conjunct->root()->execute(delete_conjunct.get(), file_block,
                                                         &result_column_id));
        DORIS_CHECK(result_column_id >= 0 &&
                    result_column_id < static_cast<int>(file_block->columns()));
        const auto& delete_filter = assert_cast<const ColumnUInt8&>(
                                            *file_block->get_by_position(result_column_id).column)
                                            .get_data();
        DORIS_CHECK(delete_filter.size() == rows);
        for (size_t row = 0; row < rows; ++row) {
            (*keep_filter)[row] &= !delete_filter[row];
        }
        file_block->erase(result_column_id);
    }
    return Status::OK();
}

void OrcReader::_filter_requested_columns(Block* file_block, const IColumn::Filter& keep_filter,
                                          size_t selected_rows) const {
    DORIS_CHECK(file_block != nullptr);
    for (const auto& [_, block_position] : _request->local_positions) {
        const auto position = block_position.value();
        DORIS_CHECK(position < file_block->columns());
        file_block->replace_by_position(
                position,
                file_block->get_by_position(position).column->filter(keep_filter, selected_rows));
    }
}

Status OrcReader::close() {
    _collect_profile();
    if (_state != nullptr) {
        _state = std::make_unique<OrcReaderScanState>();
    }
    return format::FileReader::close();
}

} // namespace doris::format::orc
