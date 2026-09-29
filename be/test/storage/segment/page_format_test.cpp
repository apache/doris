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

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/segment_v2.pb.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "common/config.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column_complex.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/primitive_type.h"
#include "core/extended_types.h"
#include "core/types.h"
#include "core/value/bitmap_value.h"
#include "core/value/decimalv2_value.h"
#include "core/value/hll.h"
#include "core/value/quantile_state.h"
#include "core/value/timestamp_ns_value.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "storage/olap_common.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/column_writer.h"
#include "storage/segment/encoding_info.h"
#include "storage/storage_layout.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/types.h"
#include "util/coding.h"
#include "util/slice.h"

namespace doris::segment_v2 {
namespace {

constexpr std::string_view kTestDir = "./ut_dir/page_format_test";
constexpr std::string_view kGoldenDir = "./be/test/storage/test_data/page_format";
constexpr std::string_view kGoldenOutputDirEnv = "DORIS_PAGE_FORMAT_GOLDEN_OUTPUT_DIR";
// Small budgets make every case span several pages, so the goldens pin where the column writer
// cuts its pages as well as how it encodes them.
constexpr size_t kDataPageSize = 512;
constexpr size_t kDictPageSize = 512;
// RLE packs a BOOL into about one bit, so it needs a smaller budget to cut these rows into
// several pages.
constexpr size_t kRlePageSize = 32;
// A Segment writer appends one block at a time, and RLE pages are cut only between appends.
constexpr std::array<size_t, 6> kBlockRows {1, 7, 64, 173, 311, 1000};
// Aggregate states are serialized by the functions of this BE exec version.
constexpr int32_t kGoldenBeExecVersion = 10;
constexpr size_t kVocabularyRows = 200;
constexpr size_t kStringRows = 260;

struct TypeSpec {
    std::string_view name;
    std::string_view storage_type;
    int32_t length;
    int32_t precision;
    int32_t scale;
    bool nullable;
    std::string_view aggregate_function;
    std::string_view aggregate_argument;
    int32_t aggregate_argument_length;
};

// One spec per storage type a ScalarColumnWriter writes, and one per serialized shape of
// AGG_STATE. Two types with registered encodings have none: no writer is given UNSIGNED_INT, as
// array and map offsets are UNSIGNED_BIGINT, and the root of a VARIANT is JSONB, which the JSONB
// spec covers. A column holds NULLs unless it is never nullable: array and map offsets, and the
// AGG_STATE, HLL, BITMAP and QUANTILE_STATE columns FE only creates NOT NULL.
constexpr std::array kTypeSpecs {
        TypeSpec {"bool", "BOOLEAN", 1, 0, 0, true, {}, {}, 0},
        TypeSpec {"tinyint", "TINYINT", 1, 0, 0, true, {}, {}, 0},
        TypeSpec {"smallint", "SMALLINT", 2, 0, 0, true, {}, {}, 0},
        TypeSpec {"int", "INT", 4, 0, 0, true, {}, {}, 0},
        TypeSpec {"bigint", "BIGINT", 8, 0, 0, true, {}, {}, 0},
        TypeSpec {"largeint", "LARGEINT", 16, 0, 0, true, {}, {}, 0},
        TypeSpec {"unsigned_bigint", "UNSIGNED_BIGINT", 8, 0, 0, false, {}, {}, 0},
        TypeSpec {"float", "FLOAT", 4, 0, 0, true, {}, {}, 0},
        TypeSpec {"double", "DOUBLE", 8, 0, 0, true, {}, {}, 0},
        TypeSpec {"date", "DATE", 3, 0, 0, true, {}, {}, 0},
        TypeSpec {"datetime", "DATETIME", 8, 0, 0, true, {}, {}, 0},
        TypeSpec {"datev2", "DATEV2", 4, 0, 0, true, {}, {}, 0},
        TypeSpec {"datetimev2", "DATETIMEV2", 8, 0, 6, true, {}, {}, 0},
        TypeSpec {"timestamptz", "TIMESTAMPTZ", 8, 0, 6, true, {}, {}, 0},
        TypeSpec {"timestamp_ns", "TIMESTAMP_NS", 8, 0, 9, true, {}, {}, 0},
        TypeSpec {"decimalv2", "DECIMAL", 12, 27, 9, true, {}, {}, 0},
        TypeSpec {"decimal32", "DECIMAL32", 4, 9, 2, true, {}, {}, 0},
        TypeSpec {"decimal64", "DECIMAL64", 8, 18, 4, true, {}, {}, 0},
        TypeSpec {"decimal128", "DECIMAL128I", 16, 38, 9, true, {}, {}, 0},
        TypeSpec {"decimal256", "DECIMAL256", 32, 76, 18, true, {}, {}, 0},
        TypeSpec {"ipv4", "IPV4", 4, 0, 0, true, {}, {}, 0},
        TypeSpec {"ipv6", "IPV6", 16, 0, 0, true, {}, {}, 0},
        TypeSpec {"char", "CHAR", 16, 0, 0, true, {}, {}, 0},
        TypeSpec {"varchar", "VARCHAR", 65533, 0, 0, true, {}, {}, 0},
        TypeSpec {"string", "STRING", 2147483643, 0, 0, true, {}, {}, 0},
        TypeSpec {"jsonb", "JSONB", 2147483643, 0, 0, true, {}, {}, 0},
        TypeSpec {"hll", "HLL", 16387, 0, 0, false, {}, {}, 0},
        TypeSpec {"bitmap", "BITMAP", 16, 0, 0, false, {}, {}, 0},
        TypeSpec {"quantile_state", "QUANTILE_STATE", 16, 0, 0, false, {}, {}, 0},
        TypeSpec {"agg_state_count", "AGG_STATE", 1, 0, 0, false, "count", "INT", 4},
        TypeSpec {"agg_state_hll_union", "AGG_STATE", 1, 0, 0, false, "hll_union", "HLL", 16387},
        TypeSpec {"agg_state_bitmap_union", "AGG_STATE", 1, 0, 0, false, "bitmap_union", "BITMAP",
                  16},
};

struct CaseInput {
    TabletColumn column;
    ColumnWithTypeAndName data;
};

struct PageFormatCase {
    std::string name;
    EncodingTypePB encoding;
    // Decides the dictionary page encoding: PLAIN for V1 and V2 Segments, PLAIN_V3 for V3.
    TabletStorageFormatPB storage_format;
};

// The pages ScalarColumnWriter::write_data() writes, each as it is on disk.
struct WrittenColumn {
    ColumnMetaPB meta;
    std::vector<std::string> data_pages;
    std::string dict_page;
};

using GoldenFiles = std::map<std::string, std::string>;

struct GoldenPage {
    std::string file;
    std::string label;
    size_t bytes;
};

// One golden file per page encoding, holding the pages of its columns one after another.
struct Goldens {
    GoldenFiles files;
    std::vector<GoldenPage> pages;

    // The golden files, and layout.txt listing their pages file by file.
    GoldenFiles with_layout() const {
        std::string layout;
        for (const auto& [file, bytes] : files) {
            for (const auto& page : pages) {
                if (page.file == file) {
                    layout += fmt::format("{} {} bytes={}\n", file, page.label, page.bytes);
                }
            }
        }
        GoldenFiles all = files;
        all.emplace("layout.txt", std::move(layout));
        return all;
    }
};

// PageIO::write_page() appends a page, footer and checksum included, in one call.
class PageRecordingFileWriter final : public io::FileWriter {
public:
    explicit PageRecordingFileWriter(io::FileWriterPtr file) : _file(std::move(file)) {}

    Status close(bool non_block = false) override { return _file->close(non_block); }

    Status appendv(const Slice* data, size_t data_cnt) override {
        auto& page = pages.emplace_back();
        for (size_t i = 0; i < data_cnt; ++i) {
            page.append(data[i].data, data[i].size);
        }
        return _file->appendv(data, data_cnt);
    }

    const io::Path& path() const override { return _file->path(); }
    size_t bytes_appended() const override { return _file->bytes_appended(); }
    State state() const override { return _file->state(); }

    std::vector<std::string> pages;

private:
    io::FileWriterPtr _file;
};

uint64_t mix(uint64_t value) {
    value += 0x9e3779b97f4a7c15ULL;
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
}

// A narrow increasing run, full-width noise, then short runs of the extremes.
template <typename T>
T integer_at(size_t row, size_t rows) {
    if (row < rows / 3) {
        return static_cast<T>(static_cast<int64_t>(row) * 3 - 50);
    }
    if (row < rows / 3 * 2) {
        if constexpr (sizeof(T) > sizeof(uint64_t)) {
            return static_cast<T>((static_cast<unsigned __int128>(mix(row)) << 64) |
                                  mix(rows + row));
        } else {
            return static_cast<T>(mix(row));
        }
    }
    const std::array<T, 5> extremes {std::numeric_limits<T>::min(), std::numeric_limits<T>::max(),
                                     static_cast<T>(0), static_cast<T>(-1), static_cast<T>(1)};
    return extremes[row / 3 % extremes.size()];
}

template <typename Native>
Native decimal_at(size_t row, size_t rows, Native bound) {
    if (row < rows / 3) {
        return static_cast<Native>(static_cast<int64_t>(row) * 7 - 100);
    }
    if (row < rows / 3 * 2) {
        if constexpr (std::is_same_v<Native, wide::Int256>) {
            return wide::Int256(integer_at<__int128>(row, rows)) *
                   wide::Int256(integer_at<int64_t>(row, rows)) % (bound + 1);
        } else {
            return integer_at<Native>(row, rows) % (bound + 1);
        }
    }
    const std::array<Native, 5> extremes {-bound, bound, Native(0), Native(1), Native(-1)};
    return extremes[row / 3 % extremes.size()];
}

template <typename T>
T floating_at(size_t row) {
    using Bits = std::conditional_t<sizeof(T) == sizeof(uint32_t), uint32_t, uint64_t>;
    const Bits sign = Bits(1) << (sizeof(Bits) * 8 - 1);
    // Storage keeps one canonical NaN, so a negative NaN and a signaling NaN with a payload are
    // written as the quiet one.
    const std::array<T, 10> specials {
            T(0),
            -T(0),
            std::numeric_limits<T>::infinity(),
            -std::numeric_limits<T>::infinity(),
            std::numeric_limits<T>::quiet_NaN(),
            std::bit_cast<T>(std::bit_cast<Bits>(std::numeric_limits<T>::quiet_NaN()) | sign),
            std::bit_cast<T>(std::bit_cast<Bits>(std::numeric_limits<T>::infinity()) | Bits(1)),
            std::numeric_limits<T>::denorm_min(),
            std::numeric_limits<T>::max(),
            std::numeric_limits<T>::lowest()};
    if (row % 5 == 0) {
        return specials[row / 5 % specials.size()];
    }
    return static_cast<T>(static_cast<int64_t>(mix(row) % 2000001) - 1000000) / T(64);
}

struct CivilTime {
    int year;
    int month;
    int day;
    int hour;
    int minute;
    int second;
    int microsecond;
};

// Days of one year in order, instants spread over the whole range, then the edges of the range.
CivilTime civil_time_at(size_t row, size_t rows) {
    if (row < rows / 3) {
        const int r = static_cast<int>(row);
        return {2024,       1 + r / 28 % 12, 1 + r % 28,        r % 24,
                r * 7 % 60, r * 13 % 60,     r * 1001 % 1000000};
    }
    if (row < rows / 3 * 2) {
        const uint64_t bits = mix(row);
        return {1 + static_cast<int>(bits % 9999),       1 + static_cast<int>((bits >> 16) % 12),
                1 + static_cast<int>((bits >> 24) % 28), static_cast<int>((bits >> 32) % 24),
                static_cast<int>((bits >> 40) % 60),     static_cast<int>((bits >> 48) % 60),
                static_cast<int>((bits >> 8) % 1000000)};
    }
    constexpr std::array<CivilTime, 4> edges {{{1, 1, 1, 0, 0, 0, 0},
                                               {9999, 12, 31, 23, 59, 59, 999999},
                                               {1970, 1, 1, 0, 0, 0, 0},
                                               {2000, 2, 29, 12, 30, 45, 500000}}};
    return edges[row / 3 % edges.size()];
}

std::string date_text_at(size_t row, size_t rows) {
    const auto time = civil_time_at(row, rows);
    return fmt::format("{:04}-{:02}-{:02}", time.year, time.month, time.day);
}

std::string datetime_text_at(size_t row, size_t rows) {
    const auto time = civil_time_at(row, rows);
    return fmt::format("{} {:02}:{:02}:{:02}", date_text_at(row, rows), time.hour, time.minute,
                       time.second);
}

std::string datetimev2_text_at(size_t row, size_t rows) {
    return fmt::format("{}.{:06}", datetime_text_at(row, rows),
                       civil_time_at(row, rows).microsecond);
}

// A small vocabulary fills dictionary pages with codes. The distinct tail overflows the
// dictionary, so the last pages fall back to plain encoding; two of its values exceed a page.
std::string text_at(size_t row, size_t max_length) {
    constexpr std::array<std::string_view, 12> vocabulary {"",
                                                           "a",
                                                           "pad ",
                                                           "中文",
                                                           "éàü",
                                                           "middle",
                                                           "zzzz",
                                                           "0123456789abcdef",
                                                           "quick brown fox",
                                                           "x\ty",
                                                           "NULL",
                                                           "-0"};
    std::string text;
    if (row < kVocabularyRows) {
        text = vocabulary[mix(row) % vocabulary.size()];
    } else if (row == kVocabularyRows + 20) {
        text.assign(700, 'L');
    } else if (row == kVocabularyRows + 50) {
        text.assign(1100, 'O');
    } else {
        text = fmt::format("row-{:03}-{}", row, std::string(row % 37, 'z'));
    }
    text.resize(std::min(text.size(), max_length));
    return text;
}

std::string json_at(size_t row) {
    constexpr std::array<std::string_view, 12> vocabulary {
            "null",       "true",
            "false",      "0",
            "-1.5",       R"("")",
            R"("text")",  "[]",
            "{}",         R"([1,"x",null])",
            R"({"a":1})", R"({"k":"中文","n":{"x":[1,2,3]}})"};
    if (row < kVocabularyRows) {
        return std::string(vocabulary[mix(row) % vocabulary.size()]);
    }
    if (row == kVocabularyRows + 20) {
        return fmt::format(R"({{"blob":"{}"}})", std::string(700, 'L'));
    }
    if (row == kVocabularyRows + 50) {
        return fmt::format(R"({{"blob":"{}"}})", std::string(1100, 'O'));
    }
    return fmt::format(R"({{"id":{},"pad":"{}"}})", row, std::string(row % 37, 'z'));
}

// Empty, one explicit value, and enough values to switch to sparse registers. Several explicit
// values are left out: they are written in hash set order.
HyperLogLog hll_at(size_t row) {
    HyperLogLog hll;
    if (row % 4 == 0) {
        return hll;
    }
    const size_t values = row % 8 == 2 ? HLL_EXPLICIT_INT64_NUM + 10 : 1;
    for (size_t value = 0; value < values; ++value) {
        hll.update(mix(row * 1000 + value));
    }
    return hll;
}

// Empty, 32 and 64 bit singles, and 32 and 64 bit Roaring bitmaps. Sets of 2 to 32 values are
// left out: they are written in hash set order.
BitmapValue bitmap_at(size_t row) {
    switch (row % 4) {
    case 0:
        return BitmapValue();
    case 1:
        return BitmapValue(static_cast<uint64_t>(row * 1000));
    case 2:
        return BitmapValue((uint64_t(1) << 33) + row);
    default: {
        const uint64_t base = row % 8 == 7 ? uint64_t(1) << 40 : 0;
        std::vector<uint64_t> values;
        for (size_t value = 0; value < 40 + row % 7; ++value) {
            values.push_back(base + row * 100 + value * 3);
        }
        return BitmapValue(values);
    }
    }
}

QuantileState quantile_state_at(size_t row) {
    QuantileState state;
    const size_t values = row % 3 == 0 ? 0 : row % 3 == 1 ? 1 : 2 + row % 5;
    for (size_t value = 0; value < values; ++value) {
        state.add_value(static_cast<double>(row) * 0.5 + static_cast<double>(value) * 0.25);
    }
    return state;
}

std::string serialized_hll_at(size_t row) {
    const auto hll = hll_at(row);
    std::string serialized(hll.max_serialized_size(), '\0');
    serialized.resize(hll.serialize(reinterpret_cast<uint8_t*>(serialized.data())));
    return serialized;
}

// Scattered NULLs in the middle third and a run of them after it, so that pages with and without
// a null map are written.
bool is_null_at(size_t row, size_t rows) {
    const size_t third = rows / 3;
    return (row >= third && row < third * 2 && row % 7 == 3) ||
           (row >= third * 2 && row < third * 2 + rows / 10);
}

template <PrimitiveType PT, typename ValueAt>
void append_values(IColumn& column, size_t rows, ValueAt value_at) {
    auto& typed = assert_cast<typename PrimitiveTypeTraits<PT>::ColumnType&>(column);
    for (size_t row = 0; row < rows; ++row) {
        typed.insert_value(value_at(row));
    }
}

template <typename TextAt>
Status append_texts(const IDataType& data_type, IColumn& column, size_t rows, TextAt text_at) {
    for (size_t row = 0; row < rows; ++row) {
        const std::string text = text_at(row);
        StringRef ref(text.data(), text.size());
        RETURN_IF_ERROR(data_type.from_string(ref, &column));
    }
    return Status::OK();
}

ColumnPB column_pb(const TypeSpec& spec) {
    ColumnPB column;
    column.set_unique_id(1);
    column.set_name("v");
    column.set_type(std::string(spec.storage_type));
    column.set_is_key(false);
    column.set_is_nullable(spec.nullable);
    column.set_aggregation(spec.aggregate_function.empty() ? "NONE"
                                                           : std::string(spec.aggregate_function));
    column.set_length(spec.length);
    column.set_index_length(spec.length);
    column.set_precision(spec.precision);
    column.set_frac(spec.scale);
    if (!spec.aggregate_function.empty()) {
        column.set_result_is_nullable(false);
        column.set_be_exec_version(kGoldenBeExecVersion);
        auto* argument = column.add_children_columns();
        argument->set_unique_id(2);
        argument->set_name("arg");
        argument->set_type(std::string(spec.aggregate_argument));
        argument->set_is_key(false);
        argument->set_is_nullable(false);
        argument->set_aggregation("NONE");
        argument->set_length(spec.aggregate_argument_length);
        argument->set_index_length(spec.aggregate_argument_length);
    }
    return column;
}

Result<CaseInput> create_input(const TypeSpec& spec) {
    CaseInput input;
    input.column.init_from_pb(column_pb(spec));
    const FieldType type = input.column.type();
    DataTypePtr data_type;
    MutableColumnPtr column;
    if (type == FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT) {
        // Array and map offsets, which have no DataType.
        column = ColumnOffset64::create();
    } else {
        data_type = remove_nullable(DataTypeFactory::instance().create_data_type(input.column));
        column = data_type->create_column();
    }
    // Two and a half data pages, and at least 300 rows.
    const size_t fixed_rows =
            std::max(kDataPageSize * 5 / 2 / field_type_size(type), size_t {300}) + 3;
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_BOOL:
        append_values<TYPE_BOOLEAN>(*column, fixed_rows, [&](size_t row) {
            return static_cast<UInt8>(row < fixed_rows / 2 ? row / 11 % 2 : mix(row) & 1);
        });
        break;
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        append_values<TYPE_TINYINT>(*column, fixed_rows,
                                    [&](size_t row) { return integer_at<Int8>(row, fixed_rows); });
        break;
    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        append_values<TYPE_SMALLINT>(*column, fixed_rows, [&](size_t row) {
            return integer_at<Int16>(row, fixed_rows);
        });
        break;
    case FieldType::OLAP_FIELD_TYPE_INT:
        append_values<TYPE_INT>(*column, fixed_rows,
                                [&](size_t row) { return integer_at<Int32>(row, fixed_rows); });
        break;
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        append_values<TYPE_BIGINT>(*column, fixed_rows,
                                   [&](size_t row) { return integer_at<Int64>(row, fixed_rows); });
        break;
    case FieldType::OLAP_FIELD_TYPE_LARGEINT:
        append_values<TYPE_LARGEINT>(*column, fixed_rows, [&](size_t row) {
            return integer_at<Int128>(row, fixed_rows);
        });
        break;
    case FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
        append_values<TYPE_UINT64>(*column, fixed_rows,
                                   [&](size_t row) { return integer_at<UInt64>(row, fixed_rows); });
        break;
    case FieldType::OLAP_FIELD_TYPE_FLOAT:
        append_values<TYPE_FLOAT>(*column, fixed_rows, floating_at<Float32>);
        break;
    case FieldType::OLAP_FIELD_TYPE_DOUBLE:
        append_values<TYPE_DOUBLE>(*column, fixed_rows, floating_at<Float64>);
        break;
    case FieldType::OLAP_FIELD_TYPE_DATE:
    case FieldType::OLAP_FIELD_TYPE_DATEV2:
        RETURN_IF_ERROR_RESULT(append_texts(*data_type, *column, fixed_rows, [&](size_t row) {
            return date_text_at(row, fixed_rows);
        }));
        break;
    case FieldType::OLAP_FIELD_TYPE_DATETIME:
        RETURN_IF_ERROR_RESULT(append_texts(*data_type, *column, fixed_rows, [&](size_t row) {
            return datetime_text_at(row, fixed_rows);
        }));
        break;
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2:
        RETURN_IF_ERROR_RESULT(append_texts(*data_type, *column, fixed_rows, [&](size_t row) {
            return datetimev2_text_at(row, fixed_rows);
        }));
        break;
    case FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ:
        RETURN_IF_ERROR_RESULT(append_texts(*data_type, *column, fixed_rows, [&](size_t row) {
            return datetimev2_text_at(row, fixed_rows) + " +00:00";
        }));
        break;
    case FieldType::OLAP_FIELD_TYPE_TIMESTAMP_NS:
        append_values<TYPE_TIMESTAMP_NS>(*column, fixed_rows, [&](size_t row) {
            return TimeStampNsValue(integer_at<Int64>(row, fixed_rows));
        });
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL:
        append_values<TYPE_DECIMALV2>(*column, fixed_rows, [&](size_t row) {
            return DecimalV2Value(decimal_at<Int128>(
                    row, fixed_rows, Int128(999999999999999999LL) * 1000000000 + 999999999));
        });
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32:
        append_values<TYPE_DECIMAL32>(*column, fixed_rows, [&](size_t row) {
            return Decimal32(decimal_at<Int32>(row, fixed_rows, 999999999));
        });
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64:
        append_values<TYPE_DECIMAL64>(*column, fixed_rows, [&](size_t row) {
            return Decimal64(decimal_at<Int64>(row, fixed_rows, 999999999999999999LL));
        });
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I:
        append_values<TYPE_DECIMAL128I>(*column, fixed_rows, [&](size_t row) {
            return Decimal128V3(decimal_at<Int128>(
                    row, fixed_rows,
                    Int128(9999999999999999999ULL) * Int128(10000000000000000000ULL) +
                            Int128(9999999999999999999ULL)));
        });
        break;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL256: {
        const auto bound = wide::Int256::_impl::from_str(std::string(76, '9').c_str());
        append_values<TYPE_DECIMAL256>(*column, fixed_rows, [&](size_t row) {
            return Decimal256(decimal_at<wide::Int256>(row, fixed_rows, bound));
        });
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_IPV4:
        append_values<TYPE_IPV4>(*column, fixed_rows,
                                 [&](size_t row) { return integer_at<IPv4>(row, fixed_rows); });
        break;
    case FieldType::OLAP_FIELD_TYPE_IPV6:
        append_values<TYPE_IPV6>(*column, fixed_rows,
                                 [&](size_t row) { return integer_at<IPv6>(row, fixed_rows); });
        break;
    case FieldType::OLAP_FIELD_TYPE_CHAR:
    case FieldType::OLAP_FIELD_TYPE_VARCHAR:
    case FieldType::OLAP_FIELD_TYPE_STRING: {
        const size_t max_length =
                type == FieldType::OLAP_FIELD_TYPE_CHAR ? spec.length : std::string::npos;
        for (size_t row = 0; row < kStringRows; ++row) {
            const auto text = text_at(row, max_length);
            column->insert_data(text.data(), text.size());
        }
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_JSONB:
        RETURN_IF_ERROR_RESULT(append_texts(*data_type, *column, kStringRows, json_at));
        break;
    case FieldType::OLAP_FIELD_TYPE_HLL:
        append_values<TYPE_HLL>(*column, 40, hll_at);
        break;
    case FieldType::OLAP_FIELD_TYPE_BITMAP:
        append_values<TYPE_BITMAP>(*column, 40, bitmap_at);
        break;
    case FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE:
        append_values<TYPE_QUANTILE_STATE>(*column, 120, quantile_state_at);
        break;
    case FieldType::OLAP_FIELD_TYPE_AGG_STATE:
        if (spec.aggregate_function == "count") {
            for (uint64_t row = 0; row < 160; ++row) {
                const uint64_t count = integer_at<uint64_t>(row, 160);
                column->insert_data(reinterpret_cast<const char*>(&count), sizeof(count));
            }
        } else if (spec.aggregate_function == "hll_union") {
            for (size_t row = 0; row < 40; ++row) {
                const auto serialized = serialized_hll_at(row);
                column->insert_data(serialized.data(), serialized.size());
            }
        } else {
            DORIS_CHECK(spec.aggregate_function == "bitmap_union");
            append_values<TYPE_BITMAP>(*column, 40, bitmap_at);
        }
        break;
    default:
        return ResultError(Status::InternalError("no rows for storage type {}", spec.name));
    }
    if (input.column.is_nullable()) {
        const size_t rows = column->size();
        auto null_map = ColumnUInt8::create();
        for (size_t row = 0; row < rows; ++row) {
            null_map->insert_value(is_null_at(row, rows));
        }
        column = ColumnNullable::create(std::move(column), std::move(null_map));
        data_type = make_nullable(data_type);
    }
    input.data = {std::move(column), data_type, "v"};
    return input;
}

std::vector<EncodingTypePB> registered_encodings(FieldType type) {
    std::vector<EncodingTypePB> encodings;
    for (int value = EncodingTypePB_MIN; value <= EncodingTypePB_MAX; ++value) {
        const EncodingInfo* encoding_info = nullptr;
        if (EncodingTypePB_IsValid(value) &&
            EncodingInfo::get(type, static_cast<EncodingTypePB>(value), &encoding_info).ok()) {
            encodings.push_back(static_cast<EncodingTypePB>(value));
        }
    }
    return encodings;
}

std::string encoding_name(EncodingTypePB encoding) {
    std::string name = EncodingTypePB_Name(encoding);
    constexpr std::string_view suffix = "_ENCODING";
    if (const auto position = name.find(suffix); position != std::string::npos) {
        name.erase(position, suffix.size());
    }
    std::transform(name.begin(), name.end(), name.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return name;
}

std::vector<PageFormatCase> page_format_cases(const TypeSpec& spec, FieldType type) {
    std::vector<PageFormatCase> cases;
    for (const auto encoding : registered_encodings(type)) {
        // Nothing has written FOR pages since bitmap indexes were removed, and
        // FrameOfReferencePageDecoder cannot read their values back, so no golden pins them.
        if (encoding == FOR_ENCODING) {
            continue;
        }
        if (encoding != DICT_ENCODING) {
            cases.push_back({fmt::format("{}_{}", spec.name, encoding_name(encoding)), encoding,
                             TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2});
            continue;
        }
        cases.push_back({fmt::format("{}_dict_plain_words", spec.name), encoding,
                         TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2});
        cases.push_back({fmt::format("{}_dict_plain_v3_words", spec.name), encoding,
                         TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3});
    }
    return cases;
}

// As VerticalSegmentWriter::_init_column_meta() fills it, except for the encoding. Pages are left
// uncompressed, so a page file holds what the page builder encoded.
void init_column_meta(ColumnMetaPB* meta, const TabletColumn& column) {
    meta->set_column_id(0);
    meta->set_type(int(column.type()));
    meta->set_length(static_cast<int32_t>(column.length()));
    meta->set_compression(NO_COMPRESSION);
    meta->set_is_nullable(column.is_nullable());
    meta->set_precision(column.precision());
    meta->set_frac(column.frac());
    meta->set_unique_id(column.unique_id());
    for (uint32_t i = 0; i < column.get_subtype_count(); ++i) {
        init_column_meta(meta->add_children_columns(), column.get_sub_column(i));
    }
    meta->set_result_is_nullable(column.get_result_is_nullable());
    meta->set_function_name(column.get_aggregation_name());
    meta->set_be_exec_version(column.get_be_exec_version());
}

// Appends the rows block by block to a ScalarColumnWriter, the way a Segment writer does, and
// keeps the pages it writes.
Result<WrittenColumn> write_column(const PageFormatCase& page_case, const CaseInput& input,
                                   const std::string& path) {
    io::FileWriterPtr file;
    RETURN_IF_ERROR_RESULT(io::global_local_filesystem()->create_file(path, &file));
    PageRecordingFileWriter file_writer(std::move(file));
    WrittenColumn written;
    ColumnWriterOptions options;
    options.meta = &written.meta;
    init_column_meta(options.meta, input.column);
    options.meta->set_encoding(page_case.encoding);
    options.storage_format = page_case.storage_format;
    options.data_page_size = page_case.encoding == RLE ? kRlePageSize : kDataPageSize;
    options.dict_page_size = kDictPageSize;
    ScalarColumnWriter writer(options, std::make_shared<TabletColumn>(input.column), &file_writer);
    RETURN_IF_ERROR_RESULT(writer.init());

    const size_t rows = input.data.column->size();
    for (size_t row_pos = 0, block = 0; row_pos < rows; ++block) {
        const size_t block_rows = std::min(kBlockRows[block % kBlockRows.size()], rows - row_pos);
        RETURN_IF_ERROR_RESULT(writer.append(*input.data.column, row_pos, block_rows));
        row_pos += block_rows;
    }
    RETURN_IF_ERROR_RESULT(writer.finish());
    RETURN_IF_ERROR_RESULT(writer.write_data());
    written.data_pages = std::move(file_writer.pages);
    if (page_case.encoding == DICT_ENCODING) {
        // write_data() writes the dictionary page after the data pages.
        written.dict_page = std::move(written.data_pages.back());
        written.data_pages.pop_back();
    }
    // Not pinned: the ordinal index only lets the column be read back.
    RETURN_IF_ERROR_RESULT(writer.write_ordinal_index());
    RETURN_IF_ERROR_RESULT(file_writer.close());
    return written;
}

// A row's StorageValue: what the column writer stores for it.
std::string storage_value(FieldType type, const IColumn& column, size_t row,
                          PaddedPODArray<char>& tmp_buffer) {
    switch (type) {
#define M(FT)                                                                     \
    case FieldType::FT: {                                                         \
        const auto value = StorageLayout<FieldType::FT>::storage_at(column, row); \
        return {reinterpret_cast<const char*>(&value), sizeof(value)};            \
    }
        DORIS_APPLY_FOR_FIXED_WIDTH_STORAGE_LAYOUT_TYPES(M)
#undef M
#define M(FT)                                                                                      \
    case FieldType::FT: {                                                                          \
        const StringRef value = StorageLayout<FieldType::FT>::storage_at(column, row, tmp_buffer); \
        return {value.data, value.size};                                                           \
    }
        M(OLAP_FIELD_TYPE_CHAR)
        M(OLAP_FIELD_TYPE_VARCHAR)
        M(OLAP_FIELD_TYPE_STRING)
        M(OLAP_FIELD_TYPE_JSONB)
        M(OLAP_FIELD_TYPE_HLL)
        M(OLAP_FIELD_TYPE_BITMAP)
        M(OLAP_FIELD_TYPE_QUANTILE_STATE)
        M(OLAP_FIELD_TYPE_AGG_STATE)
#undef M
    default:
        throw Exception(ErrorCode::INTERNAL_ERROR, "no storage layout for type {}", int(type));
    }
}

// The StorageValues of these rows; NULL rows are left empty.
std::vector<std::string> storage_cells(FieldType type, const IColumn& data) {
    const auto* nullable = check_and_get_column<ColumnNullable>(data);
    const IColumn& values = nullable != nullptr ? nullable->get_nested_column() : data;
    PaddedPODArray<char> tmp_buffer;
    std::vector<std::string> cells(data.size());
    for (size_t row = 0; row < data.size(); ++row) {
        if (nullable == nullptr || !nullable->is_null_at(row)) {
            cells[row] = storage_value(type, values, row, tmp_buffer);
        }
    }
    return cells;
}

// Reads the column back the way a Segment reads it and checks that it holds the rows written.
Status verify_read_back(const PageFormatCase& page_case, const CaseInput& input,
                        const WrittenColumn& written, const std::string& path) {
    io::FileReaderSPtr file_reader;
    RETURN_IF_ERROR(io::global_local_filesystem()->open_file(path, &file_reader));
    ColumnReaderOptions reader_options;
    reader_options.be_exec_version = BeExecVersionManager::get_newest_version();
    std::shared_ptr<ColumnReader> reader;
    RETURN_IF_ERROR(ColumnReader::create(reader_options, written.meta, written.meta.num_rows(),
                                         file_reader, &reader));
    ColumnIteratorUPtr iterator;
    RETURN_IF_ERROR(reader->new_iterator(&iterator, &input.column));
    OlapReaderStatistics stats;
    ColumnIteratorOptions iterator_options;
    iterator_options.stats = &stats;
    iterator_options.file_reader = file_reader.get();
    RETURN_IF_ERROR(iterator->init(iterator_options));
    RETURN_IF_ERROR(iterator->seek_to_ordinal(0));
    MutableColumnPtr read = input.data.column->clone_empty();
    size_t rows = written.meta.num_rows();
    bool has_null = false;
    RETURN_IF_ERROR(iterator->next_batch(&rows, read, &has_null));
    if (read->size() != input.data.column->size()) {
        return Status::InternalError("read {} rows back, wrote {}", read->size(),
                                     input.data.column->size());
    }

    if (input.column.is_nullable()) {
        const auto& read_null_map = assert_cast<const ColumnNullable&>(*read).get_null_map_data();
        const auto& null_map =
                assert_cast<const ColumnNullable&>(*input.data.column).get_null_map_data();
        if (!std::equal(read_null_map.begin(), read_null_map.end(), null_map.begin())) {
            return Status::InternalError("the NULL rows read back differ from the ones written");
        }
    }
    auto expected = storage_cells(input.column.type(), *input.data.column);
    auto actual = storage_cells(input.column.type(), *read);
    // Reading turns a single-value bitmap into a one-value set, which is written differently, so
    // bitmaps are compared by value.
    if (check_and_get_column<ColumnBitmap>(*remove_nullable(input.data.column)) != nullptr) {
        for (auto* cells : {&expected, &actual}) {
            for (auto& cell : *cells) {
                if (!cell.empty()) {
                    cell = BitmapValue(cell.data()).to_string();
                }
            }
        }
    }
    const auto [actual_cell, expected_cell] =
            std::mismatch(actual.begin(), actual.end(), expected.begin());
    if (actual_cell != actual.end()) {
        auto hex = [](const std::string& cell) {
            std::string text;
            for (const char byte : cell.substr(0, 64)) {
                text += fmt::format("{:02x}", static_cast<uint8_t>(byte));
            }
            return text;
        };
        return Status::InternalError("row {} reads back as {} instead of {}",
                                     std::distance(actual.begin(), actual_cell), hex(*actual_cell),
                                     hex(*expected_cell));
    }
    return Status::OK();
}

// A page ends with its PageFooterPB, the footer length and the checksum.
uint32_t page_rows(const std::string& page) {
    const size_t footer_size =
            decode_fixed32_le(reinterpret_cast<const uint8_t*>(page.data() + page.size() - 8));
    PageFooterPB footer;
    DORIS_CHECK(footer.ParseFromArray(page.data() + page.size() - 8 - footer_size,
                                      static_cast<int>(footer_size)));
    return footer.data_page_footer().num_values();
}

void add_goldens(Goldens* goldens, const PageFormatCase& page_case, const WrittenColumn& written) {
    const auto file = encoding_name(page_case.encoding) + ".dat";
    auto add_page = [&](const std::string& page, std::string label) {
        goldens->files[file] += page;
        goldens->pages.push_back({file, std::move(label), page.size()});
    };
    for (size_t index = 0; index < written.data_pages.size(); ++index) {
        const auto& page = written.data_pages[index];
        add_page(page,
                 fmt::format("{} data_page_{:03} rows={}", page_case.name, index, page_rows(page)));
    }
    if (!written.dict_page.empty()) {
        add_page(written.dict_page, fmt::format("{} dict_page", page_case.name));
    }
}

Result<GoldenFiles> read_golden_files(const std::filesystem::path& directory) {
    GoldenFiles files;
    std::error_code error;
    for (std::filesystem::directory_iterator entry(directory, error), end; !error && entry != end;
         entry.increment(error)) {
        std::ifstream input(entry->path(), std::ios::binary);
        files.emplace(entry->path().filename().string(),
                      std::string(std::istreambuf_iterator<char>(input), {}));
    }
    if (error) {
        return ResultError(Status::IOError("failed to list golden directory {}: {}",
                                           directory.string(), error.message()));
    }
    return files;
}

std::vector<std::string_view> split_lines(std::string_view text) {
    std::vector<std::string_view> lines;
    while (!text.empty()) {
        const size_t end = std::min(text.find('\n'), text.size());
        lines.push_back(text.substr(0, end));
        text.remove_prefix(std::min(end + 1, text.size()));
    }
    return lines;
}

Status compare_goldens(const Goldens& current) {
    auto golden_result = read_golden_files(kGoldenDir);
    if (!golden_result.has_value()) {
        return golden_result.error();
    }
    const auto& golden = golden_result.value();
    const auto current_files = current.with_layout();
    auto names = [](const GoldenFiles& files) {
        std::vector<std::string_view> names;
        for (const auto& [name, bytes] : files) {
            names.push_back(name);
        }
        return fmt::format("{}", fmt::join(names, ", "));
    };
    if (names(current_files) != names(golden)) {
        return Status::InternalError("golden files changed: current [{}], golden [{}]",
                                     names(current_files), names(golden));
    }
    const auto layout = split_lines(current_files.at("layout.txt"));
    const auto golden_layout = split_lines(golden.at("layout.txt"));
    if (layout != golden_layout) {
        const auto [line, golden_line] = std::mismatch(layout.begin(), layout.end(),
                                                       golden_layout.begin(), golden_layout.end());
        return Status::InternalError(
                "layout.txt changed at line {}: current \"{}\", golden \"{}\"",
                std::distance(layout.begin(), line) + 1,
                line == layout.end() ? std::string_view() : *line,
                golden_line == golden_layout.end() ? std::string_view() : *golden_line);
    }
    // The layouts match, so each page sits at the same offset in both files.
    std::vector<std::string> changed;
    for (const auto& [name, bytes] : current.files) {
        const auto& golden_bytes = golden.at(name);
        if (bytes == golden_bytes) {
            continue;
        }
        if (bytes.size() != golden_bytes.size()) {
            changed.push_back(fmt::format("{} has {} bytes, golden {}", name, bytes.size(),
                                          golden_bytes.size()));
            continue;
        }
        const auto offset = static_cast<size_t>(std::distance(
                bytes.begin(),
                std::mismatch(bytes.begin(), bytes.end(), golden_bytes.begin(), golden_bytes.end())
                        .first));
        size_t page_offset = 0;
        for (const auto& page : current.pages) {
            if (page.file != name) {
                continue;
            }
            if (offset < page_offset + page.bytes) {
                changed.push_back(
                        fmt::format("{} of {} at byte {}", page.label, name, offset - page_offset));
                break;
            }
            page_offset += page.bytes;
        }
    }
    if (!changed.empty()) {
        return Status::InternalError("page bytes changed: {}", fmt::join(changed, "; "));
    }
    return Status::OK();
}

Result<std::filesystem::path> golden_output_root(std::string_view output_dir) {
    if (output_dir.empty()) {
        return ResultError(Status::InvalidArgument("{} must not be empty", kGoldenOutputDirEnv));
    }
    std::error_code error;
    const auto output_root = std::filesystem::weakly_canonical(output_dir, error);
    if (error) {
        return ResultError(
                Status::IOError("failed to resolve {}: {}", kGoldenOutputDirEnv, error.message()));
    }
    const auto checked_in_root = std::filesystem::weakly_canonical(kGoldenDir, error);
    if (error) {
        return ResultError(Status::IOError("failed to resolve checked-in golden directory: {}",
                                           error.message()));
    }
    if (io::LocalFileSystem::contain_path(checked_in_root, output_root)) {
        return ResultError(Status::InvalidArgument(
                "{} must be outside the checked-in golden directory", kGoldenOutputDirEnv));
    }
    return output_root;
}

Status write_golden_files(const std::filesystem::path& directory, const GoldenFiles& files) {
    RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(directory,
                                                                    /*failed_if_exists=*/true));
    for (const auto& [name, bytes] : files) {
        std::ofstream output(directory / name, std::ios::binary);
        output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        if (!output) {
            return Status::IOError("failed to write {}", (directory / name).string());
        }
    }
    return Status::OK();
}

} // namespace

class PageFormatTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_disable_storage_page_cache = config::disable_storage_page_cache;
        config::disable_storage_page_cache = true;
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(kTestDir).ok());
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
        config::disable_storage_page_cache = _saved_disable_storage_page_cache;
    }

private:
    bool _saved_disable_storage_page_cache = false;
};

// Registering a storage type's encoding without a spec above would leave its pages unpinned.
TEST_F(PageFormatTest, EveryRegisteredStorageTypeHasASpec) {
    std::set<FieldType> specified;
    for (const auto& spec : kTypeSpecs) {
        specified.insert(TabletColumn::get_field_type_by_string(std::string(spec.storage_type)));
    }
    // Beyond the last FieldType, so that a new one is checked too.
    for (int value = 0; value < 64; ++value) {
        const auto type = static_cast<FieldType>(value);
        if (type == FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT ||
            type == FieldType::OLAP_FIELD_TYPE_VARIANT || specified.contains(type)) {
            continue;
        }
        EXPECT_TRUE(registered_encodings(type).empty()) << "storage type " << value;
    }
}

// Each page encoding has a golden file holding, column after column, the pages the column writer
// wrote with it as they are on disk: the encoded values, the null map, the page footer and the
// checksum. layout.txt lists the pages. Changing these bytes changes the on-disk format; regenerate
// the goldens with DORIS_PAGE_FORMAT_GOLDEN_OUTPUT_DIR only for an intended format change.
TEST_F(PageFormatTest, RegisteredEncodingsKeepTheirPageBytes) {
    Goldens goldens;
    for (const auto& spec : kTypeSpecs) {
        auto input = create_input(spec);
        ASSERT_TRUE(input.has_value()) << spec.name << ": " << input.error();
        for (const auto& page_case : page_format_cases(spec, input.value().column.type())) {
            SCOPED_TRACE(page_case.name);
            const auto path = fmt::format("{}/{}.dat", kTestDir, page_case.name);
            auto written = write_column(page_case, input.value(), path);
            ASSERT_TRUE(written.has_value()) << written.error();
            EXPECT_GE(written.value().data_pages.size(), 2U);
            auto rewritten = write_column(page_case, input.value(), path + ".repeat");
            ASSERT_TRUE(rewritten.has_value()) << rewritten.error();
            EXPECT_TRUE(written.value().data_pages == rewritten.value().data_pages &&
                        written.value().dict_page == rewritten.value().dict_page)
                    << "page bytes are not deterministic";
            const auto read_back =
                    verify_read_back(page_case, input.value(), written.value(), path);
            EXPECT_TRUE(read_back.ok()) << read_back;
            add_goldens(&goldens, page_case, written.value());
        }
    }
    if (const char* output_dir = std::getenv(kGoldenOutputDirEnv.data()); output_dir != nullptr) {
        auto output_root = golden_output_root(output_dir);
        ASSERT_TRUE(output_root.has_value()) << output_root.error();
        const auto dumped = write_golden_files(output_root.value(), goldens.with_layout());
        EXPECT_TRUE(dumped.ok()) << dumped;
    } else {
        const auto compared = compare_goldens(goldens);
        EXPECT_TRUE(compared.ok()) << compared;
    }
}

} // namespace doris::segment_v2
