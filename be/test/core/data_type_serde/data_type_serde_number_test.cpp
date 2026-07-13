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

#include <arrow/api.h>
#include <cctz/time_zone.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <streamvbyte.h>

#include <cmath>
#include <cstddef>
#include <iostream>
#include <limits>
#include <type_traits>

#include "common/config.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/data_type/common_data_type_serder_test.h"
#include "core/data_type/common_data_type_test.h"
#include "core/data_type/data_type.h"
#include "core/data_type/primitive_type.h"
#include "core/data_type_serde/data_type_date_or_datetime_serde.h"
#include "core/data_type_serde/data_type_datetimev2_serde.h"
#include "core/data_type_serde/data_type_datev2_serde.h"
#include "core/types.h"
#include "testutil/test_util.h"
#include "util/slice.h"
#include "util/string_util.h"

namespace doris {
static std::string test_data_dir;

static auto serde_float32 = std::make_shared<DataTypeNumberSerDe<TYPE_FLOAT>>();
static auto serde_float64 = std::make_shared<DataTypeNumberSerDe<TYPE_DOUBLE>>();
static auto serde_int8 = std::make_shared<DataTypeNumberSerDe<TYPE_TINYINT>>();
static auto serde_int16 = std::make_shared<DataTypeNumberSerDe<TYPE_SMALLINT>>();
static auto serde_int32 = std::make_shared<DataTypeNumberSerDe<TYPE_INT>>();
static auto serde_int64 = std::make_shared<DataTypeNumberSerDe<TYPE_BIGINT>>();
static auto serde_int128 = std::make_shared<DataTypeNumberSerDe<TYPE_LARGEINT>>();
static auto serde_uint8 = std::make_shared<DataTypeNumberSerDe<TYPE_BOOLEAN>>();
static auto serde_datev2_num = std::make_shared<DataTypeNumberSerDe<TYPE_DATEV2>>();
static auto serde_datetimev2_num = std::make_shared<DataTypeNumberSerDe<TYPE_DATETIMEV2>>();

static ColumnFloat32::MutablePtr column_float32;
static ColumnFloat64::MutablePtr column_float64;
static ColumnInt8::MutablePtr column_int8;
static ColumnInt16::MutablePtr column_int16;
static ColumnInt32::MutablePtr column_int32;
static ColumnInt64::MutablePtr column_int64;
static ColumnInt128::MutablePtr column_int128;
static ColumnUInt8::MutablePtr column_uint8;

class DataTypeNumberSerDeTest : public ::testing::Test {
public:
    static void SetUpTestSuite() {
        auto root_dir = std::string(getenv("ROOT"));
        test_data_dir = root_dir + "/be/test/data/vec/columns";

        column_float32 = ColumnFloat32::create();
        column_float64 = ColumnFloat64::create();

        column_int8 = ColumnInt8::create();
        column_int16 = ColumnInt16::create();
        column_int32 = ColumnInt32::create();
        column_int64 = ColumnInt64::create();
        column_int128 = ColumnInt128::create();

        column_uint8 = ColumnUInt8::create();

        load_columns_data();
    }
    static void load_columns_data() {
        std::cout << "loading test dataset" << std::endl;
        auto test_func = [&](const MutableColumnPtr& column, const auto& serde,
                             const std::string& data_file_name) {
            MutableColumns columns;
            columns.push_back(column->get_ptr());
            DataTypeSerDeSPtrs serdes = {serde};
            load_columns_data_from_file(columns, serdes, ';', {0},
                                        test_data_dir + "/" + data_file_name);
            EXPECT_TRUE(!column->empty());
        };
        test_func(column_float32->get_ptr(), serde_float32, "FLOAT.csv");
        test_func(column_float64->get_ptr(), serde_float64, "DOUBLE.csv");

        test_func(column_int8->get_ptr(), serde_int8, "TINYINT.csv");
        test_func(column_int16->get_ptr(), serde_int16, "SMALLINT.csv");
        test_func(column_int32->get_ptr(), serde_int32, "INT.csv");
        test_func(column_int64->get_ptr(), serde_int64, "BIGINT.csv");
        test_func(column_int128->get_ptr(), serde_int128, "LARGEINT.csv");

        test_func(column_uint8->get_ptr(), serde_uint8, "TINYINT_UNSIGNED.csv");
    }
};

class RowStoreCompactJsonbConfigGuard {
public:
    explicit RowStoreCompactJsonbConfigGuard(bool enabled)
            : _old_value(config::enable_row_store_compact_jsonb) {
        config::enable_row_store_compact_jsonb = enabled;
    }

    ~RowStoreCompactJsonbConfigGuard() { config::enable_row_store_compact_jsonb = _old_value; }

private:
    bool _old_value;
};

TEST_F(DataTypeNumberSerDeTest, serdes) {
    auto test_func = [](const auto& serde, const auto& source_column) {
        using SerdeType = decltype(serde);
        using ColumnType = typename std::remove_reference<SerdeType>::type::ColumnType;

        auto row_count = source_column->size();
        auto option = DataTypeSerDe::FormatOptions();
        char field_delim = ';';
        option.field_delim = std::string(1, field_delim);

        {
            auto ser_col = ColumnString::create();
            ser_col->reserve(row_count);
            MutableColumnPtr deser_column = source_column->clone_empty();
            const auto* deser_col_with_type = assert_cast<const ColumnType*>(deser_column.get());

            VectorBufferWriter buffer_writer(*ser_col.get());
            for (size_t j = 0; j != row_count; ++j) {
                auto st =
                        serde.serialize_one_cell_to_json(*source_column, j, buffer_writer, option);
                EXPECT_TRUE(st.ok()) << "Failed to serialize column at row " << j << ": " << st;

                buffer_writer.commit();
                std::string actual_str_value = ser_col->get_data_at(j).to_string();
                Slice slice {actual_str_value.data(), actual_str_value.size()};
                st = serde.deserialize_one_cell_from_json(*deser_column, slice, option);
                EXPECT_TRUE(st.ok()) << "Failed to deserialize column at row " << j << ": " << st;
                if constexpr (std::is_same_v<ColumnType, ColumnFloat32> ||
                              std::is_same_v<ColumnType, ColumnFloat64>) {
                    // for float and double, we need to check the value with a tolerance
                    auto expected_value = source_column->get_element(j);
                    auto actual_value = deser_col_with_type->get_element(j);
                    if (std::isnan(expected_value)) {
                        EXPECT_TRUE(std::isnan(actual_value))
                                << "Row " << j << " value mismatch: expected NaN, got "
                                << actual_value;
                    } else if (std::isinf(expected_value)) {
                        EXPECT_EQ(actual_value, expected_value);
                    } else {
                        EXPECT_NEAR(actual_value, expected_value, 0.00001)
                                << "Row " << j << " value mismatch: expected " << expected_value
                                << ", got " << actual_value;
                    }
                } else {
                    EXPECT_EQ(deser_col_with_type->get_element(j), source_column->get_element(j));
                }
            }
        }

        // test serialize_column_to_json
        {
            auto ser_col = ColumnString::create();
            ser_col->reserve(row_count);

            VectorBufferWriter buffer_writer(*ser_col.get());
            auto st = serde.serialize_column_to_json(*source_column, 0, source_column->size(),
                                                     buffer_writer, option);
            EXPECT_TRUE(st.ok()) << "Failed to serialize column to json: " << st;
            buffer_writer.commit();

            std::string json_data((char*)ser_col->get_chars().data(), ser_col->get_chars().size());
            std::vector<std::string> strs = doris::split(json_data, std::string(1, field_delim));
            std::vector<Slice> slices;
            for (const auto& s : strs) {
                Slice tmp_slice(s.data(), s.size());
                tmp_slice.trim_prefix();
                slices.emplace_back(tmp_slice);
            }

            MutableColumnPtr deser_column = source_column->clone_empty();
            const auto* deser_col_with_type = assert_cast<const ColumnType*>(deser_column.get());
            uint64_t num_deserialized = 0;
            st = serde.deserialize_column_from_json_vector(*deser_column, slices, &num_deserialized,
                                                           option);
            EXPECT_TRUE(st.ok()) << "Failed to deserialize column from json: " << st;
            EXPECT_EQ(num_deserialized, row_count);
            for (size_t j = 0; j != row_count; ++j) {
                if constexpr (std::is_same_v<ColumnType, ColumnFloat32> ||
                              std::is_same_v<ColumnType, ColumnFloat64>) {
                    // for float and double, we need to check the value with a tolerance
                    auto expected_value = source_column->get_element(j);
                    auto actual_value = deser_col_with_type->get_element(j);
                    if (std::isnan(expected_value)) {
                        EXPECT_TRUE(std::isnan(actual_value))
                                << "Row " << j << " value mismatch: expected NaN, got "
                                << actual_value;
                    } else if (std::isinf(expected_value)) {
                        EXPECT_TRUE(std::isinf(actual_value))
                                << "Row " << j << " value mismatch: expected inf, got "
                                << actual_value;
                    } else {
                        // EXPECT_EQ(actual_value, expected_value);
                        EXPECT_NEAR(actual_value, expected_value, 0.00001)
                                << "Row " << j << " value mismatch: expected " << expected_value
                                << ", got " << actual_value;
                    }
                } else {
                    EXPECT_EQ(deser_col_with_type->get_element(j), source_column->get_element(j));
                }
            }
        }

        {
            // test write_column_to_pb/read_column_from_pb
            PValues pv = PValues();
            Status st = serde.write_column_to_pb(*source_column, pv, 0, row_count);
            EXPECT_TRUE(st.ok()) << "Failed to write column to pb: " << st;

            MutableColumnPtr deser_column = source_column->clone_empty();
            const auto* deser_col_with_type = assert_cast<const ColumnType*>(deser_column.get());
            st = serde.read_column_from_pb(*deser_column, pv);
            EXPECT_TRUE(st.ok()) << "Failed to read column from pb: " << st;
            for (size_t j = 0; j != row_count; ++j) {
                if constexpr (std::is_same_v<ColumnType, ColumnFloat32> ||
                              std::is_same_v<ColumnType, ColumnFloat64>) {
                    // for float and double, we need to check the value with a tolerance
                    auto expected_value = source_column->get_element(j);
                    auto actual_value = deser_col_with_type->get_element(j);
                    if (std::isnan(expected_value)) {
                        EXPECT_TRUE(std::isnan(actual_value))
                                << "Row " << j << " value mismatch: expected NaN, got "
                                << actual_value;
                    } else {
                        EXPECT_EQ(actual_value, expected_value);
                    }
                } else {
                    EXPECT_EQ(deser_col_with_type->get_element(j), source_column->get_element(j));
                }
            }
        }
        {
            // test write_one_cell_to_jsonb/read_one_cell_from_jsonb
            JsonbWriterT<JsonbOutStream> jsonb_writer;
            jsonb_writer.writeStartObject();
            Arena pool;
            DataTypeSerDe::FormatOptions options;
            auto tz = cctz::utc_time_zone();
            options.timezone = &tz;

            for (size_t j = 0; j != row_count; ++j) {
                serde.write_one_cell_to_jsonb(*source_column, jsonb_writer, pool, 0, j, options);
            }
            jsonb_writer.writeEndObject();

            auto ser_col = ColumnString::create();
            ser_col->reserve(row_count);
            MutableColumnPtr deser_column = source_column->clone_empty();
            const auto* deser_col_with_type = assert_cast<const ColumnType*>(deser_column.get());
            const JsonbDocument* pdoc = nullptr;
            auto st = JsonbDocument::checkAndCreateDocument(jsonb_writer.getOutput()->getBuffer(),
                                                            jsonb_writer.getOutput()->getSize(),
                                                            &pdoc);
            ASSERT_TRUE(st.ok()) << "checkAndCreateDocument failed: " << st.to_string();
            const JsonbDocument& doc = *pdoc;
            for (auto it = doc->begin(); it != doc->end(); ++it) {
                serde.read_one_cell_from_jsonb(*deser_column, it->value());
            }
            for (size_t j = 0; j != row_count; ++j) {
                if constexpr (std::is_same_v<ColumnType, ColumnFloat32> ||
                              std::is_same_v<ColumnType, ColumnFloat64>) {
                    // for float and double, we need to check the value with a tolerance
                    auto expected_value = source_column->get_element(j);
                    auto actual_value = deser_col_with_type->get_element(j);
                    if (std::isnan(expected_value)) {
                        EXPECT_TRUE(std::isnan(actual_value))
                                << "Row " << j << " value mismatch: expected NaN, got "
                                << actual_value;
                    } else {
                        EXPECT_EQ(actual_value, expected_value);
                    }
                } else {
                    EXPECT_EQ(deser_col_with_type->get_element(j), source_column->get_element(j));
                }
            }
        }
    };
    test_func(*serde_float32, column_float32);
    test_func(*serde_float64, column_float64);
    test_func(*serde_int8, column_int8);
    test_func(*serde_int16, column_int16);
    test_func(*serde_int32, column_int32);
    test_func(*serde_int64, column_int64);
    test_func(*serde_int128, column_int128);
    test_func(*serde_uint8, column_uint8);
}

// Run with UBSan enabled to catch misalignment errors.
TEST_F(DataTypeNumberSerDeTest, ArrowMemNotAligned) {
    // 1.Prepare the data.
    std::vector<std::string> strings = {"9223372036854775807", "9223372036854775806",
                                        "9223372036854775805", "9223372036854775804",
                                        "9223372036854775803"};

    int32_t total_length = 0;
    std::vector<int32_t> offsets = {0};
    for (const auto& str : strings) {
        total_length += static_cast<int32_t>(str.length());
        offsets.push_back(total_length);
    }

    // 2.Create an unaligned memory buffer.
    std::vector<uint8_t> value_storage(total_length + 10);
    std::vector<uint8_t> offset_storage((strings.size() + 1) * sizeof(int32_t) + 10);

    uint8_t* unaligned_value_data = value_storage.data() + 1;
    uint8_t* unaligned_offset_data = offset_storage.data() + 1;

    // 3. Copy data to unaligned memory
    int32_t current_pos = 0;
    for (size_t i = 0; i < strings.size(); ++i) {
        memcpy(unaligned_value_data + current_pos, strings[i].data(), strings[i].length());
        current_pos += strings[i].length();
    }

    for (size_t i = 0; i < offsets.size(); ++i) {
        memcpy(unaligned_offset_data + i * sizeof(int32_t), &offsets[i], sizeof(int32_t));
    }

    // 4. Create Arrow array with unaligned memory
    auto value_buffer = arrow::Buffer::Wrap(unaligned_value_data, total_length);
    auto offset_buffer =
            arrow::Buffer::Wrap(unaligned_offset_data, offsets.size() * sizeof(int32_t));
    auto arr = std::make_shared<arrow::StringArray>(strings.size(), offset_buffer, value_buffer);

    const auto* offsets_ptr = arr->raw_value_offsets();
    uintptr_t address = reinterpret_cast<uintptr_t>(offsets_ptr);
    EXPECT_EQ((reinterpret_cast<uintptr_t>(address) % 4), 1);

    // 5.Test read_column_from_arrow
    cctz::time_zone tz;
    auto st = serde_int128->read_column_from_arrow(*column_int128, arr.get(), 0, 1, tz);
    EXPECT_TRUE(st.ok());
}

TEST_F(DataTypeNumberSerDeTest, ArrowStringToUnsignedDateLikeTypes) {
    std::vector<std::string> strings = {"20240102", "20240102112233"};
    std::vector<int32_t> offsets = {0};
    int32_t total_length = 0;
    for (const auto& str : strings) {
        total_length += static_cast<int32_t>(str.length());
        offsets.push_back(total_length);
    }

    std::string value_bytes;
    value_bytes.reserve(total_length);
    for (const auto& str : strings) {
        value_bytes.append(str);
    }

    auto value_buffer = arrow::Buffer::Wrap(value_bytes.data(), value_bytes.size());
    auto offset_buffer = arrow::Buffer::Wrap(offsets);
    auto arr = std::make_shared<arrow::StringArray>(strings.size(), offset_buffer, value_buffer);

    auto datev2_column = ColumnVector<TYPE_DATEV2>::create();
    auto datetimev2_column = ColumnVector<TYPE_DATETIMEV2>::create();
    cctz::time_zone tz;

    auto st = serde_datev2_num->read_column_from_arrow(*datev2_column, arr.get(), 0, 1, tz);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(1, datev2_column->size());
    EXPECT_EQ(20240102U, datev2_column->get_data()[0].to_date_int_val());

    st = serde_datetimev2_num->read_column_from_arrow(*datetimev2_column, arr.get(), 1, 2, tz);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(1, datetimev2_column->size());
    EXPECT_EQ(20240102112233ULL, datetimev2_column->get_data()[0].to_date_int_val());
}

static JsonbType expected_jsonb_integer_type(int128_t value) {
    if (value >= std::numeric_limits<int8_t>::min() &&
        value <= std::numeric_limits<int8_t>::max()) {
        return JsonbType::T_Int8;
    }
    if (value >= std::numeric_limits<int16_t>::min() &&
        value <= std::numeric_limits<int16_t>::max()) {
        return JsonbType::T_Int16;
    }
    if (value >= std::numeric_limits<int32_t>::min() &&
        value <= std::numeric_limits<int32_t>::max()) {
        return JsonbType::T_Int32;
    }
    if (value >= std::numeric_limits<int64_t>::min() &&
        value <= std::numeric_limits<int64_t>::max()) {
        return JsonbType::T_Int64;
    }
    return JsonbType::T_Int128;
}

template <typename SerDeType>
void check_number_row_store_jsonb_width(const SerDeType& serde,
                                        const typename SerDeType::ColumnType& source_column,
                                        JsonbType expected_type) {
    JsonbWriterT<JsonbOutStream> jsonb_writer;
    jsonb_writer.writeStartObject();
    Arena pool;
    DataTypeSerDe::FormatOptions options;
    auto tz = cctz::utc_time_zone();
    options.timezone = &tz;
    options.enable_row_store_compact_jsonb = config::enable_row_store_compact_jsonb;
    serde.write_one_cell_to_jsonb(source_column, jsonb_writer, pool, 0, 0, options);
    jsonb_writer.writeEndObject();

    const JsonbDocument* pdoc = nullptr;
    auto st = JsonbDocument::checkAndCreateDocument(jsonb_writer.getOutput()->getBuffer(),
                                                    jsonb_writer.getOutput()->getSize(), &pdoc);
    ASSERT_TRUE(st.ok()) << "checkAndCreateDocument failed: " << st;
    const JsonbDocument& doc = *pdoc;
    auto it = doc->begin();
    ASSERT_TRUE(it != doc->end());
    ASSERT_EQ(it->value()->type, expected_type);

    MutableColumnPtr dst = source_column.clone_empty();
    serde.read_one_cell_from_jsonb(*dst, it->value());
    ASSERT_EQ(dst->size(), 1);
    const auto* typed_dst = assert_cast<const typename SerDeType::ColumnType*>(dst.get());
    EXPECT_EQ(typed_dst->get_element(0), source_column.get_element(0));
}

template <PrimitiveType T>
void check_number_row_store_jsonb_width(typename PrimitiveTypeTraits<T>::CppType value,
                                        JsonbType expected_type) {
    using ColumnType = typename PrimitiveTypeTraits<T>::ColumnType;
    auto column = ColumnType::create();
    column->insert_value(value);
    DataTypeNumberSerDe<T> serde;
    check_number_row_store_jsonb_width(serde, *column, expected_type);
}

template <PrimitiveType T>
void check_number_row_store_jsonb_read_compat(const JsonbValue* value,
                                              typename PrimitiveTypeTraits<T>::CppType expected) {
    using ColumnType = typename PrimitiveTypeTraits<T>::ColumnType;
    auto dst = ColumnType::create();
    DataTypeNumberSerDe<T> serde;
    serde.read_one_cell_from_jsonb(*dst, value);
    ASSERT_EQ(dst->size(), 1);
    EXPECT_EQ(dst->get_element(0), expected);
}

template <PrimitiveType T, typename Writer>
void check_number_row_store_jsonb_encoded_read_compat(
        typename PrimitiveTypeTraits<T>::CppType expected, JsonbType expected_type,
        Writer write_value) {
    JsonbWriterT<JsonbOutStream> jsonb_writer;
    jsonb_writer.writeStartObject();
    jsonb_writer.writeKey(static_cast<JsonbKeyValue::keyid_type>(0));
    write_value(jsonb_writer);
    jsonb_writer.writeEndObject();

    const JsonbDocument* pdoc = nullptr;
    auto st = JsonbDocument::checkAndCreateDocument(jsonb_writer.getOutput()->getBuffer(),
                                                    jsonb_writer.getOutput()->getSize(), &pdoc);
    ASSERT_TRUE(st.ok()) << st;
    auto it = (*pdoc)->begin();
    ASSERT_TRUE(it != (*pdoc)->end());
    ASSERT_EQ(it->value()->type, expected_type);
    check_number_row_store_jsonb_read_compat<T>(it->value(), expected);
}

TEST_F(DataTypeNumberSerDeTest, RowStoreIntegerJsonbWidth) {
    {
        RowStoreCompactJsonbConfigGuard guard(false);
        check_number_row_store_jsonb_width<TYPE_BOOLEAN>(1, JsonbType::T_Int8);
        check_number_row_store_jsonb_width<TYPE_TINYINT>(-1, JsonbType::T_Int8);
        check_number_row_store_jsonb_width<TYPE_SMALLINT>(1, JsonbType::T_Int16);
        check_number_row_store_jsonb_width<TYPE_INT>(1, JsonbType::T_Int32);
        check_number_row_store_jsonb_width<TYPE_BIGINT>(1, JsonbType::T_Int64);
        check_number_row_store_jsonb_width<TYPE_LARGEINT>(static_cast<Int128>(1),
                                                          JsonbType::T_Int128);
    }

    {
        RowStoreCompactJsonbConfigGuard guard(true);
        check_number_row_store_jsonb_width<TYPE_BOOLEAN>(1, JsonbType::T_Int8);
        check_number_row_store_jsonb_width<TYPE_TINYINT>(-1, JsonbType::T_Int8);
        check_number_row_store_jsonb_width<TYPE_SMALLINT>(1, JsonbType::T_Int8);
        check_number_row_store_jsonb_width<TYPE_SMALLINT>(128, JsonbType::T_Int16);
        check_number_row_store_jsonb_width<TYPE_INT>(1, JsonbType::T_Int8);
        check_number_row_store_jsonb_width<TYPE_INT>(128, JsonbType::T_Int16);
        check_number_row_store_jsonb_width<TYPE_INT>(32768, JsonbType::T_Int32);

        check_number_row_store_jsonb_width<TYPE_BIGINT>(1, JsonbType::T_Int8);
        check_number_row_store_jsonb_width<TYPE_BIGINT>(128, JsonbType::T_Int16);
        check_number_row_store_jsonb_width<TYPE_BIGINT>(32768, JsonbType::T_Int32);
        check_number_row_store_jsonb_width<TYPE_BIGINT>(static_cast<Int64>(1) << 40,
                                                        JsonbType::T_Int64);

        check_number_row_store_jsonb_width<TYPE_LARGEINT>(static_cast<Int128>(1),
                                                          JsonbType::T_Int8);
        check_number_row_store_jsonb_width<TYPE_LARGEINT>(static_cast<Int128>(1) << 40,
                                                          JsonbType::T_Int64);
        check_number_row_store_jsonb_width<TYPE_LARGEINT>(static_cast<Int128>(1) << 100,
                                                          JsonbType::T_Int128);
    }

    // Backward compatibility: existing row-store data may still contain fixed-width tags.
    check_number_row_store_jsonb_encoded_read_compat<TYPE_BOOLEAN>(
            1, JsonbType::T_Int8, [](JsonbWriter& writer) { writer.writeInt8(1); });
    check_number_row_store_jsonb_encoded_read_compat<TYPE_TINYINT>(
            -1, JsonbType::T_Int8, [](JsonbWriter& writer) { writer.writeInt8(-1); });
    check_number_row_store_jsonb_encoded_read_compat<TYPE_SMALLINT>(
            1, JsonbType::T_Int16, [](JsonbWriter& writer) { writer.writeInt16(1); });
    check_number_row_store_jsonb_encoded_read_compat<TYPE_INT>(
            1, JsonbType::T_Int32, [](JsonbWriter& writer) { writer.writeInt32(1); });
    check_number_row_store_jsonb_encoded_read_compat<TYPE_BIGINT>(
            1, JsonbType::T_Int64, [](JsonbWriter& writer) { writer.writeInt64(1); });
    check_number_row_store_jsonb_encoded_read_compat<TYPE_LARGEINT>(
            static_cast<Int128>(1), JsonbType::T_Int128,
            [](JsonbWriter& writer) { writer.writeInt128(1); });

    // The reader accepts compact tags even when compact writes are disabled.
    RowStoreCompactJsonbConfigGuard guard(false);
    check_number_row_store_jsonb_encoded_read_compat<TYPE_BIGINT>(
            1, JsonbType::T_Int8, [](JsonbWriter& writer) { writer.writeInt8(1); });
    check_number_row_store_jsonb_encoded_read_compat<TYPE_LARGEINT>(
            static_cast<Int128>(1), JsonbType::T_Int8,
            [](JsonbWriter& writer) { writer.writeInt8(1); });
}

TEST_F(DataTypeNumberSerDeTest, RowStoreDateTimeJsonbWidth) {
    VecDateTimeValue date;
    date.unchecked_set_time(2026, 5, 20, 0, 0, 0);
    auto date_column = ColumnDate::create();
    date_column->insert_value(date);
    DataTypeDateSerDe<> date_serde;
    {
        RowStoreCompactJsonbConfigGuard guard(false);
        check_number_row_store_jsonb_width(date_serde, *date_column, JsonbType::T_Int64);
    }
    {
        RowStoreCompactJsonbConfigGuard guard(true);
        check_number_row_store_jsonb_width(
                date_serde, *date_column,
                expected_jsonb_integer_type(*reinterpret_cast<const Int64*>(&date)));
    }
    const auto date_raw_value = *reinterpret_cast<const Int64*>(&date);
    check_number_row_store_jsonb_encoded_read_compat<TYPE_DATE>(
            date, JsonbType::T_Int64,
            [date_raw_value](JsonbWriter& writer) { writer.writeInt64(date_raw_value); });

    VecDateTimeValue datetime;
    datetime.unchecked_set_time(2026, 5, 20, 1, 2, 3);
    auto datetime_column = ColumnDateTime::create();
    datetime_column->insert_value(datetime);
    DataTypeDateTimeSerDe datetime_serde(0);
    {
        RowStoreCompactJsonbConfigGuard guard(false);
        check_number_row_store_jsonb_width(datetime_serde, *datetime_column, JsonbType::T_Int64);
    }
    {
        RowStoreCompactJsonbConfigGuard guard(true);
        check_number_row_store_jsonb_width(
                datetime_serde, *datetime_column,
                expected_jsonb_integer_type(*reinterpret_cast<const Int64*>(&datetime)));
    }
    const auto datetime_raw_value = *reinterpret_cast<const Int64*>(&datetime);
    check_number_row_store_jsonb_encoded_read_compat<TYPE_DATETIME>(
            datetime, JsonbType::T_Int64,
            [datetime_raw_value](JsonbWriter& writer) { writer.writeInt64(datetime_raw_value); });

    DateV2Value<DateV2ValueType> datev2;
    datev2.unchecked_set_time(2026, 5, 20, 0, 0, 0, 0);
    {
        RowStoreCompactJsonbConfigGuard guard(false);
        check_number_row_store_jsonb_width<TYPE_DATEV2>(datev2, JsonbType::T_Int32);
    }
    {
        RowStoreCompactJsonbConfigGuard guard(true);
        check_number_row_store_jsonb_width<TYPE_DATEV2>(
                datev2, expected_jsonb_integer_type(*reinterpret_cast<const UInt32*>(&datev2)));
    }
    const auto datev2_raw_value = *reinterpret_cast<const UInt32*>(&datev2);
    check_number_row_store_jsonb_encoded_read_compat<TYPE_DATEV2>(
            datev2, JsonbType::T_Int32, [datev2_raw_value](JsonbWriter& writer) {
                writer.writeInt32(static_cast<Int32>(datev2_raw_value));
            });

    DateV2Value<DateTimeV2ValueType> datetimev2;
    datetimev2.unchecked_set_time(2026, 5, 20, 1, 2, 3, 0);
    {
        RowStoreCompactJsonbConfigGuard guard(false);
        check_number_row_store_jsonb_width<TYPE_DATETIMEV2>(datetimev2, JsonbType::T_Int64);
    }
    {
        RowStoreCompactJsonbConfigGuard guard(true);
        check_number_row_store_jsonb_width<TYPE_DATETIMEV2>(
                datetimev2,
                expected_jsonb_integer_type(*reinterpret_cast<const UInt64*>(&datetimev2)));
    }
    const auto datetimev2_raw_value = *reinterpret_cast<const UInt64*>(&datetimev2);
    check_number_row_store_jsonb_encoded_read_compat<TYPE_DATETIMEV2>(
            datetimev2, JsonbType::T_Int64, [datetimev2_raw_value](JsonbWriter& writer) {
                writer.writeInt64(static_cast<Int64>(datetimev2_raw_value));
            });
}

} // namespace doris
