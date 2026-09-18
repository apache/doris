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

#include <unicode/ucnv.h>
#include <unicode/ucnv_err.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_varbinary.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_varbinary.h"
#include "core/string_ref.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {
namespace {

enum class CharacterSet : uint8_t {
    US_ASCII,
    ISO_8859_1,
    UTF_8,
    UTF_16BE,
    UTF_16LE,
    UTF_16,
    SIZE,
};

constexpr std::array<std::string_view, static_cast<size_t>(CharacterSet::SIZE)>
        SUPPORTED_CHARACTER_SETS = {"US-ASCII", "ISO-8859-1", "UTF-8",
                                    "UTF-16BE", "UTF-16LE",   "UTF-16"};

bool equals_ignore_case(StringRef value, std::string_view expected) {
    if (value.size != expected.size()) {
        return false;
    }
    for (size_t i = 0; i < value.size; ++i) {
        const char current = value.data[i] >= 'a' && value.data[i] <= 'z'
                                     ? value.data[i] - ('a' - 'A')
                                     : value.data[i];
        if (current != expected[i]) {
            return false;
        }
    }
    return true;
}

Status parse_character_set(StringRef value, CharacterSet& character_set) {
    for (size_t i = 0; i < SUPPORTED_CHARACTER_SETS.size(); ++i) {
        if (equals_ignore_case(value, SUPPORTED_CHARACTER_SETS[i])) {
            character_set = static_cast<CharacterSet>(i);
            return Status::OK();
        }
    }
    return Status::InvalidArgument(
            "Unsupported character set '{}'. Supported character sets are US-ASCII, "
            "ISO-8859-1, UTF-8, UTF-16BE, UTF-16LE, and UTF-16",
            std::string(value.data, value.size));
}

using ConverterPtr = std::unique_ptr<UConverter, decltype(&ucnv_close)>;

class ConverterPair {
public:
    ConverterPair() : _source(nullptr, ucnv_close), _target(nullptr, ucnv_close) {}

    Status open(std::string_view source_name, std::string_view target_name) {
        UErrorCode error = U_ZERO_ERROR;
        _source.reset(ucnv_open(source_name.data(), &error));
        if (U_FAILURE(error)) {
            return Status::InternalError("Failed to open ICU converter '{}': {}", source_name,
                                         u_errorName(error));
        }

        error = U_ZERO_ERROR;
        ucnv_setToUCallBack(_source.get(), UCNV_TO_U_CALLBACK_STOP, nullptr, nullptr, nullptr,
                            &error);
        if (U_FAILURE(error)) {
            return Status::InternalError("Failed to configure ICU converter '{}': {}", source_name,
                                         u_errorName(error));
        }

        error = U_ZERO_ERROR;
        _target.reset(ucnv_open(target_name.data(), &error));
        if (U_FAILURE(error)) {
            return Status::InternalError("Failed to open ICU converter '{}': {}", target_name,
                                         u_errorName(error));
        }

        error = U_ZERO_ERROR;
        ucnv_setFromUCallBack(_target.get(), UCNV_FROM_U_CALLBACK_STOP, nullptr, nullptr, nullptr,
                              &error);
        if (U_FAILURE(error)) {
            return Status::InternalError("Failed to configure ICU converter '{}': {}", target_name,
                                         u_errorName(error));
        }
        return Status::OK();
    }

    Status convert(StringRef input, std::string_view character_set_name, std::string& output) {
        output.clear();
        if (input.size == 0) {
            return Status::OK();
        }
        if (input.size > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
            return Status::InvalidArgument("Input is too large for character conversion using '{}'",
                                           character_set_name);
        }

        UErrorCode error = U_ZERO_ERROR;
        int32_t utf16_size = ucnv_toUChars(_source.get(), nullptr, 0, input.data,
                                           static_cast<int32_t>(input.size), &error);
        if (error != U_BUFFER_OVERFLOW_ERROR && U_FAILURE(error)) {
            return conversion_error(character_set_name, error);
        }

        _utf16.resize(static_cast<size_t>(utf16_size));
        error = U_ZERO_ERROR;
        ucnv_toUChars(_source.get(), _utf16.data(), utf16_size, input.data,
                      static_cast<int32_t>(input.size), &error);
        if (U_FAILURE(error)) {
            return conversion_error(character_set_name, error);
        }

        error = U_ZERO_ERROR;
        int32_t output_size =
                ucnv_fromUChars(_target.get(), nullptr, 0, _utf16.data(), utf16_size, &error);
        if (error != U_BUFFER_OVERFLOW_ERROR && U_FAILURE(error)) {
            return conversion_error(character_set_name, error);
        }

        output.resize(static_cast<size_t>(output_size));
        error = U_ZERO_ERROR;
        ucnv_fromUChars(_target.get(), output.data(), output_size, _utf16.data(), utf16_size,
                        &error);
        if (U_FAILURE(error)) {
            return conversion_error(character_set_name, error);
        }
        return Status::OK();
    }

private:
    static Status conversion_error(std::string_view character_set_name, UErrorCode error) {
        return Status::InvalidArgument("Character conversion using '{}' failed: {}",
                                       character_set_name, u_errorName(error));
    }

    ConverterPtr _source;
    ConverterPtr _target;
    std::vector<UChar> _utf16;
};

template <bool Encode>
class FunctionCharacterEncoding : public IFunction {
public:
    static constexpr auto name = Encode ? "encode" : "decode";

    static FunctionPtr create() { return std::make_shared<FunctionCharacterEncoding>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DataTypePtr result_type;
        if constexpr (Encode) {
            result_type = std::make_shared<DataTypeVarbinary>();
        } else {
            result_type = std::make_shared<DataTypeString>();
        }
        return have_nullable(arguments) ? make_nullable(result_type) : result_type;
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* /*context*/, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto [input_column, input_is_const] =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        auto [character_set_column, character_set_is_const] =
                unpack_if_const(block.get_by_position(arguments[1]).column);
        const auto* input_nullable = check_and_get_column<ColumnNullable>(input_column.get());
        const auto* character_set_nullable =
                check_and_get_column<ColumnNullable>(character_set_column.get());
        const IColumn* input_nested =
                input_nullable ? &input_nullable->get_nested_column() : input_column.get();
        const IColumn* character_set_nested = character_set_nullable
                                                      ? &character_set_nullable->get_nested_column()
                                                      : character_set_column.get();
        const auto& character_sets = assert_cast<const ColumnString&>(*character_set_nested);
        const NullMap* input_null_map =
                input_nullable ? &input_nullable->get_null_map_data() : nullptr;
        const NullMap* character_set_null_map =
                character_set_nullable ? &character_set_nullable->get_null_map_data() : nullptr;
        const bool has_nullable = input_null_map != nullptr || character_set_null_map != nullptr;
        auto result_column = create_result_column();
        result_column->reserve(input_rows_count);
        ColumnUInt8::MutablePtr result_null_column;
        if (has_nullable) {
            result_null_column = ColumnUInt8::create(input_rows_count, 0);
        }
        ConverterCache converters;
        std::string converted;
        CharacterSet constant_character_set = CharacterSet::UTF_8;
        if (character_set_is_const && input_rows_count != 0 &&
            !(character_set_null_map && (*character_set_null_map)[0])) {
            RETURN_IF_ERROR(
                    parse_character_set(character_sets.get_data_at(0), constant_character_set));
        }

        for (size_t row = 0; row < input_rows_count; ++row) {
            const size_t input_index = index_check_const(row, input_is_const);
            const size_t character_set_index = index_check_const(row, character_set_is_const);
            const bool input_is_null = input_null_map && (*input_null_map)[input_index];
            const bool character_set_is_null =
                    character_set_null_map && (*character_set_null_map)[character_set_index];
            if (input_is_null || character_set_is_null) {
                result_column->insert_default();
                result_null_column->get_data()[row] = 1;
                continue;
            }

            CharacterSet character_set = constant_character_set;
            if (!character_set_is_const) {
                RETURN_IF_ERROR(parse_character_set(character_sets.get_data_at(character_set_index),
                                                    character_set));
            }

            const StringRef input = input_nested->get_data_at(input_index);
            RETURN_IF_ERROR(convert_input(input, character_set, converters, converted));
            result_column->insert_data(converted.data(), converted.size());
        }

        if (has_nullable) {
            block.replace_by_position(result,
                                      ColumnNullable::create(std::move(result_column),
                                                             std::move(result_null_column)));
        } else {
            block.replace_by_position(result, std::move(result_column));
        }
        return Status::OK();
    }

private:
    using ResultColumn = std::conditional_t<Encode, ColumnVarbinary, ColumnString>;
    static constexpr auto UTF16_LITTLE_ENDIAN_CONVERTER = static_cast<size_t>(CharacterSet::SIZE);
    using ConverterCache =
            std::array<std::unique_ptr<ConverterPair>, UTF16_LITTLE_ENDIAN_CONVERTER + 1>;

    struct ConversionSpec {
        StringRef input;
        size_t converter_index;
        std::string_view converter_character_set;
    };

    static typename ResultColumn::MutablePtr create_result_column() {
        return ResultColumn::create();
    }

    static ConversionSpec get_conversion_spec(StringRef input, CharacterSet character_set) {
        const auto character_set_index = static_cast<size_t>(character_set);
        ConversionSpec spec {input, character_set_index,
                             SUPPORTED_CHARACTER_SETS[character_set_index]};
        if (character_set != CharacterSet::UTF_16) {
            return spec;
        }

        // Java's UTF-16 encoder always emits a big-endian BOM. ICU's generic UTF-16 converter
        // follows the host byte order, so encode with UTF-16BE and add the BOM explicitly.
        spec.converter_character_set =
                SUPPORTED_CHARACTER_SETS[static_cast<size_t>(CharacterSet::UTF_16BE)];
        if constexpr (Encode) {
            return spec;
        }

        // Java's UTF-16 decoder honors either BOM and defaults to big endian without a BOM.
        if (input.size < 2) {
            return spec;
        }
        const auto first = static_cast<uint8_t>(input.data[0]);
        const auto second = static_cast<uint8_t>(input.data[1]);
        if (first == 0xFE && second == 0xFF) {
            spec.input = input.substring(2);
        } else if (first == 0xFF && second == 0xFE) {
            spec.input = input.substring(2);
            spec.converter_index = UTF16_LITTLE_ENDIAN_CONVERTER;
            spec.converter_character_set =
                    SUPPORTED_CHARACTER_SETS[static_cast<size_t>(CharacterSet::UTF_16LE)];
        }
        return spec;
    }

    static Status convert_input(StringRef input, CharacterSet character_set,
                                ConverterCache& converters, std::string& converted) {
        const ConversionSpec spec = get_conversion_spec(input, character_set);
        if (converters[spec.converter_index] == nullptr) {
            converters[spec.converter_index] = std::make_unique<ConverterPair>();
            if constexpr (Encode) {
                RETURN_IF_ERROR(converters[spec.converter_index]->open(
                        "UTF-8", spec.converter_character_set));
            } else {
                RETURN_IF_ERROR(converters[spec.converter_index]->open(spec.converter_character_set,
                                                                       "UTF-8"));
            }
        }

        RETURN_IF_ERROR(converters[spec.converter_index]->convert(
                spec.input, SUPPORTED_CHARACTER_SETS[static_cast<size_t>(character_set)],
                converted));
        if constexpr (Encode) {
            if (character_set == CharacterSet::UTF_16 && input.size != 0) {
                converted.insert(0, "\xFE\xFF", 2);
            }
        }
        return Status::OK();
    }
};

using FunctionEncode = FunctionCharacterEncoding<true>;
using FunctionDecode = FunctionCharacterEncoding<false>;

} // namespace

void register_function_character_encoding(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionEncode>();
    factory.register_function<FunctionDecode>();
}

} // namespace doris
