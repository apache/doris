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

#include <simdutf.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
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

Status conversion_error(std::string_view character_set_name, simdutf::error_code error) {
    return Status::InvalidArgument("Character conversion using '{}' failed: {}", character_set_name,
                                   simdutf::error_to_string(error));
}

Status reject_too_large(std::string_view character_set_name) {
    return Status::InvalidArgument("Input is too large for character conversion using '{}'",
                                   character_set_name);
}

// Grow the byte buffer, then return the address of the newly reserved range.
char* reserve_output(ColumnString::Chars& output, size_t extra) {
    const size_t start = output.size();
    ColumnString::check_chars_length(start + extra, 0);
    output.resize(start + extra);
    return reinterpret_cast<char*>(output.data() + start);
}

Status copy_validated(StringRef input, std::string_view character_set_name,
                      simdutf::result validation, ColumnString::Chars& output) {
    if (validation.error != simdutf::SUCCESS) {
        return conversion_error(character_set_name, validation.error);
    }
    memcpy(reserve_output(output, input.size), input.data, input.size);
    return Status::OK();
}

// Doris string bytes are not guaranteed to be char16_t-aligned.
const char16_t* utf16_units(StringRef input, std::vector<char16_t>& aligned) {
    if (reinterpret_cast<uintptr_t>(input.data) % alignof(char16_t) == 0) {
        return reinterpret_cast<const char16_t*>(input.data);
    }
    const size_t units = input.size / 2;
    aligned.resize(units);
    memcpy(aligned.data(), input.data, input.size);
    return aligned.data();
}

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

    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* /*context*/, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto [input_column, input_is_const] =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        auto [character_set_column, character_set_is_const] =
                unpack_if_const(block.get_by_position(arguments[1]).column);
        DCHECK(character_set_is_const);
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
        if constexpr (Encode) {
            result_column->get_data().reserve(input_rows_count);
        } else {
            result_column->reserve(input_rows_count);
        }
        ColumnUInt8::MutablePtr result_null_column;
        if (has_nullable) {
            result_null_column = ColumnUInt8::create(input_rows_count, 0);
        }
        // Varbinary owns out-of-line values in an arena and inlines small values. Share one
        // tracked scratch buffer across rows so that its capacity is not retained per row.
        ColumnString::Chars scratch;
        std::vector<char16_t> utf16_scratch;
        CharacterSet constant_character_set = CharacterSet::UTF_8;
        if (character_set_is_const && input_rows_count != 0 &&
            !(character_set_null_map && (*character_set_null_map)[0])) {
            RETURN_IF_ERROR(
                    parse_character_set(character_sets.get_data_at(0), constant_character_set));
        }

        for (size_t row = 0; row < input_rows_count; ++row) {
            const size_t input_index = index_check_const(row, input_is_const);
            const bool input_is_null = input_null_map && (*input_null_map)[input_index];
            const bool character_set_is_null =
                    character_set_null_map && (*character_set_null_map)[0];
            if (input_is_null || character_set_is_null) {
                result_column->insert_default();
                result_null_column->get_data()[row] = 1;
                continue;
            }

            const StringRef input = input_nested->get_data_at(input_index);
            if constexpr (Encode) {
                scratch.clear();
                RETURN_IF_ERROR(
                        convert_input(input, constant_character_set, utf16_scratch, scratch));
                result_column->insert_data(reinterpret_cast<const char*>(scratch.data()),
                                           scratch.size());
            } else {
                // Write straight into the result column, including when the buffer grows.
                auto& chars = result_column->get_chars();
                RETURN_IF_ERROR(convert_input(input, constant_character_set, utf16_scratch, chars));
                result_column->get_offsets().push_back(chars.size());
            }
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

    static typename ResultColumn::MutablePtr create_result_column() {
        return ResultColumn::create();
    }

    static std::string_view charset_name(CharacterSet character_set) {
        return SUPPORTED_CHARACTER_SETS[static_cast<size_t>(character_set)];
    }

    static Status convert_input(StringRef input, CharacterSet character_set,
                                std::vector<char16_t>& utf16_scratch,
                                ColumnString::Chars& converted) {
        if (input.size == 0) {
            return Status::OK();
        }
        const std::string_view name = charset_name(character_set);
        if (input.size > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
            return reject_too_large(name);
        }
        if constexpr (Encode) {
            return encode_input(input, character_set, name, utf16_scratch, converted);
        } else {
            return decode_input(input, character_set, name, utf16_scratch, converted);
        }
    }

    static Status encode_input(StringRef input, CharacterSet character_set, std::string_view name,
                               std::vector<char16_t>& utf16_scratch, ColumnString::Chars& output) {
        switch (character_set) {
        case CharacterSet::US_ASCII:
            return copy_validated(input, name,
                                  simdutf::validate_ascii_with_errors(input.data, input.size),
                                  output);
        case CharacterSet::UTF_8:
            return copy_validated(input, name,
                                  simdutf::validate_utf8_with_errors(input.data, input.size),
                                  output);
        case CharacterSet::ISO_8859_1:
            return encode_latin1(input, name, output);
        case CharacterSet::UTF_16BE:
            return encode_utf16(input, name, false, false, utf16_scratch, output);
        case CharacterSet::UTF_16LE:
            return encode_utf16(input, name, true, false, utf16_scratch, output);
        case CharacterSet::UTF_16:
            // Java's UTF-16 encoder always emits a big-endian BOM.
            return encode_utf16(input, name, false, true, utf16_scratch, output);
        default:
            return Status::InvalidArgument("Unsupported character set '{}'", name);
        }
    }

    static Status decode_input(StringRef input, CharacterSet character_set, std::string_view name,
                               std::vector<char16_t>& utf16_scratch, ColumnString::Chars& output) {
        switch (character_set) {
        case CharacterSet::US_ASCII:
            return copy_validated(input, name,
                                  simdutf::validate_ascii_with_errors(input.data, input.size),
                                  output);
        case CharacterSet::UTF_8:
            return copy_validated(input, name,
                                  simdutf::validate_utf8_with_errors(input.data, input.size),
                                  output);
        case CharacterSet::ISO_8859_1:
            return decode_latin1(input, name, output);
        case CharacterSet::UTF_16BE:
            return decode_utf16(input, name, false, utf16_scratch, output);
        case CharacterSet::UTF_16LE:
            return decode_utf16(input, name, true, utf16_scratch, output);
        case CharacterSet::UTF_16:
            return decode_utf16_with_bom(input, name, utf16_scratch, output);
        default:
            return Status::InvalidArgument("Unsupported character set '{}'", name);
        }
    }

    static Status encode_latin1(StringRef input, std::string_view name,
                                ColumnString::Chars& output) {
        const size_t start = output.size();
        char* dest = reserve_output(output, input.size);
        const size_t written = simdutf::convert_utf8_to_latin1(input.data, input.size, dest);
        if (written == 0) {
            const simdutf::result detail =
                    simdutf::convert_utf8_to_latin1_with_errors(input.data, input.size, dest);
            output.resize(start);
            const simdutf::error_code error =
                    detail.error == simdutf::SUCCESS ? simdutf::OTHER : detail.error;
            return conversion_error(name, error);
        }
        output.resize(start + written);
        return Status::OK();
    }

    static Status decode_latin1(StringRef input, std::string_view name,
                                ColumnString::Chars& output) {
        const size_t need = simdutf::utf8_length_from_latin1(input.data, input.size);
        char* dest = reserve_output(output, need);
        const size_t written = simdutf::convert_latin1_to_utf8(input.data, input.size, dest);
        if (written != need) {
            output.resize(output.size() - need);
            return conversion_error(name, simdutf::OTHER);
        }
        return Status::OK();
    }

    static Status encode_utf16(StringRef input, std::string_view name, bool little_endian,
                               bool write_bom, std::vector<char16_t>& utf16_scratch,
                               ColumnString::Chars& output) {
        utf16_scratch.resize(input.size);
        const simdutf::result result =
                little_endian ? simdutf::convert_utf8_to_utf16le_with_errors(input.data, input.size,
                                                                             utf16_scratch.data())
                              : simdutf::convert_utf8_to_utf16be_with_errors(input.data, input.size,
                                                                             utf16_scratch.data());
        if (result.error != simdutf::SUCCESS) {
            return conversion_error(name, result.error);
        }
        const size_t payload_bytes = result.count * sizeof(char16_t);
        const size_t start = output.size();
        char* dest = reserve_output(output, payload_bytes + (write_bom ? 2 : 0));
        if (write_bom) {
            auto* bytes = reinterpret_cast<uint8_t*>(dest);
            bytes[0] = 0xFE;
            bytes[1] = 0xFF;
            dest += 2;
        }
        memcpy(dest, utf16_scratch.data(), payload_bytes);
        return Status::OK();
    }

    // Java's UTF-16 decoder honors either BOM and defaults to big endian without one.
    static Status decode_utf16_with_bom(StringRef input, std::string_view name,
                                        std::vector<char16_t>& utf16_scratch,
                                        ColumnString::Chars& output) {
        if (input.size < 2) {
            return conversion_error(name, simdutf::TOO_SHORT);
        }
        const auto first = static_cast<uint8_t>(input.data[0]);
        const auto second = static_cast<uint8_t>(input.data[1]);
        bool little_endian = false;
        if (first == 0xFE && second == 0xFF) {
            input = input.substring(2);
        } else if (first == 0xFF && second == 0xFE) {
            input = input.substring(2);
            little_endian = true;
        }
        if (input.size == 0) {
            return Status::OK();
        }
        return decode_utf16(input, name, little_endian, utf16_scratch, output);
    }

    static Status decode_utf16(StringRef input, std::string_view name, bool little_endian,
                               std::vector<char16_t>& utf16_scratch, ColumnString::Chars& output) {
        if (input.size % 2 != 0) {
            return conversion_error(name, simdutf::TOO_SHORT);
        }
        const size_t units = input.size / 2;
        if (units > (std::numeric_limits<size_t>::max() / 3)) {
            return reject_too_large(name);
        }
        const char16_t* units_ptr = utf16_units(input, utf16_scratch);
        const size_t start = output.size();
        char* dest = reserve_output(output, units * 3);
        const simdutf::result result =
                little_endian
                        ? simdutf::convert_utf16le_to_utf8_with_errors(units_ptr, units, dest)
                        : simdutf::convert_utf16be_to_utf8_with_errors(units_ptr, units, dest);
        if (result.error != simdutf::SUCCESS) {
            output.resize(start);
            return conversion_error(name, result.error);
        }
        output.resize(start + result.count);
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
