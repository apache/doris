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
#include <glog/logging.h>

#include "common/status.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/types.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"
#include "util/simd/vstring_function.h"

namespace doris {

struct Match {
    std::string::size_type offset;
    std::string::size_type length;
};

class RegexpSplit {
public:
    void init(re2::RE2* re2, int32_t max_splits);
    void set(const char* pos, const char* end);
    bool get(const char*& token_begin, const char*& token_end);

private:
    const char* _begin;
    const char* _pos;
    const char* _end;

    std::int32_t _max_splits = 0;
    std::vector<Match> _matches;
    int32_t _splits;
    re2::RE2* _re2 = nullptr;
    unsigned _number_of_subpatterns = 0;

    unsigned match(const char* subject, size_t subject_size, size_t start_pos,
                   std::vector<Match>& matches, unsigned limit) const;
};

unsigned RegexpSplit::match(const char* subject, size_t subject_size, size_t start_pos,
                            std::vector<Match>& matches, unsigned limit) const {
    matches.clear();

    if (limit == 0) {
        return 0;
    }

    limit = std::min(limit, _number_of_subpatterns + 1);
    std::vector<re2::StringPiece> pieces(limit);

    if (!_re2->Match({subject, subject_size}, start_pos, subject_size, re2::RE2::UNANCHORED,
                     pieces.data(), limit)) {
        return 0;
    } else {
        matches.resize(limit);
        for (size_t i = 0; i < limit; ++i) {
            if (pieces[i].empty()) {
                matches[i].offset = std::string::npos;
                matches[i].length = 0;
            } else {
                matches[i].offset = pieces[i].data() - subject;
                matches[i].length = pieces[i].length();
            }
        }
        return limit;
    }
}

void RegexpSplit::init(re2::RE2* re2, int32_t max_splits) {
    _max_splits = max_splits;
    _re2 = re2;
    if (_re2) {
        _number_of_subpatterns = _re2->NumberOfCapturingGroups();
    }
}

// Called for each next string.
void RegexpSplit::set(const char* pos, const char* end) {
    _begin = pos;
    _pos = pos;
    _end = end;
    _splits = 0;
}

// Get the next token, if any, or return false.
bool RegexpSplit::get(const char*& token_begin, const char*& token_end) {
    if (!_re2) {
        if (_pos == _end) {
            return false;
        }

        token_begin = _pos;
        if (_max_splits != -1) {
            if (_splits == _max_splits - 1) {
                token_end = _end;
                _pos = _end;
                return true;
            }
        }

        const auto utf8_byte_length = get_utf8_byte_length(static_cast<uint8_t>(*_pos));
        _pos += std::min<size_t>(utf8_byte_length, _end - _pos);
        token_end = _pos;
        ++_splits;
    } else {
        if (!_pos || _pos > _end) {
            return false;
        }

        token_begin = _pos;
        if (_max_splits != -1) {
            if (_splits == _max_splits - 1) {
                token_end = _end;
                _pos = nullptr;
                return true;
            }
        }

        if (!match(_begin, _end - _begin, _pos - _begin, _matches, _number_of_subpatterns + 1) ||
            !_matches[0].length) {
            token_end = _end;
            _pos = _end + 1;
        } else {
            token_end = _begin + _matches[0].offset;
            _pos = token_end + _matches[0].length;
            ++_splits;
        }
    }

    return true;
}

template <typename Impl>
class SplitByRegexp : public IFunction {
public:
    static constexpr auto name = "split_by_regexp";

    static FunctionPtr create() { return std::make_shared<SplitByRegexp>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override {
        return get_variadic_argument_types_impl().size();
    }

    bool is_variadic() const override { return true; }

    DataTypes get_variadic_argument_types_impl() const override {
        return Impl::get_variadic_argument_types();
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(is_string_type(arguments[0]->get_primitive_type()))
                << "first argument for function: " << name << " should be string"
                << " and arguments[0] is " << arguments[0]->get_name();
        DCHECK(is_string_type(arguments[1]->get_primitive_type()))
                << "second argument for function: " << name << " should be string"
                << " and arguments[1] is " << arguments[1]->get_name();
        auto nullable_string_type = make_nullable(std::make_shared<DataTypeString>());
        DataTypePtr base = std::make_shared<DataTypeArray>(nullable_string_type);
        if constexpr (Impl::HAS_CONSTANT_LIMIT) {
            // use_default_implementation_for_nulls() is disabled below for this variant, so,
            // unlike the usual get_return_type_impl contract, arguments here keep their original
            // nullability and the wrapping is not automatic: replicate it by hand.
            bool nullable = false;
            for (const auto& argument : arguments) {
                nullable |= argument->is_nullable();
            }
            return nullable ? make_nullable(base) : base;
        }
        return base;
    }

    bool use_default_implementation_for_nulls() const override { return !Impl::HAS_CONSTANT_LIMIT; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        return Impl::execute_impl(context, block, arguments, result, input_rows_count);
    }
};

struct ExecuteImpl {
    using NullMapType = PaddedPODArray<UInt8>;
    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        const auto& [first_column, left_const] =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        const auto& [second_column, right_const] =
                unpack_if_const(block.get_by_position(arguments[1]).column);
        const auto& [three_column, three_is_const] =
                unpack_if_const(block.get_by_position(arguments[2]).column);
        auto limit_value = assert_cast<const ColumnInt32&>(*three_column).get_element(0);
        const auto& src_column = assert_cast<const ColumnString&>(*first_column);
        const auto& pattern_column = assert_cast<const ColumnString&>(*second_column);

        auto nullable_string_type = make_nullable(std::make_shared<DataTypeString>());
        auto dest_column_ptr = ColumnArray::create(nullable_string_type->create_column(),
                                                   ColumnArray::ColumnOffsets::create());
        IColumn* dest_nested_column = &dest_column_ptr->get_data();
        auto& dest_offsets = dest_column_ptr->get_offsets();
        DCHECK(dest_nested_column != nullptr);

        NullMapType* dest_nested_null_map = nullptr;
        auto* dest_nullable_col = assert_cast<ColumnNullable*>(dest_nested_column);
        auto& dest_column_string =
                assert_cast<ColumnString&>(*(dest_nullable_col->get_nested_column_ptr()));
        dest_nested_null_map = &dest_nullable_col->get_null_map_column().get_data();
        RE2::Options opts;
        opts.set_never_nl(false);
        opts.set_dot_nl(true);
        // split_by_regexp(ColumnString, "xxx")
        if (right_const) {
            // The source is a constant too when the limit is a constant BE evaluates to a full
            // column, such as uniform(...).
            RETURN_IF_ERROR(_execute_constant_pattern(
                    src_column, left_const, pattern_column.get_data_at(0), dest_column_string,
                    dest_offsets, dest_nested_null_map, limit_value, input_rows_count, &opts));
        } else if (left_const) {
            // split_by_regexp("xxx", ColumnString)
            _execute_constant_src_string(src_column.get_data_at(0), pattern_column,
                                         dest_column_string, dest_offsets, dest_nested_null_map,
                                         limit_value, input_rows_count, &opts);
        } else {
            // split_by_regexp(ColumnString, ColumnString)
            _execute_vector_vector(src_column, pattern_column, dest_column_string, dest_offsets,
                                   dest_nested_null_map, limit_value, input_rows_count, &opts);
        }

        block.replace_by_position(result, std::move(dest_column_ptr));
        return Status::OK();
    }

private:
    static Status _execute_constant_pattern(const ColumnString& src_column_string, bool src_const,
                                            const StringRef& pattern_ref,
                                            ColumnString& dest_column_string,
                                            ColumnArray::Offsets64& dest_offsets,
                                            NullMapType* dest_nested_null_map, Int32 limit_value,
                                            size_t input_rows_count, RE2::Options* opts) {
        const char* token_begin = nullptr;
        const char* token_end = nullptr;
        UInt64 index = 0;
        std::unique_ptr<re2::RE2> re2_ptr = nullptr;
        if (pattern_ref.size) {
            re2_ptr = std::make_unique<re2::RE2>(pattern_ref.to_string_view(), *opts);
        }
        RegexpSplit RegexpSplit;
        RegexpSplit.init(re2_ptr.get(), limit_value);
        for (int row = 0; row < input_rows_count; ++row) {
            auto str_data = src_column_string.get_data_at(index_check_const(row, src_const));
            RegexpSplit.set(str_data.begin(), str_data.end());
            while (RegexpSplit.get(token_begin, token_end)) {
                size_t token_size = token_end - token_begin;
                dest_column_string.insert_data(token_begin, token_size);
                dest_nested_null_map->push_back(false);
                index += 1;
            }
            dest_offsets.push_back(index);
        }
        return Status::OK();
    }

    static void _execute_constant_src_string(const StringRef& str_ref,
                                             const ColumnString& pattern_column,
                                             ColumnString& dest_column_string,
                                             ColumnArray::Offsets64& dest_offsets,
                                             NullMapType* dest_nested_null_map, Int32 limit_value,
                                             size_t input_rows_count, RE2::Options* opts) {
        const char* token_begin = nullptr;
        const char* token_end = nullptr;
        UInt64 index = 0;
        RegexpSplit RegexpSplit;

        for (int row = 0; row < input_rows_count; ++row) {
            std::unique_ptr<re2::RE2> re2_ptr = nullptr;
            auto pattern = pattern_column.get_data_at(row);
            if (pattern.size) {
                re2_ptr = std::make_unique<re2::RE2>(pattern.to_string_view(), *opts);
                if (!re2_ptr->ok()) {
                    dest_column_string.insert_default();
                    dest_nested_null_map->push_back(true);
                    index += 1;
                    dest_offsets.push_back(index);
                    continue;
                }
            }

            RegexpSplit.init(re2_ptr.get(), limit_value);
            RegexpSplit.set(str_ref.begin(), str_ref.end());
            while (RegexpSplit.get(token_begin, token_end)) {
                size_t token_size = token_end - token_begin;
                dest_column_string.insert_data(token_begin, token_size);
                dest_nested_null_map->push_back(false);
                index += 1;
            }
            dest_offsets.push_back(index);
        }
    }

    static void _execute_vector_vector(const ColumnString& src_column_string,
                                       const ColumnString& pattern_column,
                                       ColumnString& dest_column_string,
                                       ColumnArray::Offsets64& dest_offsets,
                                       NullMapType* dest_nested_null_map, Int32 limit_value,
                                       size_t input_rows_count, RE2::Options* opts) {
        const char* token_begin = nullptr;
        const char* token_end = nullptr;
        UInt64 index = 0;
        RegexpSplit RegexpSplit;

        for (int row = 0; row < input_rows_count; ++row) {
            std::unique_ptr<re2::RE2> re2_ptr = nullptr;
            auto str_data = src_column_string.get_data_at(row);
            auto pattern = pattern_column.get_data_at(row);
            if (pattern.size) {
                re2_ptr = std::make_unique<re2::RE2>(pattern.to_string_view(), *opts);
                if (!re2_ptr->ok()) {
                    dest_column_string.insert_default();
                    dest_nested_null_map->push_back(true);
                    index += 1;
                    dest_offsets.push_back(index);
                    continue;
                }
            }
            RegexpSplit.init(re2_ptr.get(), limit_value);
            RegexpSplit.set(str_data.begin(), str_data.end());
            while (RegexpSplit.get(token_begin, token_end)) {
                size_t token_size = token_end - token_begin;
                dest_column_string.insert_data(token_begin, token_size);
                dest_nested_null_map->push_back(false);
                index += 1;
            }
            dest_offsets.push_back(index);
        }
    }
};

struct TwoArgumentImpl {
    static constexpr bool HAS_CONSTANT_LIMIT = false;

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()};
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        DCHECK_EQ(arguments.size(), 2);
        auto max_limit = ColumnConst::create(ColumnInt32::create(1, -1), input_rows_count);
        block.insert({std::move(max_limit), std::make_shared<DataTypeInt32>(), "max_limit"});
        ColumnNumbers temp_arguments = {arguments[0], arguments[1], block.columns() - 1};
        return ExecuteImpl::execute_impl(context, block, temp_arguments, result, input_rows_count);
    }
};

struct ThreeArgumentImpl {
    // use_default_implementation_for_nulls() is disabled for this variant so the limit can be
    // validated before the generic nullable-argument shortcut silently returns NULL for it.
    static constexpr bool HAS_CONSTANT_LIMIT = true;

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>(),
                std::make_shared<DataTypeInt32>()};
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        DCHECK_EQ(arguments.size(), 3);
        const ColumnWithTypeAndName& src_with_type = block.get_by_position(arguments[0]);
        const ColumnWithTypeAndName& pattern_with_type = block.get_by_position(arguments[1]);
        const ColumnWithTypeAndName& limit_with_type = block.get_by_position(arguments[2]);
        bool src_nullable = src_with_type.type->is_nullable();
        bool pattern_nullable = pattern_with_type.type->is_nullable();
        bool limit_nullable = limit_with_type.type->is_nullable();
        NullableColumnInfo src_info =
                src_nullable ? src_with_type.get_nullable_column_info() : NullableColumnInfo {};
        NullableColumnInfo pattern_info = pattern_nullable
                                                  ? pattern_with_type.get_nullable_column_info()
                                                  : NullableColumnInfo {};
        NullableColumnInfo limit_info =
                limit_nullable ? limit_with_type.get_nullable_column_info() : NullableColumnInfo {};

        // FE checks a literal limit, and a value only BE can evaluate is checked here, before the
        // generic nullable-argument shortcut below silently propagates a NULL limit as NULL.
        if (limit_nullable && limit_info.only_null) {
            return Status::InvalidArgument(
                    "the third parameter of split_by_regexp function must be a positive constant, "
                    "but got NULL");
        }
        ColumnWithTypeAndName unnested_limit =
                limit_nullable ? limit_with_type.unnest_nullable(limit_info, false)
                               : limit_with_type;
        const auto limit_value = unnested_limit.column->get_int(0);
        if (limit_value < 0) {
            return Status::InvalidArgument(
                    "the third parameter of split_by_regexp function must be a positive constant, "
                    "but got {}",
                    limit_value);
        }

        if ((src_nullable && src_info.only_null) || (pattern_nullable && pattern_info.only_null)) {
            block.get_by_position(result).column =
                    block.get_by_position(result).type->create_column_const(input_rows_count,
                                                                            Field());
            return Status::OK();
        }

        ColumnWithTypeAndName unnested_src =
                src_nullable ? src_with_type.unnest_nullable(src_info, false) : src_with_type;
        ColumnWithTypeAndName unnested_pattern =
                pattern_nullable ? pattern_with_type.unnest_nullable(pattern_info, false)
                                 : pattern_with_type;

        Block unnested_block;
        unnested_block.insert(unnested_src);
        unnested_block.insert(unnested_pattern);
        unnested_block.insert(unnested_limit);
        unnested_block.insert(
                {nullptr,
                 std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeString>())),
                 "result"});
        ColumnNumbers temp_arguments = {0, 1, 2};
        RETURN_IF_ERROR(ExecuteImpl::execute_impl(context, unnested_block, temp_arguments, 3,
                                                  input_rows_count));

        ColumnPtr dense_result = unnested_block.get_by_position(3).column;
        block.replace_by_position(
                result, (src_nullable || pattern_nullable || limit_nullable)
                                ? wrap_in_nullable(dense_result, block, arguments, input_rows_count)
                                : dense_result);
        return Status::OK();
    }
};

void register_function_split_by_regexp(SimpleFunctionFactory& factory) {
    factory.register_function<SplitByRegexp<TwoArgumentImpl>>();
    factory.register_function<SplitByRegexp<ThreeArgumentImpl>>();
}

} // namespace doris
