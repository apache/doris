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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionHash.cpp
// and modified by Doris

#include "exprs/function/function_hash.h"

#include <vector>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_const.h"
#include "core/column/column_string.h"
#include "core/column/column_varbinary.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "core/value/large_int_value.h"
#include "exec/common/template_helpers.hpp"
#include "exprs/function/function_helpers.h"
#include "exprs/function/function_variadic_arguments.h"
#include "exprs/function/simple_function_factory.h"
#include "util/hash/murmur_hash3.h"
#include "util/hash_util.hpp"

namespace doris {
constexpr uint64_t emtpy_value = 0xe28dbde7fe22e41c;

namespace {

__int128_t pack_murmur_hash3_128(uint64_t h1, uint64_t h2) {
    static_assert(sizeof(__int128_t) == sizeof(uint64_t) * 2);
    // Store the two MurmurHash3 x64 128-bit lanes in a single LARGEINT value. Keep h1 in the
    // low 64 bits and h2 in the high 64 bits to match murmur_hash3_x64_128's out[0]/out[1].
    const auto value =
            (static_cast<unsigned __int128>(h2) << 64) | static_cast<unsigned __int128>(h1);
    return static_cast<__int128_t>(value);
}

void unpack_murmur_hash3_128(__int128_t value, uint64_t& h1, uint64_t& h2) {
    static_assert(sizeof(__int128_t) == sizeof(uint64_t) * 2);
    const auto unsigned_value = static_cast<unsigned __int128>(value);
    h1 = static_cast<uint64_t>(unsigned_value);
    h2 = static_cast<uint64_t>(unsigned_value >> 64);
}

void init_murmur_hash3_128(__int128_t& value, const void* data, size_t size) {
    uint64_t hash[2] = {0, 0};
    murmur_hash3_x64_128(data, size, 0, hash);
    value = pack_murmur_hash3_128(hash[0], hash[1]);
}

void update_murmur_hash3_128(__int128_t& value, const void* data, size_t size) {
    uint64_t h1 = 0;
    uint64_t h2 = 0;
    unpack_murmur_hash3_128(value, h1, h2);
    murmur_hash3_x64_process(data, size, h1, h2);
    value = pack_murmur_hash3_128(h1, h2);
}

template <bool first, typename StateContainer>
Status execute_murmur_hash3_128_column(const IColumn* column, size_t input_rows_count,
                                       StateContainer& state, const char* function_name) {
    if (const auto* col_from = check_and_get_column<ColumnString>(column)) {
        const typename ColumnString::Chars& data = col_from->get_chars();
        const typename ColumnString::Offsets& offsets = col_from->get_offsets();
        size_t size = offsets.size();
        ColumnString::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i) {
            if constexpr (first) {
                init_murmur_hash3_128(state[i],
                                      reinterpret_cast<const char*>(&data[current_offset]),
                                      offsets[i] - current_offset);
            } else {
                update_murmur_hash3_128(state[i],
                                        reinterpret_cast<const char*>(&data[current_offset]),
                                        offsets[i] - current_offset);
            }
            current_offset = offsets[i];
        }
    } else if (const ColumnConst* col_from_const =
                       check_and_get_column_const_string_or_fixedstring(column)) {
        auto value = col_from_const->get_value<TYPE_STRING>();
        for (size_t i = 0; i < input_rows_count; ++i) {
            if constexpr (first) {
                init_murmur_hash3_128(state[i], value.data(), value.size());
            } else {
                update_murmur_hash3_128(state[i], value.data(), value.size());
            }
        }
    } else {
        DCHECK(false);
        return Status::NotSupported("Illegal column {} of argument of function {}",
                                    column->get_name(), function_name);
    }
    return Status::OK();
}

} // namespace

template <PrimitiveType ReturnType, bool is_mmh64_v2 = false>
struct MurmurHash3Impl {
    static constexpr auto get_name() {
        if constexpr (ReturnType == TYPE_INT) {
            return "murmur_hash3_32";
        } else if constexpr (ReturnType == TYPE_LARGEINT) {
            return "murmur_hash3_u64_v2";
        } else if constexpr (is_mmh64_v2) {
            return "murmur_hash3_64_v2";
        } else {
            return "murmur_hash3_64";
        }
    }
    static constexpr auto name = get_name();

    static Status empty_apply(IColumn& icolumn, size_t input_rows_count) {
        ColumnVector<ReturnType>& vec_to = assert_cast<ColumnVector<ReturnType>&>(icolumn);
        vec_to.get_data().assign(
                input_rows_count,
                static_cast<typename PrimitiveTypeTraits<ReturnType>::CppType>(emtpy_value));
        return Status::OK();
    }

    static Status first_apply(const IDataType* type, const IColumn* column, size_t input_rows_count,
                              IColumn& icolumn) {
        return execute<true>(type, column, input_rows_count, icolumn);
    }

    static Status combine_apply(const IDataType* type, const IColumn* column,
                                size_t input_rows_count, IColumn& icolumn) {
        return execute<false>(type, column, input_rows_count, icolumn);
    }

    template <bool first>
    static Status execute(const IDataType* type, const IColumn* column, size_t input_rows_count,
                          IColumn& col_to) {
        auto& to_column = assert_cast<ColumnVector<ReturnType>&>(col_to);
        if constexpr (first) {
            if constexpr (ReturnType == TYPE_INT) {
                to_column.insert_many_vals(static_cast<Int32>(HashUtil::MURMUR3_32_SEED),
                                           input_rows_count);
            } else {
                to_column.insert_many_defaults(input_rows_count);
            }
        }
        auto& col_to_data = to_column.get_data();
        if (const auto* col_from = check_and_get_column<ColumnString>(column)) {
            const typename ColumnString::Chars& data = col_from->get_chars();
            const typename ColumnString::Offsets& offsets = col_from->get_offsets();
            size_t size = offsets.size();
            ColumnString::Offset current_offset = 0;
            for (size_t i = 0; i < size; ++i) {
                if constexpr (ReturnType == TYPE_INT) {
                    col_to_data[i] = HashUtil::murmur_hash3_32(
                            reinterpret_cast<const char*>(&data[current_offset]),
                            offsets[i] - current_offset, col_to_data[i]);
                } else {
                    col_to_data[i] = HashUtil::murmur_hash3_64<is_mmh64_v2>(
                            reinterpret_cast<const char*>(&data[current_offset]),
                            offsets[i] - current_offset, static_cast<uint64_t>(col_to_data[i]));
                }
                current_offset = offsets[i];
            }
        } else if (const ColumnConst* col_from_const =
                           check_and_get_column_const_string_or_fixedstring(column)) {
            auto value = col_from_const->get_value<TYPE_STRING>();
            for (size_t i = 0; i < input_rows_count; ++i) {
                if constexpr (ReturnType == TYPE_INT) {
                    col_to_data[i] =
                            HashUtil::murmur_hash3_32(value.data(), value.size(), col_to_data[i]);
                } else {
                    col_to_data[i] = HashUtil::murmur_hash3_64<is_mmh64_v2>(
                            value.data(), value.size(), static_cast<uint64_t>(col_to_data[i]));
                }
            }
        } else {
            DCHECK(false);
            return Status::NotSupported("Illegal column {} of argument of function {}",
                                        column->get_name(), name);
        }
        return Status::OK();
    }
};

using FunctionMurmurHash3_32 =
        FunctionVariadicArgumentsBase<DataTypeInt32, MurmurHash3Impl<TYPE_INT>>;
using FunctionMurmurHash3_64 =
        FunctionVariadicArgumentsBase<DataTypeInt64, MurmurHash3Impl<TYPE_BIGINT>>;
using FunctionMurmurHash3_64_V2 =
        FunctionVariadicArgumentsBase<DataTypeInt64, MurmurHash3Impl<TYPE_BIGINT, true>>;
using FunctionMurmurHash3U64V2 =
        FunctionVariadicArgumentsBase<DataTypeInt128, MurmurHash3Impl<TYPE_LARGEINT, true>>;

struct MurmurHash3128Impl {
    static constexpr auto name = "murmur_hash3_128";

    static Status empty_apply(IColumn& /*icolumn*/, size_t /*input_rows_count*/) {
        return Status::InvalidArgument("Function {} requires at least one argument", name);
    }

    static Status first_apply(const IDataType* type, const IColumn* column, size_t input_rows_count,
                              IColumn& icolumn) {
        return execute<true>(type, column, input_rows_count, icolumn);
    }

    static Status combine_apply(const IDataType* type, const IColumn* column,
                                size_t input_rows_count, IColumn& icolumn) {
        return execute<false>(type, column, input_rows_count, icolumn);
    }

    template <bool first>
    static Status execute(const IDataType* type, const IColumn* column, size_t input_rows_count,
                          IColumn& col_to) {
        auto& to_column = assert_cast<ColumnVector<TYPE_LARGEINT>&>(col_to);
        if constexpr (first) {
            // The first argument initializes one 128-bit hash state per row. Later arguments reuse
            // the same result column and update the saved state in place.
            to_column.insert_many_defaults(input_rows_count);
        }
        auto& col_to_data = to_column.get_data();
        return execute_murmur_hash3_128_column<first>(column, input_rows_count, col_to_data, name);
    }
};

using FunctionMurmurHash3_128 = FunctionVariadicArgumentsBase<DataTypeInt128, MurmurHash3128Impl>;

class FunctionMurmurHash3U128 : public IFunction {
public:
    static constexpr auto name = "murmur_hash3_u128";

    static FunctionPtr create() { return std::make_shared<FunctionMurmurHash3U128>(); }

    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }

    size_t get_number_of_arguments() const override { return 0; }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& /*arguments*/) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* /*context*/, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        if (arguments.empty()) {
            return Status::InvalidArgument("Function {} requires at least one argument", name);
        }

        std::vector<__int128_t> state(input_rows_count);
        const ColumnWithTypeAndName& first_col = block.get_by_position(arguments[0]);
        RETURN_IF_ERROR(execute_murmur_hash3_128_column<true>(first_col.column.get(),
                                                              input_rows_count, state, name));

        for (size_t i = 1; i < arguments.size(); ++i) {
            const ColumnWithTypeAndName& col = block.get_by_position(arguments[i]);
            RETURN_IF_ERROR(execute_murmur_hash3_128_column<false>(col.column.get(),
                                                                   input_rows_count, state, name));
        }

        auto result_column = ColumnString::create();
        result_column->reserve(input_rows_count);
        for (const auto value : state) {
            auto unsigned_value = static_cast<__uint128_t>(value);
            std::string value_str = LargeIntValue::to_string(unsigned_value);
            result_column->insert_data(value_str.data(), value_str.size());
        }
        block.get_by_position(result).column = std::move(result_column);
        return Status::OK();
    }
};

#ifdef BE_TEST
const char* murmur_hash3_get_name_type_int_for_test() {
    return MurmurHash3Impl<TYPE_INT>::get_name();
}

const char* murmur_hash3_get_name_type_bigint_for_test() {
    return MurmurHash3Impl<TYPE_BIGINT>::get_name();
}

const char* murmur_hash3_get_name_type_bigint_v2_for_test() {
    return MurmurHash3Impl<TYPE_BIGINT, true>::get_name();
}
#endif

template <PrimitiveType ReturnType>
struct XxHashImpl {
    static constexpr auto name = ReturnType == TYPE_INT ? "xxhash_32" : "xxhash_64";

    static Status empty_apply(IColumn& icolumn, size_t input_rows_count) {
        ColumnVector<ReturnType>& vec_to = assert_cast<ColumnVector<ReturnType>&>(icolumn);
        vec_to.get_data().assign(
                input_rows_count,
                static_cast<typename PrimitiveTypeTraits<ReturnType>::CppType>(emtpy_value));
        return Status::OK();
    }

    static Status first_apply(const IDataType* type, const IColumn* column, size_t input_rows_count,
                              IColumn& icolumn) {
        return execute<true>(type, column, input_rows_count, icolumn);
    }

    static Status combine_apply(const IDataType* type, const IColumn* column,
                                size_t input_rows_count, IColumn& icolumn) {
        return execute<false>(type, column, input_rows_count, icolumn);
    }

    template <bool first>
    static Status execute(const IDataType* type, const IColumn* column, size_t input_rows_count,
                          IColumn& col_to) {
        auto& to_column = assert_cast<ColumnVector<ReturnType>&>(col_to);
        if constexpr (first) {
            to_column.insert_many_defaults(input_rows_count);
        }
        auto& col_to_data = to_column.get_data();
        if (const auto* col_from = check_and_get_column<ColumnString>(column)) {
            const typename ColumnString::Chars& data = col_from->get_chars();
            const typename ColumnString::Offsets& offsets = col_from->get_offsets();
            size_t size = offsets.size();
            ColumnString::Offset current_offset = 0;
            for (size_t i = 0; i < size; ++i) {
                if constexpr (ReturnType == TYPE_INT) {
                    col_to_data[i] = HashUtil::xxHash32WithSeed(
                            reinterpret_cast<const char*>(&data[current_offset]),
                            offsets[i] - current_offset, col_to_data[i]);
                } else {
                    col_to_data[i] = HashUtil::xxHash64WithSeed(
                            reinterpret_cast<const char*>(&data[current_offset]),
                            offsets[i] - current_offset, col_to_data[i]);
                }
                current_offset = offsets[i];
            }
        } else if (const ColumnConst* col_from_const =
                           check_and_get_column_const_string_or_fixedstring(column)) {
            auto value = col_from_const->get_value<TYPE_STRING>();
            for (size_t i = 0; i < input_rows_count; ++i) {
                if constexpr (ReturnType == TYPE_INT) {
                    col_to_data[i] =
                            HashUtil::xxHash32WithSeed(value.data(), value.size(), col_to_data[i]);
                } else {
                    col_to_data[i] =
                            HashUtil::xxHash64WithSeed(value.data(), value.size(), col_to_data[i]);
                }
            }
        } else if (const auto* vb_col = check_and_get_column<ColumnVarbinary>(column)) {
            for (size_t i = 0; i < input_rows_count; ++i) {
                auto data_ref = vb_col->get_data_at(i);
                if constexpr (ReturnType == TYPE_INT) {
                    col_to_data[i] = HashUtil::xxHash32WithSeed(data_ref.data, data_ref.size,
                                                                col_to_data[i]);
                } else {
                    col_to_data[i] = HashUtil::xxHash64WithSeed(data_ref.data, data_ref.size,
                                                                col_to_data[i]);
                }
            }
        } else {
            DCHECK(false);
            return Status::NotSupported("Illegal column {} of argument of function {}",
                                        column->get_name(), name);
        }
        return Status::OK();
    }
};

using FunctionXxHash_32 = FunctionVariadicArgumentsBase<DataTypeInt32, XxHashImpl<TYPE_INT>>;
using FunctionXxHash_64 = FunctionVariadicArgumentsBase<DataTypeInt64, XxHashImpl<TYPE_BIGINT>>;

void register_function_hash(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionMurmurHash3_32>();
    factory.register_function<FunctionMurmurHash3_64>();
    factory.register_function<FunctionMurmurHash3_64_V2>();
    factory.register_function<FunctionMurmurHash3U64V2>();
    factory.register_function<FunctionMurmurHash3_128>();
    factory.register_function<FunctionMurmurHash3U128>();
    factory.register_function<FunctionXxHash_32>();
    factory.register_function<FunctionXxHash_64>();
    factory.register_alias("xxhash_64", "xxhash3_64");
}
} // namespace doris
