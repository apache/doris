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

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"
#include "util/encryption_util.h"
#include "util/string_util.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/pod_array.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"
#include "vec/functions/function_string.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/util.hpp"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

inline StringCaseUnorderedMap<EncryptionMode> aes_mode_map {
        {"AES_128_ECB", EncryptionMode::AES_128_ECB},
        {"AES_192_ECB", EncryptionMode::AES_192_ECB},
        {"AES_256_ECB", EncryptionMode::AES_256_ECB},
        {"AES_128_CBC", EncryptionMode::AES_128_CBC},
        {"AES_192_CBC", EncryptionMode::AES_192_CBC},
        {"AES_256_CBC", EncryptionMode::AES_256_CBC},
        {"AES_128_CFB", EncryptionMode::AES_128_CFB},
        {"AES_192_CFB", EncryptionMode::AES_192_CFB},
        {"AES_256_CFB", EncryptionMode::AES_256_CFB},
        {"AES_128_CFB1", EncryptionMode::AES_128_CFB1},
        {"AES_192_CFB1", EncryptionMode::AES_192_CFB1},
        {"AES_256_CFB1", EncryptionMode::AES_256_CFB1},
        {"AES_128_CFB8", EncryptionMode::AES_128_CFB8},
        {"AES_192_CFB8", EncryptionMode::AES_192_CFB8},
        {"AES_256_CFB8", EncryptionMode::AES_256_CFB8},
        {"AES_128_CFB128", EncryptionMode::AES_128_CFB128},
        {"AES_192_CFB128", EncryptionMode::AES_192_CFB128},
        {"AES_256_CFB128", EncryptionMode::AES_256_CFB128},
        {"AES_128_CTR", EncryptionMode::AES_128_CTR},
        {"AES_192_CTR", EncryptionMode::AES_192_CTR},
        {"AES_256_CTR", EncryptionMode::AES_256_CTR},
        {"AES_128_OFB", EncryptionMode::AES_128_OFB},
        {"AES_192_OFB", EncryptionMode::AES_192_OFB},
        {"AES_256_OFB", EncryptionMode::AES_256_OFB}};
inline StringCaseUnorderedMap<EncryptionMode> sm4_mode_map {
        {"SM4_128_ECB", EncryptionMode::SM4_128_ECB},
        {"SM4_128_CBC", EncryptionMode::SM4_128_CBC},
        {"SM4_128_CFB128", EncryptionMode::SM4_128_CFB128},
        {"SM4_128_OFB", EncryptionMode::SM4_128_OFB},
        {"SM4_128_CTR", EncryptionMode::SM4_128_CTR}};
template <typename Impl, typename FunctionName>
class FunctionEncryptionAndDecrypt : public IFunction {
public:
    static constexpr auto name = FunctionName::name;

    String get_name() const override { return name; }

    static FunctionPtr create() { return std::make_shared<FunctionEncryptionAndDecrypt>(); }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    DataTypes get_variadic_argument_types_impl() const override {
        return Impl::get_variadic_argument_types_impl();
    }

    size_t get_number_of_arguments() const override {
        return get_variadic_argument_types_impl().size();
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        size_t argument_size = arguments.size();
        ColumnPtr argument_columns[argument_size];
        std::vector<const ColumnString::Offsets*> offsets_list(argument_size);
        std::vector<const ColumnString::Chars*> chars_list(argument_size);

        auto result_null_map = ColumnUInt8::create(input_rows_count, 0);
        auto result_data_column = ColumnString::create();

        auto& result_data = result_data_column->get_chars();
        auto& result_offset = result_data_column->get_offsets();
        result_offset.resize(input_rows_count);

        for (int i = 0; i < argument_size; ++i) {
            argument_columns[i] =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
            if (auto* nullable = check_and_get_column<ColumnNullable>(*argument_columns[i])) {
                VectorizedUtils::update_null_map(result_null_map->get_data(),
                                                 nullable->get_null_map_data());
                argument_columns[i] = nullable->get_nested_column_ptr();
            }
        }

        for (size_t i = 0; i < argument_size; ++i) {
            auto col_str = assert_cast<const ColumnString*>(argument_columns[i].get());
            offsets_list[i] = &col_str->get_offsets();
            chars_list[i] = &col_str->get_chars();
        }

        static_cast<void>(Impl::vector_vector(offsets_list, chars_list, input_rows_count,
                                              result_data, result_offset,
                                              result_null_map->get_data()));
        block.get_by_position(result).column =
                ColumnNullable::create(std::move(result_data_column), std::move(result_null_map));
        return Status::OK();
    }
};

template <typename Impl, bool is_encrypt>
void exectue_result(std::vector<const ColumnString::Offsets*>& offsets_list,
                    std::vector<const ColumnString::Chars*>& chars_list, size_t i,
                    EncryptionMode& encryption_mode, const char* iv_raw, int iv_length,
                    ColumnString::Chars& result_data, ColumnString::Offsets& result_offset,
                    NullMap& null_map) {
    int src_size = (*offsets_list[0])[i] - (*offsets_list[0])[i - 1];
    const auto src_raw =
            reinterpret_cast<const char*>(&(*chars_list[0])[(*offsets_list[0])[i - 1]]);
    int key_size = (*offsets_list[1])[i] - (*offsets_list[1])[i - 1];
    const auto key_raw =
            reinterpret_cast<const char*>(&(*chars_list[1])[(*offsets_list[1])[i - 1]]);
    if (src_size == 0) {
        StringOP::push_null_string(i, result_data, result_offset, null_map);
        return;
    }
    int cipher_len = src_size;
    if constexpr (is_encrypt) {
        cipher_len += 16;
    }
    std::unique_ptr<char[]> p;
    p.reset(new char[cipher_len]);
    int ret_code = 0;

    ret_code = Impl::exectue_impl(encryption_mode, (unsigned char*)src_raw, src_size,
                                  (unsigned char*)key_raw, key_size, iv_raw, iv_length, true,
                                  (unsigned char*)p.get());

    if (ret_code < 0) {
        StringOP::push_null_string(i, result_data, result_offset, null_map);
    } else {
        StringOP::push_value_string(std::string_view(p.get(), ret_code), i, result_data,
                                    result_offset);
    }
}

template <typename Impl, EncryptionMode mode, bool is_encrypt>
struct EncryptionAndDecryptTwoImpl {
    static DataTypes get_variadic_argument_types_impl() {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>(),
                std::make_shared<DataTypeString>()};
    }

    static Status vector_vector(std::vector<const ColumnString::Offsets*>& offsets_list,
                                std::vector<const ColumnString::Chars*>& chars_list,
                                size_t input_rows_count, ColumnString::Chars& result_data,
                                ColumnString::Offsets& result_offset, NullMap& null_map) {
        for (int i = 0; i < input_rows_count; ++i) {
            if (null_map[i]) {
                StringOP::push_null_string(i, result_data, result_offset, null_map);
                continue;
            }
            EncryptionMode encryption_mode = mode;
            int mode_size = (*offsets_list[2])[i] - (*offsets_list[2])[i - 1];
            const auto mode_raw =
                    reinterpret_cast<const char*>(&(*chars_list[2])[(*offsets_list[2])[i - 1]]);
            if (mode_size != 0) {
                std::string mode_str(mode_raw, mode_size);
                if (aes_mode_map.count(mode_str) == 0) {
                    StringOP::push_null_string(i, result_data, result_offset, null_map);
                    continue;
                }
                encryption_mode = aes_mode_map.at(mode_str);
            }
            exectue_result<Impl, is_encrypt>(offsets_list, chars_list, i, encryption_mode, nullptr,
                                             0, result_data, result_offset, null_map);
        }
        return Status::OK();
    }
};

template <typename Impl, EncryptionMode mode, bool is_encrypt, bool is_sm_mode>
struct EncryptionAndDecryptFourImpl {
    static DataTypes get_variadic_argument_types_impl() {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>(),
                std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()};
    }

    static Status vector_vector(std::vector<const ColumnString::Offsets*>& offsets_list,
                                std::vector<const ColumnString::Chars*>& chars_list,
                                size_t input_rows_count, ColumnString::Chars& result_data,
                                ColumnString::Offsets& result_offset, NullMap& null_map) {
        for (int i = 0; i < input_rows_count; ++i) {
            if (null_map[i]) {
                StringOP::push_null_string(i, result_data, result_offset, null_map);
                continue;
            }

            EncryptionMode encryption_mode = mode;
            int mode_size = (*offsets_list[3])[i] - (*offsets_list[3])[i - 1];
            int iv_size = (*offsets_list[2])[i] - (*offsets_list[2])[i - 1];
            const auto mode_raw =
                    reinterpret_cast<const char*>(&(*chars_list[3])[(*offsets_list[3])[i - 1]]);
            const auto iv_raw =
                    reinterpret_cast<const char*>(&(*chars_list[2])[(*offsets_list[2])[i - 1]]);
            if (mode_size != 0) {
                std::string mode_str(mode_raw, mode_size);
                if constexpr (is_sm_mode) {
                    if (sm4_mode_map.count(mode_str) == 0) {
                        StringOP::push_null_string(i, result_data, result_offset, null_map);
                        continue;
                    }
                    encryption_mode = sm4_mode_map.at(mode_str);
                } else {
                    if (aes_mode_map.count(mode_str) == 0) {
                        StringOP::push_null_string(i, result_data, result_offset, null_map);
                        continue;
                    }
                    encryption_mode = aes_mode_map.at(mode_str);
                }
            }

            exectue_result<Impl, is_encrypt>(offsets_list, chars_list, i, encryption_mode, iv_raw,
                                             iv_size, result_data, result_offset, null_map);
        }
        return Status::OK();
    }
};

struct EncryptImpl {
    static int exectue_impl(EncryptionMode mode, const unsigned char* source,
                            uint32_t source_length, const unsigned char* key, uint32_t key_length,
                            const char* iv, int iv_length, bool padding, unsigned char* encrypt) {
        return EncryptionUtil::encrypt(mode, source, source_length, key, key_length, iv, iv_length,
                                       true, encrypt);
    }
};

struct DecryptImpl {
    static int exectue_impl(EncryptionMode mode, const unsigned char* source,
                            uint32_t source_length, const unsigned char* key, uint32_t key_length,
                            const char* iv, int iv_length, bool padding, unsigned char* encrypt) {
        return EncryptionUtil::decrypt(mode, source, source_length, key, key_length, iv, iv_length,
                                       true, encrypt);
    }
};

struct SM4EncryptName {
    static constexpr auto name = "sm4_encrypt";
};

struct SM4DecryptName {
    static constexpr auto name = "sm4_decrypt";
};

struct AESEncryptName {
    static constexpr auto name = "aes_encrypt";
};

struct AESDecryptName {
    static constexpr auto name = "aes_decrypt";
};

void register_function_encryption(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionEncryptionAndDecrypt<
            EncryptionAndDecryptTwoImpl<EncryptImpl, EncryptionMode::SM4_128_ECB, true>,
            SM4EncryptName>>();
    factory.register_function<FunctionEncryptionAndDecrypt<
            EncryptionAndDecryptTwoImpl<DecryptImpl, EncryptionMode::SM4_128_ECB, false>,
            SM4DecryptName>>();
    factory.register_function<FunctionEncryptionAndDecrypt<
            EncryptionAndDecryptTwoImpl<EncryptImpl, EncryptionMode::AES_128_ECB, true>,
            AESEncryptName>>();
    factory.register_function<FunctionEncryptionAndDecrypt<
            EncryptionAndDecryptTwoImpl<DecryptImpl, EncryptionMode::AES_128_ECB, false>,
            AESDecryptName>>();

    factory.register_function<FunctionEncryptionAndDecrypt<
            EncryptionAndDecryptFourImpl<EncryptImpl, EncryptionMode::SM4_128_ECB, true, true>,
            SM4EncryptName>>();
    factory.register_function<FunctionEncryptionAndDecrypt<
            EncryptionAndDecryptFourImpl<DecryptImpl, EncryptionMode::SM4_128_ECB, false, true>,
            SM4DecryptName>>();
    factory.register_function<FunctionEncryptionAndDecrypt<
            EncryptionAndDecryptFourImpl<EncryptImpl, EncryptionMode::AES_128_ECB, true, false>,
            AESEncryptName>>();
    factory.register_function<FunctionEncryptionAndDecrypt<
            EncryptionAndDecryptFourImpl<DecryptImpl, EncryptionMode::AES_128_ECB, false, false>,
            AESDecryptName>>();
}

} // namespace doris::vectorized
