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

#include "exprs/encryption_functions.h"
#include "runtime/string_search.hpp"
#include "util/encryption_util.h"
#include "util/string_util.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/functions/function_string.h"
#include "vec/functions/function_string_to_string.h"
#include "vec/functions/function_totype.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

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

    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
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

        Impl::vector_vector(offsets_list, chars_list, input_rows_count, result_data, result_offset,
                            result_null_map->get_data());
        block.get_by_position(result).column =
                ColumnNullable::create(std::move(result_data_column), std::move(result_null_map));
        return Status::OK();
    }
};

template <typename Impl, bool is_encrypt>
static void exectue_result(std::vector<const ColumnString::Offsets*>& offsets_list,
                           std::vector<const ColumnString::Chars*>& chars_list, size_t i,
                           EncryptionMode& encryption_mode, const char* iv_raw,
                           ColumnString::Chars& result_data, ColumnString::Offsets& result_offset,
                           NullMap& null_map) {
    int src_size = (*offsets_list[0])[i] - (*offsets_list[0])[i - 1] - 1;
    const auto src_raw =
            reinterpret_cast<const char*>(&(*chars_list[0])[(*offsets_list[0])[i - 1]]);
    int key_size = (*offsets_list[1])[i] - (*offsets_list[1])[i - 1] - 1;
    const auto key_raw =
            reinterpret_cast<const char*>(&(*chars_list[1])[(*offsets_list[1])[i - 1]]);
    if (*src_raw == '\0' && src_size == 0) {
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
                                  (unsigned char*)key_raw, key_size, iv_raw, true,
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
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()};
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
            exectue_result<Impl, is_encrypt>(offsets_list, chars_list, i, encryption_mode, nullptr,
                                             result_data, result_offset, null_map);
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
            int mode_size = (*offsets_list[3])[i] - (*offsets_list[3])[i - 1] - 1;
            const auto mode_raw =
                    reinterpret_cast<const char*>(&(*chars_list[3])[(*offsets_list[3])[i - 1]]);
            const auto iv_raw =
                    reinterpret_cast<const char*>(&(*chars_list[2])[(*offsets_list[2])[i - 1]]);
            if (*mode_raw != '\0' || mode_size != 0) {
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
                                             result_data, result_offset, null_map);
        }
        return Status::OK();
    }
};

struct EncryptImpl {
    static int exectue_impl(EncryptionMode mode, const unsigned char* source,
                            uint32_t source_length, const unsigned char* key, uint32_t key_length,
                            const char* iv, bool padding, unsigned char* encrypt) {
        return EncryptionUtil::encrypt(mode, source, source_length, key, key_length, iv, true,
                                       encrypt);
    }
};

struct DecryptImpl {
    static int exectue_impl(EncryptionMode mode, const unsigned char* source,
                            uint32_t source_length, const unsigned char* key, uint32_t key_length,
                            const char* iv, bool padding, unsigned char* encrypt) {
        return EncryptionUtil::decrypt(mode, source, source_length, key, key_length, iv, true,
                                       encrypt);
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
    factory.register_function<FunctionEncryptionAndDecrypt<EncryptionAndDecryptTwoImpl<EncryptImpl, SM4_128_ECB, true>, SM4EncryptName>>();
    factory.register_function<FunctionEncryptionAndDecrypt<EncryptionAndDecryptTwoImpl<DecryptImpl, SM4_128_ECB, false>, SM4DecryptName>>();
    factory.register_function<FunctionEncryptionAndDecrypt<EncryptionAndDecryptTwoImpl<EncryptImpl, AES_128_ECB, true>, AESEncryptName>>();
    factory.register_function<FunctionEncryptionAndDecrypt<EncryptionAndDecryptTwoImpl<DecryptImpl, AES_128_ECB, false>, AESDecryptName>>();

    factory.register_function<FunctionEncryptionAndDecrypt<EncryptionAndDecryptFourImpl<EncryptImpl, SM4_128_ECB, true, true>, SM4EncryptName>>();
    factory.register_function<FunctionEncryptionAndDecrypt<EncryptionAndDecryptFourImpl<DecryptImpl, SM4_128_ECB, false, true>, SM4DecryptName>>();
    factory.register_function<FunctionEncryptionAndDecrypt<EncryptionAndDecryptFourImpl<EncryptImpl, AES_128_ECB, true, false>, AESEncryptName>>();
    factory.register_function<FunctionEncryptionAndDecrypt<EncryptionAndDecryptFourImpl<DecryptImpl, AES_128_ECB, false, false>, AESDecryptName>>();
}

} // namespace doris::vectorized
