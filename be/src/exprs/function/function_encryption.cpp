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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "core/column/column_execute_util.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/string_ref.h"
#include "exec/common/stringop_substring.h"
#include "exec/common/util.hpp"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"
#include "util/encryption_util.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris {

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
        {"AES_256_OFB", EncryptionMode::AES_256_OFB},
        {"AES_128_GCM", EncryptionMode::AES_128_GCM},
        {"AES_192_GCM", EncryptionMode::AES_192_GCM},
        {"AES_256_GCM", EncryptionMode::AES_256_GCM}};
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

    bool use_default_implementation_for_nulls() const override {
        return Impl::use_default_implementation_for_nulls();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        return Impl::execute_impl_inner(context, block, arguments, result, input_rows_count);
    }
};

template <typename Impl, bool is_encrypt>
void execute_result(const char* src_raw, size_t src_size, const char* key_raw, size_t key_size,
                    size_t i, EncryptionMode& encryption_mode, const char* iv_raw, size_t iv_length,
                    ColumnString::Chars& result_data, ColumnString::Offsets& result_offset,
                    NullMap& null_map, const char* aad, size_t aad_length) {
    auto cipher_len = src_size;
    if constexpr (is_encrypt) {
        cipher_len += 16;
        // for output AEAD tag
        if (EncryptionUtil::is_gcm_mode(encryption_mode)) {
            cipher_len += EncryptionUtil::GCM_TAG_SIZE;
        }
    }
    std::unique_ptr<char[]> p;
    p.reset(new char[cipher_len]);
    int ret_code = 0;

    ret_code = Impl::execute_impl(encryption_mode, (unsigned char*)src_raw, src_size,
                                  (unsigned char*)key_raw, key_size, iv_raw, iv_length, true,
                                  (unsigned char*)p.get(), (unsigned char*)aad, aad_length);

    if (ret_code < 0) {
        StringOP::push_null_string(i, result_data, result_offset, null_map);
    } else {
        StringOP::push_value_string(std::string_view(p.get(), ret_code), i, result_data,
                                    result_offset);
    }
}

template <typename Impl, EncryptionMode mode, bool is_encrypt, bool is_sm_mode, int arg_num,
          int mode_index>
Status execute_with_column_views(Block& block, const ColumnNumbers& arguments, uint32_t result,
                                 size_t input_rows_count) {
    std::vector<ColumnView<TYPE_STRING>> argument_views;
    argument_views.reserve(arg_num);
    for (const auto argument : arguments) {
        argument_views.push_back(
                ColumnView<TYPE_STRING>::create(block.get_by_position(argument).column));
    }

    auto result_column = ColumnString::create();
    auto result_null_map_column = ColumnUInt8::create(input_rows_count, 0);
    auto& result_data = result_column->get_chars();
    auto& result_offset = result_column->get_offsets();
    auto& null_map = result_null_map_column->get_data();
    result_offset.resize(input_rows_count);

    for (size_t row = 0; row < input_rows_count; ++row) {
        bool is_null = false;
        for (const auto& argument_view : argument_views) {
            is_null |= argument_view.is_null_at(row);
        }
        if (is_null) {
            StringOP::push_null_string(row, result_data, result_offset, null_map);
            continue;
        }

        const auto mode_value = argument_views[mode_index].value_at(row);
        EncryptionMode encryption_mode = mode;
        if (!mode_value.empty()) {
            const std::string mode_str(mode_value.data, mode_value.size);
            if constexpr (is_sm_mode) {
                if (!sm4_mode_map.contains(mode_str)) {
                    StringOP::push_null_string(row, result_data, result_offset, null_map);
                    continue;
                }
                encryption_mode = sm4_mode_map.at(mode_str);
            } else {
                if (!aes_mode_map.contains(mode_str)) {
                    StringOP::push_null_string(row, result_data, result_offset, null_map);
                    continue;
                }
                encryption_mode = aes_mode_map.at(mode_str);
            }
        }

        if constexpr (arg_num == 5) {
            if (!EncryptionUtil::is_gcm_mode(encryption_mode)) {
                return Status::InvalidArgument("only GCM mode support AAD(the 5th arg)");
            }
        }

        const auto source = argument_views[0].value_at(row);
        const auto key = argument_views[1].value_at(row);
        const auto iv = [&]() {
            if constexpr (arg_num == 3) {
                return StringRef();
            } else {
                return argument_views[2].value_at(row);
            }
        }();
        const auto aad = [&]() {
            if constexpr (arg_num == 5) {
                return argument_views[4].value_at(row);
            } else {
                return StringRef();
            }
        }();
        execute_result<Impl, is_encrypt>(source.data, source.size, key.data, key.size, row,
                                         encryption_mode, iv.data, iv.size, result_data,
                                         result_offset, null_map, aad.data, aad.size);
    }

    block.get_by_position(result).column =
            ColumnNullable::create(std::move(result_column), std::move(result_null_map_column));
    return Status::OK();
}

template <typename Impl, EncryptionMode mode, bool is_encrypt>
struct EncryptionAndDecryptTwoImpl {
    static constexpr bool use_default_implementation_for_nulls() { return false; }

    static DataTypes get_variadic_argument_types_impl() {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>(),
                std::make_shared<DataTypeString>()};
    }

    static Status execute_impl_inner(FunctionContext* context, Block& block,
                                     const ColumnNumbers& arguments, uint32_t result,
                                     size_t input_rows_count) {
        DCHECK_EQ(3, arguments.size());
        return execute_with_column_views<Impl, mode, is_encrypt, false, 3, 2>(
                block, arguments, result, input_rows_count);
    }
};

template <typename Impl, EncryptionMode mode, bool is_encrypt, bool is_sm_mode, int arg_num = 4>
struct EncryptionAndDecryptMultiImpl {
    static constexpr bool use_default_implementation_for_nulls() { return false; }

    static DataTypes get_variadic_argument_types_impl() {
        if constexpr (arg_num == 5) {
            return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>(),
                    std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>(),
                    std::make_shared<DataTypeString>()};
        } else {
            return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>(),
                    std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()};
        }
    }

    static Status execute_impl_inner(FunctionContext* context, Block& block,
                                     const ColumnNumbers& arguments, uint32_t result,
                                     size_t input_rows_count) {
        DCHECK_EQ(arguments.size(), arg_num);
        if constexpr (arg_num == 4) {
            return execute_with_column_views<Impl, mode, is_encrypt, is_sm_mode, 4, 3>(
                    block, arguments, result, input_rows_count);
        } else {
            return execute_with_column_views<Impl, mode, is_encrypt, is_sm_mode, 5, 3>(
                    block, arguments, result, input_rows_count);
        }
    }
};

struct EncryptImpl {
    static int execute_impl(EncryptionMode mode, const unsigned char* source, size_t source_length,
                            const unsigned char* key, size_t key_length, const char* iv,
                            size_t iv_length, bool padding, unsigned char* encrypt,
                            const unsigned char* aad, size_t aad_length) {
        // now the openssl only support int, so here we need to cast size_t to uint32_t
        return EncryptionUtil::encrypt(mode, source, cast_set<uint32_t>(source_length), key,
                                       cast_set<uint32_t>(key_length), iv, cast_set<int>(iv_length),
                                       true, encrypt, aad, cast_set<uint32_t>(aad_length));
    }
};

struct DecryptImpl {
    static int execute_impl(EncryptionMode mode, const unsigned char* source, size_t source_length,
                            const unsigned char* key, size_t key_length, const char* iv,
                            size_t iv_length, bool padding, unsigned char* encrypt,
                            const unsigned char* aad, size_t aad_length) {
        return EncryptionUtil::decrypt(mode, source, cast_set<uint32_t>(source_length), key,
                                       cast_set<uint32_t>(key_length), iv, cast_set<int>(iv_length),
                                       true, encrypt, aad, cast_set<uint32_t>(aad_length));
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
            EncryptionAndDecryptMultiImpl<EncryptImpl, EncryptionMode::SM4_128_ECB, true, true>,
            SM4EncryptName>>();
    factory.register_function<FunctionEncryptionAndDecrypt<
            EncryptionAndDecryptMultiImpl<DecryptImpl, EncryptionMode::SM4_128_ECB, false, true>,
            SM4DecryptName>>();
    factory.register_function<FunctionEncryptionAndDecrypt<
            EncryptionAndDecryptMultiImpl<EncryptImpl, EncryptionMode::AES_128_ECB, true, false>,
            AESEncryptName>>();
    factory.register_function<FunctionEncryptionAndDecrypt<
            EncryptionAndDecryptMultiImpl<DecryptImpl, EncryptionMode::AES_128_ECB, false, false>,
            AESDecryptName>>();

    factory.register_function<FunctionEncryptionAndDecrypt<
            EncryptionAndDecryptMultiImpl<EncryptImpl, EncryptionMode::AES_128_GCM, true, false, 5>,
            AESEncryptName>>();
    factory.register_function<FunctionEncryptionAndDecrypt<
            EncryptionAndDecryptMultiImpl<DecryptImpl, EncryptionMode::AES_128_GCM, false, false,
                                          5>,
            AESDecryptName>>();
}

} // namespace doris
