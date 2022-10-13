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

#include <gtest/gtest.h>

#include <iostream>
#include <string>

#include "exprs/anyval_util.h"
#include "testutil/function_utils.h"
#include "util/logging.h"

namespace doris {
class EncryptionFunctionsTest : public testing::Test {
public:
    EncryptionFunctionsTest() = default;

    void SetUp() {
        utils = new FunctionUtils();
        ctx = utils->get_fn_ctx();
    }
    void TearDown() { delete utils; }

private:
    FunctionUtils* utils;
    FunctionContext* ctx;
};

TEST_F(EncryptionFunctionsTest, from_base64) {
    std::unique_ptr<doris_udf::FunctionContext> context(new doris_udf::FunctionContext());
    {
        StringVal result =
                EncryptionFunctions::from_base64(context.get(), doris_udf::StringVal("aGVsbG8="));
        StringVal expected = doris_udf::StringVal("hello");
        EXPECT_EQ(expected, result);
    }

    {
        StringVal result =
                EncryptionFunctions::from_base64(context.get(), doris_udf::StringVal::null());
        StringVal expected = doris_udf::StringVal::null();
        EXPECT_EQ(expected, result);
    }
}

TEST_F(EncryptionFunctionsTest, to_base64) {
    std::unique_ptr<doris_udf::FunctionContext> context(new doris_udf::FunctionContext());

    {
        StringVal result =
                EncryptionFunctions::to_base64(context.get(), doris_udf::StringVal("hello"));
        StringVal expected = doris_udf::StringVal("aGVsbG8=");
        EXPECT_EQ(expected, result);
    }
    {
        StringVal result =
                EncryptionFunctions::to_base64(context.get(), doris_udf::StringVal::null());
        StringVal expected = doris_udf::StringVal::null();
        EXPECT_EQ(expected, result);
    }
}

TEST_F(EncryptionFunctionsTest, aes_decrypt) {
    std::unique_ptr<doris_udf::FunctionContext> context(new doris_udf::FunctionContext());
    {
        StringVal encryptWord = EncryptionFunctions::aes_encrypt(
                context.get(), doris_udf::StringVal("hello"), doris_udf::StringVal("key"));
        StringVal result = EncryptionFunctions::aes_decrypt(context.get(), encryptWord,
                                                            doris_udf::StringVal("key"));
        StringVal expected = doris_udf::StringVal("hello");
        EXPECT_EQ(expected, result);
    }
    {
        StringVal encryptWord = EncryptionFunctions::aes_encrypt(
                context.get(), doris_udf::StringVal("hello"), doris_udf::StringVal("key"));
        encryptWord.is_null = true;
        StringVal result = EncryptionFunctions::aes_decrypt(context.get(), encryptWord,
                                                            doris_udf::StringVal("key"));
        StringVal expected = doris_udf::StringVal::null();
        EXPECT_EQ(expected, result);
    }

    {
        StringVal encryptWord = EncryptionFunctions::aes_encrypt(
                context.get(), doris_udf::StringVal("hello"), doris_udf::StringVal("key"),
                doris_udf::StringVal::null(), doris_udf::StringVal("AES_128_ECB"));
        encryptWord.is_null = true;
        StringVal result = EncryptionFunctions::aes_decrypt(
                context.get(), encryptWord, doris_udf::StringVal("key"),
                doris_udf::StringVal::null(), doris_udf::StringVal("AES_128_ECB"));
        StringVal expected = doris_udf::StringVal::null();
        EXPECT_EQ(expected, result);
    }

    {
        StringVal encryptWord = EncryptionFunctions::aes_encrypt(
                context.get(), doris_udf::StringVal("hello"), doris_udf::StringVal("key"),
                doris_udf::StringVal::null(), doris_udf::StringVal("AES_128_ECB"));
        StringVal result = EncryptionFunctions::aes_decrypt(
                context.get(), encryptWord, doris_udf::StringVal("key"),
                doris_udf::StringVal::null(), doris_udf::StringVal("AES_128_ECB"));
        StringVal expected = doris_udf::StringVal("hello");
        EXPECT_EQ(expected, result);
    }

    {
        StringVal encryptWord = EncryptionFunctions::aes_encrypt(
                context.get(), doris_udf::StringVal("hello"), doris_udf::StringVal("key"),
                doris_udf::StringVal("01234567890"), doris_udf::StringVal("AES_256_CBC"));
        StringVal result = EncryptionFunctions::aes_decrypt(
                context.get(), encryptWord, doris_udf::StringVal("key"),
                doris_udf::StringVal("01234567890"), doris_udf::StringVal("AES_256_CBC"));
        StringVal expected = doris_udf::StringVal("hello");
        EXPECT_EQ(expected, result);
    }
    {
        StringVal encryptWord = EncryptionFunctions::aes_encrypt(
                context.get(), doris_udf::StringVal("hello"), doris_udf::StringVal("key"),
                doris_udf::StringVal("01234567890"), doris_udf::StringVal("AES_192_CFB"));
        StringVal result = EncryptionFunctions::aes_decrypt(
                context.get(), encryptWord, doris_udf::StringVal("key"),
                doris_udf::StringVal("01234567890"), doris_udf::StringVal("AES_192_CFB"));
        StringVal expected = doris_udf::StringVal("hello");
        EXPECT_EQ(expected, result);
    }
    {
        StringVal encryptWord = EncryptionFunctions::aes_encrypt(
                context.get(), doris_udf::StringVal("hello"), doris_udf::StringVal("key"),
                doris_udf::StringVal("01234567890"), doris_udf::StringVal("AES_192_CFB1"));
        StringVal result = EncryptionFunctions::aes_decrypt(
                context.get(), encryptWord, doris_udf::StringVal("key"),
                doris_udf::StringVal("01234567890"), doris_udf::StringVal("AES_192_CFB1"));
        StringVal expected = doris_udf::StringVal("hello");
        EXPECT_EQ(expected, result);
    }
    {
        StringVal encryptWord = EncryptionFunctions::aes_encrypt(
                context.get(), doris_udf::StringVal("hello"), doris_udf::StringVal("key"),
                doris_udf::StringVal("01234567890"), doris_udf::StringVal("AES_192_CFB8"));
        StringVal result = EncryptionFunctions::aes_decrypt(
                context.get(), encryptWord, doris_udf::StringVal("key"),
                doris_udf::StringVal("01234567890"), doris_udf::StringVal("AES_192_CFB8"));
        StringVal expected = doris_udf::StringVal("hello");
        EXPECT_EQ(expected, result);
    }
    {
        StringVal encryptWord = EncryptionFunctions::aes_encrypt(
                context.get(), doris_udf::StringVal("hello"), doris_udf::StringVal("key"),
                doris_udf::StringVal("01234567890"), doris_udf::StringVal("AES_192_CFB128"));
        StringVal result = EncryptionFunctions::aes_decrypt(
                context.get(), encryptWord, doris_udf::StringVal("key"),
                doris_udf::StringVal("01234567890"), doris_udf::StringVal("AES_192_CFB128"));
        StringVal expected = doris_udf::StringVal("hello");
        EXPECT_EQ(expected, result);
    }
    {
        StringVal encryptWord = EncryptionFunctions::aes_encrypt(
                context.get(), doris_udf::StringVal("hello"), doris_udf::StringVal("key"),
                doris_udf::StringVal("01234567890"), doris_udf::StringVal("AES_192_CTR"));
        StringVal result = EncryptionFunctions::aes_decrypt(
                context.get(), encryptWord, doris_udf::StringVal("key"),
                doris_udf::StringVal("01234567890"), doris_udf::StringVal("AES_192_CTR"));
        StringVal expected = doris_udf::StringVal("hello");
        EXPECT_EQ(expected, result);
    }
    {
        StringVal encryptWord = EncryptionFunctions::aes_encrypt(
                context.get(), doris_udf::StringVal("hello"), doris_udf::StringVal("key"),
                doris_udf::StringVal("01234567890"), doris_udf::StringVal("AES_192_OFB"));
        StringVal result = EncryptionFunctions::aes_decrypt(
                context.get(), encryptWord, doris_udf::StringVal("key"),
                doris_udf::StringVal("01234567890"), doris_udf::StringVal("AES_192_OFB"));
        StringVal expected = doris_udf::StringVal("hello");
        EXPECT_EQ(expected, result);
    }
}

TEST_F(EncryptionFunctionsTest, sm4_decrypt) {
    std::unique_ptr<doris_udf::FunctionContext> context(new doris_udf::FunctionContext());
    {
        StringVal encryptWord = EncryptionFunctions::sm4_encrypt(
                context.get(), doris_udf::StringVal("hello"), doris_udf::StringVal("key"));
        StringVal result = EncryptionFunctions::sm4_decrypt(context.get(), encryptWord,
                                                            doris_udf::StringVal("key"));
        StringVal expected = doris_udf::StringVal("hello");
        EXPECT_EQ(expected, result);
    }
    {
        StringVal encryptWord = EncryptionFunctions::sm4_encrypt(
                context.get(), doris_udf::StringVal("hello"), doris_udf::StringVal("key"));
        encryptWord.is_null = true;
        StringVal result = EncryptionFunctions::sm4_decrypt(context.get(), encryptWord,
                                                            doris_udf::StringVal("key"));
        StringVal expected = doris_udf::StringVal::null();
        EXPECT_EQ(expected, result);
    }

    {
        StringVal encryptWord = EncryptionFunctions::sm4_encrypt(
                context.get(), doris_udf::StringVal("hello"), doris_udf::StringVal("key"),
                doris_udf::StringVal::null(), doris_udf::StringVal("SM4_128_ECB"));
        encryptWord.is_null = true;
        StringVal result = EncryptionFunctions::sm4_decrypt(
                context.get(), encryptWord, doris_udf::StringVal("key"),
                doris_udf::StringVal::null(), doris_udf::StringVal("SM4_128_ECB"));
        StringVal expected = doris_udf::StringVal::null();
        EXPECT_EQ(expected, result);
    }

    {
        StringVal encryptWord = EncryptionFunctions::sm4_encrypt(
                context.get(), doris_udf::StringVal("hello"), doris_udf::StringVal("key"),
                doris_udf::StringVal("01234567890"), doris_udf::StringVal("SM4_128_CBC"));
        StringVal result = EncryptionFunctions::sm4_decrypt(
                context.get(), encryptWord, doris_udf::StringVal("key"),
                doris_udf::StringVal("01234567890"), doris_udf::StringVal("SM4_128_CBC"));
        StringVal expected = doris_udf::StringVal("hello");
        EXPECT_EQ(expected, result);
    }
    {
        StringVal encryptWord = EncryptionFunctions::sm4_encrypt(
                context.get(), doris_udf::StringVal("hello"), doris_udf::StringVal("key"),
                doris_udf::StringVal("01234567890"), doris_udf::StringVal("SM4_128_CFB128"));
        StringVal result = EncryptionFunctions::sm4_decrypt(
                context.get(), encryptWord, doris_udf::StringVal("key"),
                doris_udf::StringVal("01234567890"), doris_udf::StringVal("SM4_128_CFB128"));
        StringVal expected = doris_udf::StringVal("hello");
        EXPECT_EQ(expected, result);
    }
    {
        StringVal encryptWord = EncryptionFunctions::sm4_encrypt(
                context.get(), doris_udf::StringVal("hello"), doris_udf::StringVal("key"),
                doris_udf::StringVal("01234567890"), doris_udf::StringVal("SM4_128_CTR"));
        StringVal result = EncryptionFunctions::sm4_decrypt(
                context.get(), encryptWord, doris_udf::StringVal("key"),
                doris_udf::StringVal("01234567890"), doris_udf::StringVal("SM4_128_CTR"));
        StringVal expected = doris_udf::StringVal("hello");
        EXPECT_EQ(expected, result);
    }
    {
        StringVal encryptWord = EncryptionFunctions::sm4_encrypt(
                context.get(), doris_udf::StringVal("hello"), doris_udf::StringVal("key"),
                doris_udf::StringVal("01234567890"), doris_udf::StringVal("SM4_128_OFB"));
        StringVal result = EncryptionFunctions::sm4_decrypt(
                context.get(), encryptWord, doris_udf::StringVal("key"),
                doris_udf::StringVal("01234567890"), doris_udf::StringVal("SM4_128_OFB"));
        StringVal expected = doris_udf::StringVal("hello");
        EXPECT_EQ(expected, result);
    }
}

} // namespace doris
