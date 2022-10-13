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

#include "util/encryption_util.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "util/url_coding.h"

namespace doris {

class EncryptionUtilTest : public testing::Test {
public:
    EncryptionUtilTest() { _aes_key = "doris_aes_key"; }

private:
    std::string _aes_key;
};

void do_aes_test(const std::string& source, const std::string& key) {
    int cipher_len = source.length() + 16;
    std::unique_ptr<unsigned char[]> dest(new unsigned char[cipher_len]);
    int ret_code = EncryptionUtil::encrypt(AES_128_ECB, (unsigned char*)source.c_str(),
                                           source.length(), (unsigned char*)key.c_str(),
                                           key.length(), nullptr, true, dest.get());
    EXPECT_TRUE(ret_code > 0);
    int encrypted_length = ret_code;
    std::unique_ptr<char[]> decrypted(new char[cipher_len]);
    ret_code = EncryptionUtil::decrypt(AES_128_ECB, dest.get(), encrypted_length,
                                       (unsigned char*)key.c_str(), key.length(), nullptr, true,
                                       (unsigned char*)decrypted.get());
    EXPECT_TRUE(ret_code > 0);
    std::string decrypted_content(decrypted.get(), ret_code);
    EXPECT_EQ(source, decrypted_content);
}

void do_sm4_test(const std::string& source, const std::string& key) {
    int cipher_len = source.length() + 16;
    std::unique_ptr<unsigned char[]> dest(new unsigned char[cipher_len]);
    int ret_code = EncryptionUtil::encrypt(SM4_128_ECB, (unsigned char*)source.c_str(),
                                           source.length(), (unsigned char*)key.c_str(),
                                           key.length(), nullptr, true, dest.get());
    EXPECT_TRUE(ret_code > 0);
    int encrypted_length = ret_code;
    std::unique_ptr<char[]> decrypted(new char[cipher_len]);
    ret_code = EncryptionUtil::decrypt(SM4_128_ECB, dest.get(), encrypted_length,
                                       (unsigned char*)key.c_str(), key.length(), nullptr, true,
                                       (unsigned char*)decrypted.get());
    EXPECT_TRUE(ret_code > 0);
    std::string decrypted_content(decrypted.get(), ret_code);
    EXPECT_EQ(source, decrypted_content);
}

TEST_F(EncryptionUtilTest, aes_test_basic) {
    std::string source_1 = "hello, doris";
    do_aes_test(source_1, _aes_key);
    std::string source_2 = "doris test";
    do_aes_test(source_2, _aes_key);
}

TEST_F(EncryptionUtilTest, sm4_test_basic) {
    std::string source_1 = "hello, doris";
    do_sm4_test(source_1, _aes_key);
    std::string source_2 = "doris test";
    do_sm4_test(source_2, _aes_key);
}

TEST_F(EncryptionUtilTest, aes_test_by_case) {
    std::string case_1 = "9qYx8l1601oWHEVCREAqZg=="; // base64 for encrypted "hello, doris"
    std::string source_1 = "hello, doris";
    std::string case_2 = "nP/db4j4yqMjXv/pItaOVA=="; // base64 for encrypted "doris test"
    std::string source_2 = "doris test";

    std::unique_ptr<char[]> encrypt_1(new char[case_1.length()]);
    int length_1 = base64_decode(case_1.c_str(), case_1.length(), encrypt_1.get());
    std::unique_ptr<char[]> decrypted_1(new char[case_1.length()]);
    int ret_code = EncryptionUtil::decrypt(AES_128_ECB, (unsigned char*)encrypt_1.get(), length_1,
                                           (unsigned char*)_aes_key.c_str(), _aes_key.length(),
                                           nullptr, true, (unsigned char*)decrypted_1.get());
    EXPECT_TRUE(ret_code > 0);
    std::string decrypted_content_1(decrypted_1.get(), ret_code);
    EXPECT_EQ(source_1, decrypted_content_1);

    std::unique_ptr<char[]> encrypt_2(new char[case_2.length()]);
    int length_2 = base64_decode(case_2.c_str(), case_2.length(), encrypt_2.get());
    std::unique_ptr<char[]> decrypted_2(new char[case_2.length()]);
    ret_code = EncryptionUtil::decrypt(AES_128_ECB, (unsigned char*)encrypt_2.get(), length_2,
                                       (unsigned char*)_aes_key.c_str(), _aes_key.length(), nullptr,
                                       true, (unsigned char*)decrypted_2.get());
    EXPECT_TRUE(ret_code > 0);
    std::string decrypted_content_2(decrypted_2.get(), ret_code);
    EXPECT_EQ(source_2, decrypted_content_2);
}

TEST_F(EncryptionUtilTest, sm4_test_by_case) {
    std::string case_1 = "P/Ub8/arZ22TW+rAT5sgYg=="; // base64 for encrypted "hello, doris"
    std::string source_1 = "hello, doris";
    std::string case_2 = "2I+UW9axOP2Tv35BGYgy+g=="; // base64 for encrypted "doris test"
    std::string source_2 = "doris test";

    std::unique_ptr<char[]> encrypt_1(new char[case_1.length()]);
    int length_1 = base64_decode(case_1.c_str(), case_1.length(), encrypt_1.get());
    std::unique_ptr<char[]> decrypted_1(new char[case_1.length()]);
    int ret_code = EncryptionUtil::decrypt(SM4_128_ECB, (unsigned char*)encrypt_1.get(), length_1,
                                           (unsigned char*)_aes_key.c_str(), _aes_key.length(),
                                           nullptr, true, (unsigned char*)decrypted_1.get());
    EXPECT_TRUE(ret_code > 0);
    std::string decrypted_content_1(decrypted_1.get(), ret_code);
    EXPECT_EQ(source_1, decrypted_content_1);

    std::unique_ptr<char[]> encrypt_2(new char[case_2.length()]);
    int length_2 = base64_decode(case_2.c_str(), case_2.length(), encrypt_2.get());
    std::unique_ptr<char[]> decrypted_2(new char[case_2.length()]);
    ret_code = EncryptionUtil::decrypt(SM4_128_ECB, (unsigned char*)encrypt_2.get(), length_2,
                                       (unsigned char*)_aes_key.c_str(), _aes_key.length(), nullptr,
                                       true, (unsigned char*)decrypted_2.get());
    EXPECT_TRUE(ret_code > 0);
    std::string decrypted_content_2(decrypted_2.get(), ret_code);
    EXPECT_EQ(source_2, decrypted_content_2);
}

TEST_F(EncryptionUtilTest, aes_with_iv_test_by_case) {
    std::string case_1 = "XbJgw1AxBNwZZPpvzPtWyg=="; // base64 for encrypted "hello, doris"
    std::string source_1 = "hello, doris";
    std::string case_2 = "gpKcO/iwgeRCIWBQdkpAkQ=="; // base64 for encrypted "doris test"
    std::string source_2 = "doris test";
    std::string iv = "doris";

    std::unique_ptr<char[]> encrypt_1(new char[case_1.length()]);
    int length_1 = base64_decode(case_1.c_str(), case_1.length(), encrypt_1.get());
    std::unique_ptr<char[]> decrypted_1(new char[case_1.length()]);
    int ret_code = EncryptionUtil::decrypt(AES_128_CBC, (unsigned char*)encrypt_1.get(), length_1,
                                           (unsigned char*)_aes_key.c_str(), _aes_key.length(),
                                           iv.c_str(), true, (unsigned char*)decrypted_1.get());
    EXPECT_TRUE(ret_code > 0);
    std::string decrypted_content_1(decrypted_1.get(), ret_code);
    EXPECT_EQ(source_1, decrypted_content_1);
    std::unique_ptr<char[]> decrypted_11(new char[case_1.length()]);

    ret_code = EncryptionUtil::decrypt(AES_128_CBC, (unsigned char*)encrypt_1.get(), length_1,
                                       (unsigned char*)_aes_key.c_str(), _aes_key.length(),
                                       iv.c_str(), true, (unsigned char*)decrypted_11.get());
    EXPECT_TRUE(ret_code > 0);
    std::string decrypted_content_11(decrypted_11.get(), ret_code);
    EXPECT_EQ(source_1, decrypted_content_11);

    std::unique_ptr<char[]> encrypt_2(new char[case_2.length()]);
    int length_2 = base64_decode(case_2.c_str(), case_2.length(), encrypt_2.get());
    std::unique_ptr<char[]> decrypted_2(new char[case_2.length()]);
    ret_code = EncryptionUtil::decrypt(AES_128_CBC, (unsigned char*)encrypt_2.get(), length_2,
                                       (unsigned char*)_aes_key.c_str(), _aes_key.length(),
                                       iv.c_str(), true, (unsigned char*)decrypted_2.get());
    EXPECT_TRUE(ret_code > 0);
    std::string decrypted_content_2(decrypted_2.get(), ret_code);
    EXPECT_EQ(source_2, decrypted_content_2);

    std::unique_ptr<char[]> decrypted_21(new char[case_2.length()]);
    ret_code = EncryptionUtil::decrypt(AES_128_CBC, (unsigned char*)encrypt_2.get(), length_2,
                                       (unsigned char*)_aes_key.c_str(), _aes_key.length(),
                                       iv.c_str(), true, (unsigned char*)decrypted_21.get());
    EXPECT_TRUE(ret_code > 0);
    std::string decrypted_content_21(decrypted_21.get(), ret_code);
    EXPECT_EQ(source_2, decrypted_content_21);
}

TEST_F(EncryptionUtilTest, sm4_with_iv_test_by_case) {
    std::string case_1 = "9FFlX59+3EbIC7rqylMNwg=="; // base64 for encrypted "hello, doris"
    std::string source_1 = "hello, doris";
    std::string case_2 = "RIJVVUUmMT/4CVNYdxVvXA=="; // base64 for encrypted "doris test"
    std::string source_2 = "doris test";
    std::string iv = "doris";

    std::unique_ptr<char[]> encrypt_1(new char[case_1.length()]);
    int length_1 = base64_decode(case_1.c_str(), case_1.length(), encrypt_1.get());
    std::unique_ptr<char[]> decrypted_1(new char[case_1.length()]);
    std::unique_ptr<char[]> decrypted_11(new char[case_1.length()]);

    int ret_code = EncryptionUtil::decrypt(SM4_128_CBC, (unsigned char*)encrypt_1.get(), length_1,
                                           (unsigned char*)_aes_key.c_str(), _aes_key.length(),
                                           iv.c_str(), true, (unsigned char*)decrypted_1.get());
    EXPECT_TRUE(ret_code > 0);
    std::string decrypted_content_1(decrypted_1.get(), ret_code);
    EXPECT_EQ(source_1, decrypted_content_1);

    std::unique_ptr<char[]> encrypt_2(new char[case_2.length()]);
    int length_2 = base64_decode(case_2.c_str(), case_2.length(), encrypt_2.get());
    std::unique_ptr<char[]> decrypted_2(new char[case_2.length()]);
    std::unique_ptr<char[]> decrypted_21(new char[case_2.length()]);

    ret_code = EncryptionUtil::decrypt(SM4_128_CBC, (unsigned char*)encrypt_2.get(), length_2,
                                       (unsigned char*)_aes_key.c_str(), _aes_key.length(),
                                       iv.c_str(), true, (unsigned char*)decrypted_2.get());
    EXPECT_TRUE(ret_code > 0);
    std::string decrypted_content_2(decrypted_2.get(), ret_code);
    EXPECT_EQ(source_2, decrypted_content_2);

    ret_code = EncryptionUtil::decrypt(SM4_128_CBC, (unsigned char*)encrypt_1.get(), length_1,
                                       (unsigned char*)_aes_key.c_str(), _aes_key.length(),
                                       iv.c_str(), true, (unsigned char*)decrypted_11.get());
    EXPECT_TRUE(ret_code > 0);
    std::string decrypted_content_11(decrypted_11.get(), ret_code);
    EXPECT_EQ(source_1, decrypted_content_11);

    ret_code = EncryptionUtil::decrypt(SM4_128_CBC, (unsigned char*)encrypt_2.get(), length_2,
                                       (unsigned char*)_aes_key.c_str(), _aes_key.length(),
                                       iv.c_str(), true, (unsigned char*)decrypted_21.get());
    EXPECT_TRUE(ret_code > 0);
    std::string decrypted_content_21(decrypted_21.get(), ret_code);
    EXPECT_EQ(source_2, decrypted_content_21);
}

} // namespace doris
