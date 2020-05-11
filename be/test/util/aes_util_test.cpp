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

#include "util/aes_util.h"

#include <string>
#include <memory>
#include <gtest/gtest.h>

#include "exprs/base64.h"

namespace doris {

class AesUtilTest : public testing::Test {
public:
    AesUtilTest() {
        _aes_key = "doris_aes_key";
    }

private:
    std::string _aes_key;
};

void do_aes_test(const std::string& source, const std::string& key) {
    int cipher_len = source.length() + 16;
    std::unique_ptr<unsigned char[]> dest(new unsigned char[cipher_len]);
    int ret_code = AesUtil::encrypt(AES_128_ECB, (unsigned char *)source.c_str(), source.length(),
            (unsigned char *)key.c_str(), key.length(), NULL, true, dest.get());
    ASSERT_TRUE(ret_code > 0);
    int encrypted_length = ret_code;
    std::unique_ptr<char[]> decrypted(new char[cipher_len]);
    ret_code = AesUtil::decrypt(AES_128_ECB, dest.get(), encrypted_length, 
            (unsigned char *)key.c_str(), key.length(), NULL, true, (unsigned char *)decrypted.get());
    ASSERT_TRUE(ret_code > 0);
    std::string decrypted_content(decrypted.get(), ret_code);
    ASSERT_EQ(source, decrypted_content);
}

TEST_F(AesUtilTest, aes_test_basic) {
    std::string source_1 = "hello, doris";
    do_aes_test(source_1, _aes_key);
    std::string source_2 = "doris test";
    do_aes_test(source_2, _aes_key);
}

TEST_F(AesUtilTest, aes_test_by_case) {
    std::string case_1 = "9qYx8l1601oWHEVCREAqZg=="; // base64 for encrypted "hello, doris"
    std::string source_1 = "hello, doris";
    std::string case_2 = "nP/db4j4yqMjXv/pItaOVA=="; // base64 for encrypted "doris test"
    std::string source_2 = "doris test";

    std::unique_ptr<char[]> encrypt_1(new char[case_1.length()]);
    int length_1 = base64_decode2(case_1.c_str(), case_1.length(), encrypt_1.get());
    std::unique_ptr<char[]> decrypted_1(new char[case_1.length()]);
    int ret_code = AesUtil::decrypt(AES_128_ECB, (unsigned char *)encrypt_1.get(), length_1,
            (unsigned char *)_aes_key.c_str(), _aes_key.length(), NULL, true, (unsigned char *)decrypted_1.get());
    ASSERT_TRUE(ret_code > 0);
    std::string decrypted_content_1(decrypted_1.get(), ret_code);
    ASSERT_EQ(source_1, decrypted_content_1);

    std::unique_ptr<char[]> encrypt_2(new char[case_2.length()]);
    int length_2 = base64_decode2(case_2.c_str(), case_2.length(), encrypt_2.get());
    std::unique_ptr<char[]> decrypted_2(new char[case_2.length()]);
    ret_code = AesUtil::decrypt(AES_128_ECB, (unsigned char *)encrypt_2.get(), length_2,
            (unsigned char *)_aes_key.c_str(), _aes_key.length(), NULL, true, (unsigned char *)decrypted_2.get());
    ASSERT_TRUE(ret_code > 0);
    std::string decrypted_content_2(decrypted_2.get(), ret_code);
    ASSERT_EQ(source_2, decrypted_content_2);
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
