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

#pragma once

#define SM3_DIGEST_LENGTH 32

#include <openssl/ossl_typ.h>
#include <stddef.h>

#include <string>

namespace doris {

class SM3Digest {
public:
    SM3Digest();
    ~SM3Digest();
    void update(const void* data, size_t length);
    void digest();

    const std::string& hex() const { return _hex; }

private:
    EVP_MD_CTX* _ctx = nullptr;
    const EVP_MD* _md = nullptr;

    std::string _hex;
};

} // namespace doris
