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
import org.junit.jupiter.api.Assertions;
suite("docs/3.0/query/query-data/encryption-function.md") {
    try {
        multi_sql """
        select to_base64(aes_encrypt('text','F3229A0B371ED2D9441B830D21A390C3'));
        select aes_decrypt(from_base64('wr2JEDVXzL9+2XtRhgIloA=='),'F3229A0B371ED2D9441B830D21A390C3');
        select md5("abc");
        select md5("abcd");
        select md5sum("ab","cd");
        select TO_BASE64(SM4_ENCRYPT('text','F3229A0B371ED2D9441B830D21A390C3'));
        set block_encryption_mode="SM4_128_CBC";
        select to_base64(SM4_ENCRYPT('text','F3229A0B371ED2D9441B830D21A390C3', '0123456789'));
        select sm3("abcd");
        select sha("123");
        select sha2('abc', 224);
        select sha2('abc', 384);
        select digital_masking(13812345678);
        """

    } catch (Throwable t) {
        Assertions.fail("examples in docs/3.0/query/query-data/encryption-function.md failed to exec, please fix it", t)
    }
}
