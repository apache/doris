/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "Murmur3.hh"
#include "wrap/gtest-wrapper.h"

namespace orc {

  // Same test as TestMurmur3#testHashCodeM3_64() in Java codes. Make sure the hash values
  // are consistent between the Java client and C++ client.
  // TODO(ORC-1025): Add exhaustive test on more strings.
  TEST(TestMurmur3, testHash64) {
    uint8_t origin[] =
        "It was the best of times, it was the worst of times,"
        " it was the age of wisdom, it was the age of foolishness,"
        " it was the epoch of belief, it was the epoch of incredulity,"
        " it was the season of Light, it was the season of Darkness,"
        " it was the spring of hope, it was the winter of despair,"
        " we had everything before us, we had nothing before us,"
        " we were all going direct to Heaven,"
        " we were all going direct the other way.";
    uint32_t len = static_cast<uint32_t>(sizeof(origin) / sizeof(uint8_t) - 1);
    uint64_t hash = Murmur3::hash64(origin, len);
    EXPECT_EQ(305830725663368540L, hash);
  }

}  // namespace orc
