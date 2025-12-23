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

#ifndef ORC_MURMUR3_HH
#define ORC_MURMUR3_HH

#include "orc/orc-config.hh"

namespace orc {

  class Murmur3 {
   public:
    static const uint32_t DEFAULT_SEED = 104729;
    static const uint64_t NULL_HASHCODE = 2862933555777941757LL;

    static uint64_t hash64(const uint8_t* data, uint32_t len);

   private:
    static uint64_t fmix64(uint64_t value);
    static uint64_t hash64(const uint8_t* data, uint32_t len, uint32_t seed);
  };

}  // namespace orc

#endif  // ORC_MURMUR3_HH
