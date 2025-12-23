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

#ifndef ORC_BLOOMFILTER_HH
#define ORC_BLOOMFILTER_HH

#include "orc/orc-config.hh"

#include <memory>
#include <vector>

namespace orc {

  class BloomFilter {
   public:
    virtual ~BloomFilter();

    // test if the element exists in BloomFilter
    virtual bool testBytes(const char* data, int64_t length) const = 0;
    virtual bool testLong(int64_t data) const = 0;
    virtual bool testDouble(double data) const = 0;
  };

  struct BloomFilterIndex {
    std::vector<std::shared_ptr<BloomFilter>> entries;
  };

}  // namespace orc

#endif  // ORC_BLOOMFILTER_HH
