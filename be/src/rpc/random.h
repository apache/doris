// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#ifndef BDG_PALO_BE_SRC_RPC_RANDOM_H
#define BDG_PALO_BE_SRC_RPC_RANDOM_H

#include <chrono>

namespace palo {

/// Convenience interface for random number and data generation.
class Random {
public:

    /// Sets the seed of the random number generator.
    /// @param s Seed value
    static void seed(unsigned int s);

    /// Returns a random 32-bit unsigned integer.
    /// @return Random 32-bit integer in the range [0,<code>maximum</code>)
    static uint32_t number32(uint32_t maximum = 0);

    /// Returns a random 64-bit unsigned integer.
    /// @return Random 64-bit integer in the range [0,<code>maximum</code>)
    static int64_t number64(int64_t maximum = 0);

    /// Returns a random double.
    /// @return Random double in the range [0.0, 1.0)
    static double uniform01();

    /// Returns a random millisecond duration.
    /// @return Random millisecond duration in the range [0,<code>maximum</code>)
    static std::chrono::milliseconds duration_millis(uint32_t maximum);
};

} //namespace palo

#endif //BDG_PALO_BE_SRC_RPC_RANDOM_H
