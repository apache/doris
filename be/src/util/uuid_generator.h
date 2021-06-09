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

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <mutex>
#include <ostream>
#include <string>

#include "util/spinlock.h"

namespace doris {

class UUIDGenerator {
public:
    boost::uuids::uuid next_uuid() {
        std::lock_guard<SpinLock> lock(_uuid_gen_lock);
        return _boost_uuid_generator();
    }

    static UUIDGenerator* instance() {
        static UUIDGenerator generator;
        return &generator;
    }

private:
    boost::uuids::basic_random_generator<boost::mt19937> _boost_uuid_generator;
    SpinLock _uuid_gen_lock;
};

} // namespace doris