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

#include <ostream>
#include <string>

#include <boost/functional/hash.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "gen_cpp/Types_types.h"  // for TUniqueId
#include "gen_cpp/types.pb.h"  // for PUniqueId
// #include "util/debug_util.h"
#include "util/hash_util.hpp"

namespace doris {
class UUIDGenerator {
public:
    static boost::uuids::uuid next_uuid() {
        std::lock_guard<std::mutex> lock(_uuid_gen_lock);
        return _boost_uuid_generator();
    }

    UUIDGenerator* instance();

private:
    boost::uuids::basic_random_generator<boost::mt19937> _boost_uuid_generator;

    static UUIDGenerator* _s_instance;
    static std::mutex _mlock;
    std::mutex _uuid_gen_lock;
};

}