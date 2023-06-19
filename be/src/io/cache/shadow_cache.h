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

#include <stdint.h>
#include <string>

namespace doris::io {

class ShadowCache {
public:
    
    virtual bool put(const std::string& key, int64_t size) = 0;

    virtual int64_t get(const std::string& key, int64_t bytes_read) = 0;

    virtual void aging() = 0;

    virtual void update_working_set_size() = 0;

    virtual void stop_update() = 0;

    virtual int64_t get_shadow_cache_key_num() = 0;

    virtual int64_t get_shadow_cache_bytes() = 0;

    virtual int64_t get_shadow_cache_read() = 0;

    virtual int64_t get_shadow_cache_hit() = 0;

    virtual int64_t get_shadow_cache_bytes_read() = 0;

    virtual int64_t get_shadow_cache_bytes_hit() = 0;

    virtual double get_false_positive_ratio() = 0;

    virtual ~ShadowCache() {}

};

} // namespace doris::io
