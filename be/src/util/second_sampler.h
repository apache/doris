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

#include <vector>

#include "common/status.h"

namespace doris {

template <typename T>
class SecondSampler {
public:
    SecondSampler(uint64_t time_span, uint64_t sample_interval) {
        if (time_span % sample_interval == 0) {
            _arr_size = time_span / sample_interval;
            _sample_arr.resize(_arr_size, 0);
            _time_span = time_span;
            _sample_interval = sample_interval;
            _is_init_succ = true;
        }
    }

    void update_sample(T current_value);

    Status get_window_value(T& result, int window_size = 10);

    Status get_full_window_value(T& result);

    bool is_init_succ() { return _is_init_succ; }

private:
    bool _is_init_succ = false;
    int _time_span = 0;
    int _sample_interval = 0;

    T _last_update_value = 0;
    int _current_idx = 0;

    std::vector<T> _sample_arr;
    int _arr_size = 0;
    int _arr_used_size = 0;
};
}; // namespace doris