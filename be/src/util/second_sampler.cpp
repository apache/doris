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

#include "second_sampler.h"

namespace doris {

template <typename T>
void SecondSampler<T>::update_sample(T current_value) {
    if (!is_init_succ()) {
        return;
    }
    T delta = current_value - _last_update_value;
    _last_update_value = current_value;
    _current_idx = (_current_idx + 1) % _arr_size;
    _sample_arr[_current_idx] = delta;

    _arr_used_size = _arr_used_size < _arr_size ? _arr_used_size + 1 : _arr_used_size;
}

template <typename T>
Status SecondSampler<T>::get_window_value(T& result, int window_size) {
    if (!is_init_succ()) {
        return Status::InvalidArgument("invalid template args");
    }
    if (window_size > _time_span) {
        return Status::InvalidArgument("window size {} exceeds time span {}.", window_size,
                                       _time_span);
    }

    int sample_num =
            (window_size / _sample_interval) + (window_size % _sample_interval == 0 ? 0 : 1);
    DCHECK(sample_num <= _arr_size);

    // when query time not reach window size, window size should be query's time
    // _arr_used_size increases as query run, but it would not exceed time_span(that is _arr_size)
    sample_num = std::min(sample_num, _arr_used_size);

    int cur_idx = _current_idx;
    T ret = 0;
    for (int i = 0; i < sample_num; i++) {
        ret += _sample_arr[cur_idx];
        cur_idx = cur_idx == 0 ? _arr_size - 1 : cur_idx - 1;
    }
    result = ret;
    return Status::OK();
}

template <typename T>
Status SecondSampler<T>::get_full_window_value(T& result) {
    if (!is_init_succ()) {
        return Status::InvalidArgument("invalid template args");
    }
    T ret = 0;
    for (int i = 0; i < _arr_size; i++) {
        ret += _sample_arr[i];
    }
    result = ret;
    return Status::OK();
}

template class SecondSampler<int64_t>;

}; // namespace doris