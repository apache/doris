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

#include <cstddef>

namespace doris {
namespace segment_v2 {

static constexpr size_t DEFAULT_PAGE_SIZE = 1024 * 1024; // default size: 1M

constexpr long ROW_STORE_PAGE_SIZE_DEFAULT_VALUE = 16384; // default row store page size: 16KB

struct PageBuilderOptions {
    size_t data_page_size = DEFAULT_PAGE_SIZE;

    size_t dict_page_size = DEFAULT_PAGE_SIZE;

    bool need_check_bitmap = true;

    bool is_dict_page = false; // page used for saving dictionary
};

struct PageDecoderOptions {
    bool need_check_bitmap = true;
};

} // namespace segment_v2
} // namespace doris
