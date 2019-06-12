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

#include "gen_cpp/segment_v2.pb.h"

namespace doris {

namespace segment_v2 {

struct BuilderOptions {
    size_t data_page_size;

    size_t dict_page_size;

    bool write_posidx;

    EncodingTypePB encoding;

    CompressionTypePB compression_type;

    bool is_nullable;

    bool has_dictionary;
};

} // namespace segment_v2

} // namespace doris
