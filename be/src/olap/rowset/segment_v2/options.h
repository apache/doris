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
#include "olap/rowset/segment_v2/page_decoder.h" // for PageDecoder

namespace doris {
namespace segment_v2 {

struct PageBuilderOptions {
    size_t data_page_size = 0;

    size_t dict_page_size = 0;
};

struct PageDecoderOptions {
    PageDecoder* dict_decoder = nullptr;
};

} // namespace segment_v2
} // namespace doris
