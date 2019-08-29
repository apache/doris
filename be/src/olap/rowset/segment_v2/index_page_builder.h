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
#include <memory>

#include "common/status.h"
#include "util/slice.h"
#include "olap/field.h"
#include "gen_cpp/segment_v2.pb.h"
#include "olap/rowset/segment_v2/binary_plain_page.h"

namespace doris {

namespace segment_v2 {

// This class is the base of index page builder
class IndexPageBuilder {
public:
    virtual ~IndexPageBuilder() { }

    // add data to index
    virtual Status add(const uint8_t* vals, size_t count) = 0;

    // flush one page
    virtual Status flush() = 0;

    // finish the index build
    virtual Slice finish() = 0;
};

} // namespace segment_v2
} // namespace doris
