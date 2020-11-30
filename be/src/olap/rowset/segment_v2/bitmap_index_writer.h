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
#include <memory>

#include "common/status.h"
#include "gen_cpp/segment_v2.pb.h"
#include "gutil/macros.h"

namespace doris {

class TypeInfo;

namespace fs {
class WritableBlock;
}

namespace segment_v2 {

class BitmapIndexWriter {
public:
    static Status create(const TypeInfo* typeinfo, std::unique_ptr<BitmapIndexWriter>* res);

    BitmapIndexWriter() = default;
    virtual ~BitmapIndexWriter() = default;

    virtual void add_values(const void* values, size_t count) = 0;

    virtual void add_nulls(uint32_t count) = 0;

    virtual Status finish(fs::WritableBlock* file, ColumnIndexMetaPB* index_meta) = 0;

    virtual uint64_t size() const = 0;

private:
    DISALLOW_COPY_AND_ASSIGN(BitmapIndexWriter);
};

} // namespace segment_v2
} // namespace doris
