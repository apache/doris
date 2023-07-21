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

#include <butil/macros.h>
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string>

#include "common/status.h"
#include "io/fs/file_system.h"

namespace doris {
class CollectionValue;
class Field;
class TabletIndex;

namespace segment_v2 {

class InvertedIndexColumnWriter {
public:
    static Status create(const Field* field, std::unique_ptr<InvertedIndexColumnWriter>* res,
                         const std::string& segment_file_name, const std::string& dir,
                         const TabletIndex* inverted_index, const io::FileSystemSPtr& fs);
    virtual Status init() = 0;

    InvertedIndexColumnWriter() = default;
    virtual ~InvertedIndexColumnWriter() = default;

    virtual Status add_values(const std::string name, const void* values, size_t count) = 0;
    virtual Status add_array_values(size_t field_size, const CollectionValue* values,
                                    size_t count) = 0;

    virtual Status add_nulls(uint32_t count) = 0;

    virtual Status finish() = 0;

    virtual int64_t size() const = 0;

    virtual int64_t file_size() const = 0;

private:
    DISALLOW_COPY_AND_ASSIGN(InvertedIndexColumnWriter);
};

} // namespace segment_v2
} // namespace doris
