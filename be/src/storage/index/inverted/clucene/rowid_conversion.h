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

#include <CLucene/index/DocIdMapping.h>

#include "core/custom_allocator.h"
#include "storage/compressed_rowid_map.h"

namespace doris {
class RowIdConversion;

namespace segment_v2 {

class CluceneRowIdMapping final : public lucene::index::DocIdMapping {
public:
    explicit CluceneRowIdMapping(const RowIdConversion& conversion);
    size_t sourceCount() const override { return _sources.size(); }
    Destination get(uint32_t source, uint32_t doc) const override;

private:
    struct Source {
        explicit Source(RowIdConversionSource view) : mapping(view) {}
        RowIdConversionSource mapping;
        mutable CompressedRowIdMap::Cursor cursor;
    };
    DorisVector<Source> _sources;
    // Compaction is synchronous on one thread. A term may interleave sources or
    // revisit earlier blocks: keep one block per source and share the Zstd workspace.
    mutable CompressedRowIdMap::DecodeWorkspace _workspace;
};

} // namespace segment_v2
} // namespace doris
