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

#include "storage/index/inverted/clucene/rowid_conversion.h"

#include <CLucene.h>

#include "common/cast_set.h"
#include "storage/rowid_conversion.h"

namespace doris::segment_v2 {

CluceneRowIdMapping::CluceneRowIdMapping(const RowIdConversion& conversion) {
    _sources.reserve(conversion.source_count());
    for (size_t source = 0; source < conversion.source_count(); ++source) {
        _sources.emplace_back(conversion.source(cast_set<uint32_t>(source)));
    }
}

CluceneRowIdMapping::Destination CluceneRowIdMapping::get(uint32_t source, uint32_t doc) const {
    if (source >= _sources.size()) {
        _CLTHROWA(CL_ERR_IO, "CLucene row-id mapping source is out of range");
    }
    const auto& entry = _sources[source];
    // Source views use the missing sentinel for out-of-range probes. A posting
    // outside its physical source instead indicates a malformed index/mapping.
    if (doc >= entry.mapping.size()) {
        _CLTHROWA(CL_ERR_IO, "CLucene row-id mapping document is out of range");
    }
    Destination destination;
    auto status = entry.mapping.get(doc, entry.cursor, _workspace, &destination);
    if (!status.ok()) {
        // A decode/allocation failure must abort index compaction, never drop a posting.
        _CLTHROWA(CL_ERR_IO, status.to_string_no_stack().c_str());
    }
    return destination;
}

} // namespace doris::segment_v2
