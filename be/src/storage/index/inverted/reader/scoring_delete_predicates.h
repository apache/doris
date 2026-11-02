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

#include <memory>
#include <vector>

#include "common/status.h"
#include "core/custom_allocator.h"
#include "storage/delete/delete_handler.h"

namespace roaring {
class Roaring;
}

namespace doris {
namespace segment_v2 {
class Segment;
}
class RuntimeState;
class StorageReadOptions;

// Uses the same historical column identities, predicate semantics and version boundaries as a
// rowset scan. It reads only predicate columns, without a search predicate or scanner range.
class ScoringDeletePredicates {
public:
    Status init(const std::vector<RowsetMetaSharedPtr>& predicates, int64_t read_version,
                const TabletSchemaSPtr& tablet_schema);
    bool empty() const { return _handler.empty(); }
    // Returns ascending IDs hidden by predicates, excluding rows already in the bitmap/sign set.
    Status collect(const std::shared_ptr<segment_v2::Segment>& segment, int64_t rowset_version,
                   const StorageReadOptions& read_options, RuntimeState* state,
                   const roaring::Roaring& already_hidden, DorisVector<uint32_t>* hidden) const;

private:
    ReadSchemaSPtr _schema;
    DeleteHandler _handler;
};
} // namespace doris
