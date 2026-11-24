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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace doris {
class KeyBoundsPB;
class ShortKeyIndexDecoder;
class Status;
class StorageReadOptions;
class TabletSchema;
struct RowSetSplits;
struct SampleSegment;
namespace segment_v2 {
class Segment;
}

class SegmentKeySeeker {
public:
    virtual ~SegmentKeySeeker() = default;
    static Status validate_schema(const TabletSchema& target, const TabletSchema& stored);
    static Status open(std::shared_ptr<segment_v2::Segment> segment, const TabletSchema& target,
                       const StorageReadOptions& options, std::unique_ptr<SegmentKeySeeker>* out,
                       const KeyBoundsPB* bounds = nullptr);
    static Status open_read_source(const std::vector<RowSetSplits>& rs_splits,
                                   const TabletSchema& target, StorageReadOptions& options,
                                   bool use_segment_cache,
                                   std::vector<std::unique_ptr<SegmentKeySeeker>>* seekers,
                                   std::vector<SampleSegment>* segments);
    virtual const ShortKeyIndexDecoder& index() const = 0;
    virtual Status read_prefix(uint32_t row, std::string* prefix) = 0;
    virtual Status read_tuple(uint32_t row, std::vector<std::string>* tuple, size_t* bytes) = 0;
    virtual Status read_key(uint32_t row, std::string* prefix, std::vector<std::string>* tuple,
                            size_t* bytes);

    Status lower_bound(const std::string& prefix, const std::vector<std::string>& tuple,
                       uint32_t rows, uint32_t* ordinal, uint32_t min_ordinal = 0);

private:
    std::string _max_key;
};
} // namespace doris
