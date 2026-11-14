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

#include "storage/iterator/union_iterator.h"

#include <algorithm>
#include <memory>
#include <optional>
#include <utility>

#include "common/status.h"
#include "core/block/block.h"
#include "storage/iterators.h"
#include "storage/schema.h"

namespace doris {
using namespace ErrorCode;

// VUnionIterator reads input iterators one by one. Every input writes the same
// visible slot prefix; a SegmentIterator may retain delete-predicate columns
// privately after that prefix.
class VUnionIterator : public RowwiseIterator {
public:
    // Iterators' ownership it transferred to this class.
    // This class will delete all iterators when destructs
    // Client should not use iterators anymore.
    VUnionIterator(std::vector<RowwiseIteratorUPtr>&& v, ReadSchemaSPtr read_schema)
            : _read_schema(std::move(read_schema)), _origin_iters(std::move(v)) {}

    ~VUnionIterator() override = default;

    Status init(const StorageReadOptions& opts) override;

    Status next_batch(Block* block) override;

    // A union does not compare keys: every flag is 0. The folds read a union only over rowsets
    // whose keys do not overlap, so no key repeats.
    Status next_batch(Block* block, std::vector<uint8_t>* same_key) override;

    const ReadSchema& schema() const override { return *_read_schema; }

    void update_profile(RuntimeProfile* profile) override {
        if (_cur_iter != nullptr) {
            _cur_iter->update_profile(profile);
        }
    }

private:
    const ReadSchemaSPtr _read_schema;
    RowwiseIteratorUPtr _cur_iter = nullptr;
    std::optional<StorageReadOptions> _read_options;
    std::vector<RowwiseIteratorUPtr> _origin_iters;
};

Status VUnionIterator::init(const StorageReadOptions& opts) {
    _read_options.emplace(opts);
    if (_origin_iters.empty()) {
        return Status::OK();
    }

    // we use back() and pop_back() of std::vector to handle each iterator,
    // so reverse the vector here to keep result block of next_batch to be
    // in the same order as the original segments.
    std::ranges::reverse(_origin_iters);

    _cur_iter = std::move(_origin_iters.back());
    RETURN_IF_ERROR(_cur_iter->init(*_read_options));
    return Status::OK();
}

Status VUnionIterator::next_batch(Block* block) {
    while (_cur_iter != nullptr) {
        auto st = _cur_iter->next_batch(block);
        if (st.is<END_OF_FILE>()) {
            _origin_iters.pop_back();
            if (!_origin_iters.empty()) {
                _cur_iter = std::move(_origin_iters.back());
                RETURN_IF_ERROR(_cur_iter->init(*_read_options));
            } else {
                _cur_iter = nullptr;
            }
        } else {
            return st;
        }
    }
    return Status::EndOfFile("End of VUnionIterator");
}

Status VUnionIterator::next_batch(Block* block, std::vector<uint8_t>* same_key) {
    same_key->clear();
    RETURN_IF_ERROR(next_batch(block));
    same_key->resize(block->rows(), 0);
    return Status::OK();
}

RowwiseIteratorUPtr new_union_iterator(std::vector<RowwiseIteratorUPtr>&& inputs,
                                       ReadSchemaSPtr read_schema) {
    return std::make_unique<VUnionIterator>(std::move(inputs), std::move(read_schema));
}

} // namespace doris
