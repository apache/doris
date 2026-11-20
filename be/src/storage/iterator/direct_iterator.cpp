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

#include "storage/iterator/direct_iterator.h"

#include <memory>
#include <utility>

#include "storage/iterator/vgeneric_iterators.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset_reader.h"

namespace doris {

namespace {

// The rows of one rowset through its rowset reader: an input of the union in DirectIterator.
class RowsetReaderIterator final : public RowwiseIterator {
public:
    RowsetReaderIterator(RowsetReaderSharedPtr rs_reader, ReadSchemaSPtr read_schema)
            : _rs_reader(std::move(rs_reader)), _read_schema(std::move(read_schema)) {}

    // Nothing to open: the rowset reader opens its segments on its first next_batch().
    Status init(const StorageReadOptions& /*opts*/) override { return Status::OK(); }

    Status next_batch(Block* block) override { return _rs_reader->next_batch(block); }

    const ReadSchema& schema() const override { return *_read_schema; }

    void update_profile(RuntimeProfile* profile) override { _rs_reader->update_profile(profile); }

private:
    const RowsetReaderSharedPtr _rs_reader;
    const ReadSchemaSPtr _read_schema;
};

} // namespace

DirectIterator::DirectIterator(std::vector<RowsetReaderSharedPtr> rs_readers,
                               ReadSchemaSPtr read_schema, OlapReaderStatistics* stats)
        : _stats(stats) {
    std::vector<RowwiseIteratorUPtr> rowsets;
    rowsets.reserve(rs_readers.size());
    for (auto& rs_reader : rs_readers) {
        rowsets.push_back(
                std::make_unique<RowsetReaderIterator>(std::move(rs_reader), read_schema));
    }
    // VUnionIterator destroys an input once it is read, and with it the input's rowset reader.
    _rowsets = new_union_iterator(std::move(rowsets), std::move(read_schema));
}

Status DirectIterator::init() {
    return _rowsets->init(StorageReadOptions(*_stats));
}

Status DirectIterator::next_block(Block* block, bool* eof) {
    auto res = _rowsets->next_batch(block);
    if (!res.ok() && !res.is<ErrorCode::END_OF_FILE>()) [[unlikely]] {
        return res;
    }
    *eof = res.is<ErrorCode::END_OF_FILE>();
    return Status::OK();
}

} // namespace doris
