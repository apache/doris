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

#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <roaring/roaring.hh>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "storage/olap_common.h"

namespace lucene::index {
class IndexReader;
}

namespace doris {

class BaseTablet;
class RuntimeState;
class Rowset;
class RowsetSegmentView;
class TabletSchema;
struct TabletReadSource;
struct CollectInfo;
class IColumn;
struct InvertedIndexAnalyzerCtx;
namespace io {
struct IOContext;
}

struct VisibleScoringDocuments {
    std::map<RowsetId, std::map<uint32_t, roaring::Roaring>> segments;
};

// Cached by immutable physical index identity, independently of a query's visibility.
// Failed/cancelled loads remain retryable; only a complete vector is published.
class ExactScoringDocLengths {
public:
    using Loader = std::function<Status(std::vector<uint64_t>*)>;
    Status get_or_load(const Loader& loader, const std::vector<uint64_t>** result,
                       uint64_t* total_tokens = nullptr,
                       const std::function<Status()>& check_cancel = {});

private:
    std::mutex _mutex;
    std::condition_variable _changed;
    bool _loading = false;
    bool _ready = false;
    std::vector<uint64_t> _lengths;
    uint64_t _total_tokens = 0;
};

namespace visible_collection_detail {
Status count_raw_tokens(const IColumn& column, size_t row, const InvertedIndexAnalyzerCtx& analyzer,
                        const std::map<std::string, std::string>& properties, uint64_t ignore_above,
                        uint64_t* count);
} // namespace visible_collection_detail

// The caller has established DUP or merge-on-write UNIQUE row identity.
Status collect_visible_scoring_documents(const std::shared_ptr<BaseTablet>& tablet,
                                         const TabletReadSource& read_source, int64_t version,
                                         const std::shared_ptr<TabletSchema>& tablet_schema,
                                         RuntimeState* state, VisibleScoringDocuments* visible);

Status collect_clucene_visible_field_statistics(
        lucene::index::IndexReader* index_reader, const std::shared_ptr<Rowset>& rowset,
        const RowsetSegmentView& segment, const CollectInfo& info, const std::wstring& field_name,
        const std::string& index_cache_key, const roaring::Roaring& visible, RuntimeState* state,
        const io::IOContext* io_ctx, bool unit_length, uint64_t* token_count,
        std::unordered_map<std::wstring, uint64_t>* term_doc_freqs);

} // namespace doris
